"""C1 – Scenario orchestrator.

One command runs one scenario end to end::

    py src/sim/launcher.py --scenario scenarios/baseline.yaml

What it does
    1. reads the YAML scenario (format: docs/log-and-scenario-spec.md),
    2. generates a deterministic source file and a Contract 1 metainfo
       (built-in, byte-compatible with Role B) – or calls Role B's
       ``make-torrent`` when ``--make-torrent-script`` is given,
    3. starts the tracker (StubTracker by default, Role A's with ``--tracker-script``),
    4. starts the seeds and leechers (StubPeer by default, Role B's with
       ``--peer-script``) using only the Contract 4 CLI,
    5. runs the churn schedule (C2) and the network / fault plan (C3),
    6. merges every stdout line into ``logs/<scenario>/events.jsonl`` (Contract 5),
    7. stops everything cleanly (graceful first, kill after a timeout) and writes
       ``logs/<scenario>/processes.json`` with every exit code.

Swapping in the real implementations needs no code change::

    py src/sim/launcher.py --scenario scenarios/baseline.yaml ^
        --peer-script "-m peer" --tracker-script "-m tracker" ^
        --make-torrent-script "-m torrent.make_torrent"
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, TextIO

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import yaml  # noqa: E402

from src.sim.churn import ChurnEvent, build_schedule, schedule_fingerprint  # noqa: E402
from src.sim.common import (  # noqa: E402
    REPO_ROOT,
    TorrentInfo,
    build_metainfo,
    configure_utf8_stdio,
    load_metainfo_any,
    make_event,
    robust_rmtree,
    sha256_file,
    write_deterministic_file,
)
from src.sim.netem import NetworkEmulator, TrackerFaultInjector  # noqa: E402
from src.sim.procutil import (  # noqa: E402
    STOP_FILE_NAME,
    Entrypoint,
    child_env,
    find_free_port,
    graceful_stop,
    hard_kill,
    is_port_free,
    spawn,
    split_args,
    wait_for_port,
)

DEFAULT_PEER = "src/sim/stub_peer.py"
DEFAULT_TRACKER = "src/sim/stub_tracker.py"
DEFAULT_WORK_ROOT = Path(tempfile.gettempdir()) / "ds02-sim"
DEFAULT_LOGS_ROOT = REPO_ROOT / "logs"


# --------------------------------------------------------------------------- config


def load_scenario(path_or_cfg: str | Path | dict) -> dict[str, Any]:
    if isinstance(path_or_cfg, dict):
        cfg = json.loads(json.dumps(path_or_cfg))  # deep copy
    else:
        p = Path(path_or_cfg)
        if not p.is_absolute() and not p.exists():
            p = REPO_ROOT / p
        with open(p, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        cfg.setdefault("name", p.stem)
    errors = validate_scenario(cfg)
    if errors:
        raise ValueError("invalid scenario: " + "; ".join(errors))
    return cfg


def validate_scenario(cfg: dict[str, Any]) -> list[str]:
    errs: list[str] = []
    if not isinstance(cfg, dict):
        return ["scenario must be a mapping"]
    if not str(cfg.get("name", "")).strip():
        errs.append("'name' is required")
    t = cfg.get("torrent", {}) or {}
    if int(t.get("file_size_bytes", 1)) <= 0:
        errs.append("torrent.file_size_bytes must be > 0")
    if int(t.get("piece_length", 1)) <= 0:
        errs.append("torrent.piece_length must be > 0")
    s = cfg.get("swarm", {}) or {}
    if int(s.get("num_seeds", 1)) < 1:
        errs.append("swarm.num_seeds must be >= 1")
    if int(s.get("num_leechers", 0)) < 0:
        errs.append("swarm.num_leechers must be >= 0")
    sim = cfg.get("simulation", {}) or {}
    if float(sim.get("duration_sec", 20)) <= 0:
        errs.append("simulation.duration_sec must be > 0")
    churn = cfg.get("churn", {}) or {}
    if churn.get("enabled") and churn.get("mode", "poisson") not in ("poisson", "flash_crowd"):
        errs.append("churn.mode must be poisson or flash_crowd")
    if churn.get("lifetime_dist", "exponential") not in ("exponential", "pareto"):
        errs.append("churn.lifetime_dist must be exponential or pareto")
    return errs


@dataclass
class LaunchOptions:
    peer: str = DEFAULT_PEER
    tracker: str | None = DEFAULT_TRACKER  # None => an external tracker is already running
    tracker_extra_args: str = ""
    peer_extra_args: str = ""
    make_torrent: str | None = None
    sim_flags: str = "auto"  # auto | on | off  (stub-only flags such as --latency-ms)
    keep_dirs: bool = False
    work_root: Path = DEFAULT_WORK_ROOT
    logs_root: Path = DEFAULT_LOGS_ROOT
    echo: bool = True
    on_event: Callable[[dict], None] | None = None


# --------------------------------------------------------------------------- runtime


@dataclass
class ProcInfo:
    node: str
    role: str  # seed | leech | tracker
    port: int
    workdir: Path | None
    proc: subprocess.Popen
    argv: list[str]
    start_ts: float
    stderr_file: TextIO | None = None
    end_reason: str = ""
    end_ts: float | None = None
    returncode: int | None = None
    reader: threading.Thread | None = None

    def summary(self) -> dict[str, Any]:
        return {
            "node": self.node,
            "role": self.role,
            "port": self.port,
            "pid": self.proc.pid,
            "returncode": self.returncode,
            "end_reason": self.end_reason or "self_exit",
            "start_ts": round(self.start_ts, 4),
            "end_ts": round(self.end_ts, 4) if self.end_ts else None,
            "dir": str(self.workdir) if self.workdir else None,
            "argv": self.argv,
        }


@dataclass
class RunResult:
    name: str
    log_path: Path
    work_dir: Path
    torrent_path: Path
    source_path: Path
    source_sha256: str
    torrent: TorrentInfo
    tracker_url: str
    processes: list[dict[str, Any]] = field(default_factory=list)
    churn_events: list[dict[str, Any]] = field(default_factory=list)
    churn_fingerprint: str = ""
    duration_sec: float = 0.0
    completed_nodes: list[str] = field(default_factory=list)

    def leech_dirs(self) -> dict[str, Path]:
        return {p["node"]: Path(p["dir"]) for p in self.processes if p["role"] == "leech" and p.get("dir")}


class ScenarioRunner:
    def __init__(self, scenario: str | Path | dict, options: LaunchOptions | None = None):
        self.cfg = load_scenario(scenario)
        self.opt = options or LaunchOptions()
        self.name = str(self.cfg["name"])
        self.peer_ep = Entrypoint.parse(self.opt.peer)
        self.tracker_ep = Entrypoint.parse(self.opt.tracker) if self.opt.tracker else None
        self.sim_flags = {"on": True, "off": False}.get(self.opt.sim_flags, self.peer_ep.is_stub_peer)

        swarm = self.cfg.get("swarm", {}) or {}
        sim = self.cfg.get("simulation", {}) or {}
        tcfg = self.cfg.get("tracker", {}) or {}
        self.num_seeds = int(swarm.get("num_seeds", 1))
        self.num_leechers = int(swarm.get("num_leechers", 3))
        self.base_port = int(swarm.get("base_port", 9001))
        self.leech_exit = bool(swarm.get("leech_exit_on_complete", False))
        self.leech_delay = float(swarm.get("leech_start_delay_sec", 0.0))
        self.duration = float(sim.get("duration_sec", 20))
        self.stop_when_complete = bool(sim.get("stop_when_complete", True))
        self.linger = float(sim.get("linger_after_complete_sec", 0.5))
        self.tracker_host = str(tcfg.get("host", "127.0.0.1"))
        self.tracker_port = int(tcfg.get("port", 8000))
        self.tracker_interval = int(tcfg.get("interval_sec", 2))

        netem_cfg = dict(self.cfg.get("netem", {}) or {})
        netem_cfg.setdefault("default_max_up_kbps", int(swarm.get("max_up_kbps", 0) or 0))
        self.netem = NetworkEmulator(netem_cfg, self.cfg.get("faults", {}) or {})
        self.schedule: list[ChurnEvent] = build_schedule(self.cfg)

        self.work_dir = Path(self.opt.work_root) / self.name
        self.log_dir = Path(self.opt.logs_root) / self.name
        self.log_path = self.log_dir / "events.jsonl"
        self.stderr_dir = self.log_dir / "stderr"
        self.tracker_dir = self.work_dir / "_tracker"

        self._log_lock = threading.Lock()
        self._log_file: TextIO | None = None
        self._noise_file: TextIO | None = None
        self._proc_lock = threading.RLock()
        self.procs: dict[str, ProcInfo] = {}
        self.history: list[ProcInfo] = []
        self.completed: set[str] = set()
        self.stop_event = threading.Event()
        self.threads: list[threading.Thread] = []
        self.start_time = 0.0
        self.torrent_path = self.work_dir / f"{self.name}.torrent.json"
        self.torrent: TorrentInfo | None = None
        self.source_path = Path()
        self.tracker_proc: ProcInfo | None = None
        self.tracker_restarts = 0
        self.pending_actions = 0

    # ------------------------------------------------------------------ logging
    def emit(self, record: dict) -> None:
        line = json.dumps(record, ensure_ascii=False)
        with self._log_lock:
            if self._log_file:
                self._log_file.write(line + "\n")
                self._log_file.flush()
            if self.opt.echo:
                try:
                    print(line, flush=True)
                except (OSError, ValueError):
                    pass
        if record.get("event") == "completed":
            node = (record.get("meta") or {}).get("node") or record.get("peer_id")
            self.completed.add(str(node))
        if self.opt.on_event:
            try:
                self.opt.on_event(record)
            except Exception:  # a dashboard bug must never break the run
                pass

    def sim_event(self, event: str, chunk: int | None = None, **meta: Any) -> None:
        self.emit(make_event("simulator", event, chunk, meta))

    def _reader(self, info: ProcInfo) -> None:
        assert info.proc.stdout is not None
        for raw in info.proc.stdout:
            line = raw.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                ok = isinstance(rec, dict) and "event" in rec and "ts" in rec
            except json.JSONDecodeError:
                ok = False
            if not ok:
                with self._log_lock:
                    if self._noise_file:
                        self._noise_file.write(f"[{info.node}] {line}\n")
                        self._noise_file.flush()
                continue
            rec.setdefault("peer_id", info.node)
            rec.setdefault("chunk_index", None)
            meta = rec.get("meta")
            if not isinstance(meta, dict):
                meta = {} if meta is None else {"value": meta}
                rec["meta"] = meta
            if rec["peer_id"] != info.node:
                meta["node"] = info.node  # real peers may use 20-char ids
            self.emit(rec)
        try:
            info.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            hard_kill(info.proc)
        info.returncode = info.proc.returncode
        info.end_ts = time.time()
        if info.stderr_file:
            try:
                info.stderr_file.close()
            except OSError:
                pass
        self.sim_event(
            "process_exit",
            target=info.node,
            role=info.role,
            returncode=info.returncode,
            end_reason=info.end_reason or "self_exit",
        )

    # ------------------------------------------------------------------ processes
    def _start(self, node: str, role: str, port: int, argv: list[str], workdir: Path | None) -> ProcInfo:
        self.stderr_dir.mkdir(parents=True, exist_ok=True)
        err = open(self.stderr_dir / f"{node}.log", "a", encoding="utf-8")
        proc = spawn(argv, stderr=err)
        info = ProcInfo(node, role, port, workdir, proc, argv, time.time(), err)
        t = threading.Thread(target=self._reader, args=(info,), daemon=True, name=f"reader-{node}")
        info.reader = t
        t.start()
        with self._proc_lock:
            self.procs[node] = info
            self.history.append(info)
        self.sim_event("spawned", target=node, role=role, port=port, pid=proc.pid)
        return info

    def spawn_peer(self, node: str, role: str, port: int) -> ProcInfo:
        assert self.torrent is not None
        peer_dir = self.work_dir / node
        peer_dir.mkdir(parents=True, exist_ok=True)
        if role == "seed":
            target = peer_dir / self.torrent.name
            if not target.exists():
                shutil.copyfile(self.source_path, target)
        argv = self.peer_ep.argv() + [
            "--torrent", str(self.torrent_path),
            "--port", str(port),
            "--dir", str(peer_dir),
            "--role", role,
            "--peer-id", node,
        ]
        argv += self.netem.peer_args(node, role, self.sim_flags)
        if role == "leech" and self.leech_exit:
            argv.append("--exit-on-complete")
        argv += split_args(self.opt.peer_extra_args)
        return self._start(node, role, port, argv, peer_dir)

    # tracker supervisor (TrackerControl protocol used by netem.TrackerFaultInjector)
    def _tracker_argv(self) -> list[str]:
        assert self.tracker_ep is not None
        argv = self.tracker_ep.argv() + ["--host", self.tracker_host, "--port", str(self.tracker_port)]
        if self.tracker_ep.is_stub_tracker:
            argv += ["--interval", str(self.tracker_interval), "--stop-file", str(self.tracker_dir / STOP_FILE_NAME)]
        argv += split_args(self.opt.tracker_extra_args)
        return argv

    def start_tracker(self) -> bool:
        if self.tracker_ep is None:
            return True
        self.tracker_dir.mkdir(parents=True, exist_ok=True)
        try:
            (self.tracker_dir / STOP_FILE_NAME).unlink()
        except OSError:
            pass
        info = self._start("tracker", "tracker", self.tracker_port, self._tracker_argv(), self.tracker_dir)
        self.tracker_proc = info
        ok = wait_for_port(self.tracker_host, self.tracker_port, timeout=8.0, proc=info.proc)
        if not ok:
            self.sim_event("tracker_restarted", ok=False, error="tracker did not open its port")
        return ok

    def crash(self) -> bool:
        info = self.tracker_proc
        if info is None or info.proc.poll() is not None:
            return False
        info.end_reason = "fault_tracker_crash"
        hard_kill(info.proc)
        return True

    def restart(self) -> bool:
        if self.stop_event.is_set():
            return False
        self.tracker_restarts += 1
        return self.start_tracker()

    # ------------------------------------------------------------------ preparation
    def prepare(self) -> None:
        robust_rmtree(self.work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        robust_rmtree(self.stderr_dir)
        for old in ("processes.json",):
            try:
                (self.log_dir / old).unlink()
            except OSError:
                pass

        if self.tracker_ep is not None and not is_port_free(self.tracker_port, self.tracker_host):
            new_port = find_free_port(self.tracker_host)
            print(f"[launcher] tracker port {self.tracker_port} busy -> using {new_port}", file=sys.stderr)
            self.tracker_port = new_port
        static_ports = [self.base_port + i for i in range(self.num_seeds + self.num_leechers)]
        static_ports += [e.port for e in self.schedule if e.action == "join"]
        busy = [p for p in static_ports if not is_port_free(p, "0.0.0.0")]
        if busy:
            raise RuntimeError(f"peer ports already in use: {busy} (close old peers or change swarm.base_port)")

        tcfg = self.cfg.get("torrent", {}) or {}
        file_name = str(tcfg.get("file_name", "sample_data.bin"))
        size = int(tcfg.get("file_size_bytes", 1048576))
        plen = int(tcfg.get("piece_length", 262144))
        data_seed = str(tcfg.get("data_seed", "ds02"))
        self.source_path = write_deterministic_file(self.work_dir / "_source" / file_name, size, data_seed)
        announce = f"http://{self.tracker_host}:{self.tracker_port}/announce"

        if self.opt.make_torrent:
            ep = Entrypoint.parse(self.opt.make_torrent)
            argv = ep.argv() + [str(self.source_path), "--announce", announce, "--piece-length", str(plen), "-o", str(self.torrent_path)]
            res = subprocess.run(argv, capture_output=True, text=True, encoding="utf-8", errors="replace", cwd=str(REPO_ROOT), env=child_env())
            if res.returncode != 0:
                raise RuntimeError(f"make-torrent failed ({res.returncode}): {res.stderr.strip()}")
            self.torrent = load_metainfo_any(self.torrent_path)
            creator = str(ep)
        else:
            self.torrent = build_metainfo(self.source_path, announce, plen, self.torrent_path)
            creator = "builtin"
        self.sim_event(
            "torrent_created",
            path=str(self.torrent_path),
            info_hash=self.torrent.info_hash,
            num_pieces=self.torrent.num_pieces,
            piece_length=self.torrent.piece_length,
            file_size_bytes=self.torrent.length,
            sha256=sha256_file(self.source_path),
            created_by=creator,
        )

    # ------------------------------------------------------------------ scheduled actions
    def _wait_until(self, rel_sec: float) -> bool:
        deadline = self.start_time + rel_sec
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                return not self.stop_event.is_set()
            if self.stop_event.wait(min(remaining, 0.2)):
                return False

    def _leave(self, node: str, mode: str, reason: str) -> None:
        with self._proc_lock:
            info = self.procs.get(node)
        if info is None or info.proc.poll() is not None:
            return
        info.end_reason = reason
        if mode == "crash":
            hard_kill(info.proc)
        else:
            threading.Thread(target=graceful_stop, args=(info.proc, info.workdir, 4.0), daemon=True).start()

    def churn_worker(self) -> None:
        try:
            for ev in self.schedule:
                if not self._wait_until(ev.timestamp):
                    return
                if ev.action == "join":
                    self.sim_event("churn_join", target=ev.peer_id, port=ev.port, cause=ev.cause)
                    self.spawn_peer(ev.peer_id, ev.role, ev.port)
                else:
                    with self._proc_lock:
                        info = self.procs.get(ev.peer_id)
                    if info is None or info.proc.poll() is not None:
                        continue  # already gone (e.g. finished with --exit-on-complete)
                    if ev.action == "crash":
                        self.sim_event("churn_crash", target=ev.peer_id, cause=ev.cause)
                        self._leave(ev.peer_id, "crash", "churn_crash")
                    else:
                        self.sim_event("churn_leave", target=ev.peer_id, cause=ev.cause)
                        self._leave(ev.peer_id, "graceful", "churn_leave")
        finally:
            with self._proc_lock:
                self.pending_actions -= 1

    def fault_worker(self) -> None:
        try:
            for k in self.netem.peer_kills:
                if not self._wait_until(k.at_sec):
                    return
                self.sim_event("fault_kill_peer", target=k.target, mode=k.mode)
                self._leave(k.target, k.mode, f"fault_{k.mode}")
        finally:
            with self._proc_lock:
                self.pending_actions -= 1

    def tracker_fault_worker(self) -> None:
        try:
            TrackerFaultInjector(
                self.netem.tracker_fault,
                self,
                lambda ev, meta: self.emit(make_event("tracker", ev, None, meta)),
                self.stop_event,
            ).run(self.start_time)
        finally:
            with self._proc_lock:
                self.pending_actions -= 1

    def _bg(self, target: Callable[[], None], name: str) -> None:
        with self._proc_lock:
            self.pending_actions += 1
        t = threading.Thread(target=target, daemon=True, name=name)
        self.threads.append(t)
        t.start()

    def _all_done(self) -> bool:
        with self._proc_lock:
            if self.pending_actions > 0:
                return False
            leeches = [i for i in self.procs.values() if i.role == "leech"]
        if not leeches:
            return False
        return all(i.node in self.completed or i.proc.poll() is not None for i in leeches)

    # ------------------------------------------------------------------ main
    def run(self) -> RunResult:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._log_file = open(self.log_path, "w", encoding="utf-8")
        self._noise_file = open(self.log_dir / "noise.log", "w", encoding="utf-8")
        result: RunResult | None = None
        try:
            self.prepare()
            assert self.torrent is not None
            self.start_time = time.time()
            self.sim_event(
                "scenario_start",
                scenario=self.name,
                peer=str(self.peer_ep),
                tracker=str(self.tracker_ep) if self.tracker_ep else "external",
                seeds=self.num_seeds,
                leechers=self.num_leechers,
                duration_sec=self.duration,
                churn_events=len(self.schedule),
                churn_fingerprint=schedule_fingerprint(self.schedule),
                random_seed=int((self.cfg.get("simulation") or {}).get("random_seed", 42)),
            )
            if not self.start_tracker():
                raise RuntimeError("tracker failed to start – see logs/<scenario>/stderr/tracker.log")

            for i in range(self.num_seeds):
                port = self.base_port + i
                self.spawn_peer(f"seed-{port}", "seed", port)
            time.sleep(0.3)
            for j in range(self.num_leechers):
                port = self.base_port + self.num_seeds + j
                self.spawn_peer(f"leech-{port}", "leech", port)
                if self.leech_delay > 0:
                    time.sleep(self.leech_delay)

            if self.schedule:
                self._bg(self.churn_worker, "churn")
            if self.netem.peer_kills:
                self._bg(self.fault_worker, "peer-faults")
            if self.netem.tracker_fault.enabled and self.tracker_ep is not None:
                self._bg(self.tracker_fault_worker, "tracker-fault")

            while time.time() - self.start_time < self.duration:
                if self.stop_when_complete and self._all_done():
                    self.stop_event.wait(self.linger)
                    break
                if self.stop_event.wait(0.2):
                    break
        finally:
            elapsed = time.time() - self.start_time if self.start_time else 0.0
            self.shutdown()
            self.sim_event(
                "scenario_end",
                scenario=self.name,
                elapsed_sec=round(elapsed, 3),
                completed=sorted(self.completed),
                tracker_restarts=self.tracker_restarts,
            )
            processes = [p.summary() for p in self.history]
            (self.log_dir / "processes.json").write_text(json.dumps(processes, indent=2), encoding="utf-8")
            with self._log_lock:
                for f in (self._log_file, self._noise_file):
                    if f:
                        f.close()
                self._log_file = self._noise_file = None
            if self.torrent is not None:
                result = RunResult(
                    name=self.name,
                    log_path=self.log_path,
                    work_dir=self.work_dir,
                    torrent_path=self.torrent_path,
                    source_path=self.source_path,
                    source_sha256=sha256_file(self.source_path) if self.source_path.exists() else "",
                    torrent=self.torrent,
                    tracker_url=self.torrent.announce,
                    processes=processes,
                    churn_events=[e.to_dict() for e in self.schedule],
                    churn_fingerprint=schedule_fingerprint(self.schedule),
                    duration_sec=round(elapsed, 3),
                    completed_nodes=sorted(self.completed),
                )
            if not self.opt.keep_dirs:
                robust_rmtree(self.work_dir)
        assert result is not None
        return result

    def shutdown(self) -> None:
        """Graceful stop of every peer in parallel, tracker last, then join readers."""
        self.stop_event.set()
        for t in self.threads:
            t.join(timeout=2.0)
        with self._proc_lock:
            peers = [i for i in self.procs.values() if i.role != "tracker" and i.proc.poll() is None]
            tracker = self.tracker_proc
        stoppers = []
        for info in peers:
            if not info.end_reason:
                info.end_reason = "scenario_end"
            t = threading.Thread(target=graceful_stop, args=(info.proc, info.workdir, 4.0), daemon=True)
            t.start()
            stoppers.append(t)
        for t in stoppers:
            t.join(timeout=6.0)
        if tracker is not None and tracker.proc.poll() is None:
            tracker.end_reason = tracker.end_reason or "scenario_end"
            graceful_stop(tracker.proc, tracker.workdir, 3.0)
        for info in list(self.history):
            if info.proc.poll() is None:
                hard_kill(info.proc)
            if info.reader:
                info.reader.join(timeout=3.0)


def run_scenario(
    scenario: str | Path | dict,
    peer_script: str = DEFAULT_PEER,
    keep_dirs: bool = False,
    **kwargs: Any,
) -> RunResult:
    """Backward-compatible helper used by run_experiments, tests and the demo."""
    opts = LaunchOptions(peer=peer_script, keep_dirs=keep_dirs, **kwargs)
    return ScenarioRunner(scenario, opts).run()


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="DS02 scenario orchestrator (C1 + C2 churn + C3 netem)")
    p.add_argument("--scenario", default="scenarios/baseline.yaml", help="scenario YAML")
    p.add_argument("--peer-script", default=DEFAULT_PEER, help='peer entrypoint: script path or "-m peer" (Role B)')
    p.add_argument("--tracker-script", default=DEFAULT_TRACKER, help='tracker entrypoint: script path or "-m tracker" (Role A)')
    p.add_argument("--no-tracker", action="store_true", help="do not start a tracker (one is already running)")
    p.add_argument("--tracker-args", default="", help="extra args appended to the tracker command")
    p.add_argument("--peer-args", default="", help="extra args appended to every peer command")
    p.add_argument("--make-torrent-script", default=None, help='Role B make-torrent, e.g. "-m torrent.make_torrent"')
    p.add_argument("--sim-flags", choices=["auto", "on", "off"], default="auto", help="pass stub-only flags (--latency-ms, --corrupt-pieces)")
    p.add_argument("--work-root", default=str(DEFAULT_WORK_ROOT), help="where peer directories are created")
    p.add_argument("--logs-root", default=str(DEFAULT_LOGS_ROOT))
    p.add_argument("--keep-dirs", action="store_true", help="keep peer directories after the run")
    p.add_argument("--quiet", action="store_true", help="do not echo events to the console")
    return p


def main(argv: list[str] | None = None) -> int:
    configure_utf8_stdio()
    args = build_arg_parser().parse_args(argv)
    opts = LaunchOptions(
        peer=args.peer_script,
        tracker=None if args.no_tracker else args.tracker_script,
        tracker_extra_args=args.tracker_args,
        peer_extra_args=args.peer_args,
        make_torrent=args.make_torrent_script,
        sim_flags=args.sim_flags,
        keep_dirs=args.keep_dirs,
        work_root=Path(args.work_root),
        logs_root=Path(args.logs_root),
        echo=not args.quiet,
    )
    runner = ScenarioRunner(args.scenario, opts)
    print(f"=== [launcher] scenario '{runner.name}' | peer={runner.peer_ep} | tracker={runner.tracker_ep or 'external'} ===", file=sys.stderr)
    res = runner.run()
    print(
        f"=== [launcher] done in {res.duration_sec}s | completed {len(res.completed_nodes)} | log: {res.log_path} ===",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
