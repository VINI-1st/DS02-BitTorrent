"""C7 – Single-command live demo with a real-time terminal dashboard.

    py demo/run_demo.py                       # StubPeer + StubTracker
    py demo/run_demo.py --peer-script "-m peer" --tracker-script "-m tracker"   # real A + B

The demo runs ``scenarios/demo_live.yaml`` through the normal launcher (C1) and
redraws a dashboard from the live Contract 5 event stream. Every checklist item
of the mandatory demonstration (spec section 6.4) is ticked only when the
corresponding *real* event is observed:

    1. Create metadata and share a file      torrent_created + a seed's announce_ok
    2. Parallel download from >= 2 peers      one leecher got piece_done from >= 2 sources
    3. Upload before completing download      piece_uploaded by a leecher not yet completed
    4. Stop one source peer while downloading fault_kill_peer (seed-9002 killed at t=4s)
    5. Switch to another source automatically switch_source after peer_lost
    6. Detect and reject an invalid chunk     invalid_chunk (seed-9003 serves corrupted data)
    7. Merge the file and verify its hash     every leecher's completed.sha256 == source SHA-256,
                                              re-checked on the files on disk at the end

Exit code 0 when all 7 items are checked, 1 otherwise.
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from collections import deque
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.sim.common import configure_utf8_stdio, robust_rmtree, sha256_file  # noqa: E402
from src.sim.launcher import DEFAULT_PEER, DEFAULT_TRACKER, LaunchOptions, RunResult, ScenarioRunner  # noqa: E402

CHECKS = [
    ("metadata", "1. Create metadata (.torrent.json) & share the file via the tracker"),
    ("parallel", "2. Download in parallel from at least two peers"),
    ("upload_early", "3. A leecher uploads chunks before finishing its own download"),
    ("stop_source", "4. Stop one source peer during downloading"),
    ("switch", "5. Switch to another source peer automatically"),
    ("invalid", "6. Detect & reject an invalid chunk (SHA-1 mismatch)"),
    ("verify", "7. Merge chunks & verify the final file hash (SHA-256)"),
]

ANSI = {"reset": "\033[0m", "bold": "\033[1m", "dim": "\033[2m", "green": "\033[32m", "red": "\033[31m", "yellow": "\033[33m", "cyan": "\033[36m"}


class Dashboard:
    def __init__(self, color: bool = True):
        self.color = color
        self.lock = threading.Lock()
        self.num_pieces = 0
        self.info_hash = ""
        self.source_sha = ""
        self.t0 = time.time()
        self.roles: dict[str, str] = {}
        self.status: dict[str, str] = {}
        self.have: dict[str, set[int]] = {}
        self.uploaded: dict[str, int] = {}
        self.sources: dict[str, set[str]] = {}
        self.completed_sha: dict[str, str] = {}
        self.events: deque[str] = deque(maxlen=10)
        self.checks: dict[str, str] = {}  # key -> evidence text
        self.tracker_state = "starting"
        self.seed_announced = False

    def c(self, text: str, *styles: str) -> str:
        if not self.color:
            return text
        return "".join(ANSI[s] for s in styles) + text + ANSI["reset"]

    def _tick(self, key: str, evidence: str) -> None:
        if key not in self.checks:
            self.checks[key] = evidence
            self.events.append(f"CHECK {key}: {evidence}")

    def _say(self, text: str) -> None:
        self.events.append(f"[{time.time() - self.t0:5.1f}s] {text}")

    def on_event(self, rec: dict) -> None:
        with self.lock:
            ev = rec.get("event")
            meta = rec.get("meta") or {}
            pid = str(meta.get("node") or rec.get("peer_id"))
            ci = rec.get("chunk_index")
            if pid == "simulator":
                target = str(meta.get("target", ""))
                if ev == "scenario_start":
                    self.t0 = float(rec["ts"])
                elif ev == "torrent_created":
                    self.num_pieces = int(meta.get("num_pieces", 0))
                    self.info_hash = str(meta.get("info_hash", ""))
                    self.source_sha = str(meta.get("sha256", ""))
                    self._say(f"metainfo created: {self.num_pieces} pieces, info_hash {self.info_hash[:12]}…")
                elif ev == "spawned" and meta.get("role") in ("seed", "leech"):
                    self.roles[target] = str(meta["role"])
                    self.status[target] = "STARTING"
                    self.have.setdefault(target, set())
                    self.uploaded.setdefault(target, 0)
                elif ev in ("fault_kill_peer", "churn_crash"):
                    self.status[target] = "CRASHED"
                    self._say(f"FAULT: {target} killed ({meta.get('mode', 'crash')})")
                    if self.roles.get(target) == "seed":
                        self._tick("stop_source", f"{target} killed while leechers were downloading")
                elif ev == "churn_leave":
                    self.status[target] = "LEAVING"
                elif ev == "process_exit" and target in self.roles:
                    if self.status.get(target) not in ("CRASHED",):
                        rc = meta.get("returncode")
                        self.status[target] = f"EXITED rc={rc}"
                return
            if pid == "tracker":
                if ev == "started":
                    self.tracker_state = "UP"
                elif ev == "tracker_crashed":
                    self.tracker_state = "DOWN"
                elif ev == "tracker_restarted":
                    self.tracker_state = "UP (restarted)"
                elif ev == "stopped":
                    self.tracker_state = "stopped"
                return

            role = self.roles.get(pid, str(meta.get("role", "leech")))
            if ev == "started":
                self.roles.setdefault(pid, role)
                self.status[pid] = "SEEDING" if role == "seed" else "DOWNLOADING"
                if role == "seed":
                    self.have[pid] = set(range(self.num_pieces))
                self._say(f"{pid} started as {role}")
            elif ev == "announce_ok":
                if role == "seed" and not self.seed_announced and self.num_pieces:
                    self.seed_announced = True
                    self._tick("metadata", f"{self.num_pieces}-piece torrent, {pid} announced to tracker")
            elif ev == "piece_done" and isinstance(ci, int):
                self.have.setdefault(pid, set()).add(ci)
                src = str(meta.get("from_peer", "?"))
                srcs = self.sources.setdefault(pid, set())
                srcs.add(src)
                self._say(f"{pid} got chunk #{ci} from {src} (SHA-1 ok)")
                if len(srcs) >= 2:
                    self._tick("parallel", f"{pid} downloaded from {', '.join(sorted(srcs)[:3])}")
            elif ev == "piece_uploaded":
                self.uploaded[pid] = self.uploaded.get(pid, 0) + 1
                if role == "leech" and pid not in self.completed_sha:
                    have = len(self.have.get(pid, set()))
                    self._tick("upload_early", f"{pid} served chunk #{ci} to {meta.get('to_peer')} at {have}/{self.num_pieces}")
            elif ev == "invalid_chunk":
                self._say(f"REJECTED chunk #{ci} from {meta.get('from_peer')} (hash mismatch)")
                self._tick("invalid", f"{pid} rejected chunk #{ci} from {meta.get('from_peer')}")
            elif ev == "peer_lost":
                self._say(f"{pid} lost source {meta.get('peer')} ({meta.get('reason')})")
            elif ev == "switch_source":
                self._say(f"{pid} chunk #{ci}: {meta.get('old_peer')} -> {meta.get('new_peer')} ({meta.get('reason')})")
                if meta.get("reason") == "peer_lost":
                    self._tick("switch", f"{pid} moved chunk #{ci} from {meta.get('old_peer')} to {meta.get('new_peer')}")
            elif ev == "completed":
                self.completed_sha[pid] = str(meta.get("sha256", ""))
                self.have[pid] = set(range(self.num_pieces))
                self.status[pid] = "COMPLETED"
                ok = self.completed_sha[pid] == self.source_sha
                self._say(f"{pid} merged file, SHA-256 {'matches' if ok else 'MISMATCH'}")
                leeches = [p for p, r in self.roles.items() if r == "leech"]
                if leeches and all(self.completed_sha.get(p) == self.source_sha for p in leeches):
                    self._tick("verify", f"{len(leeches)}/{len(leeches)} leechers SHA-256 = {self.source_sha[:16]}…")
            elif ev == "stopped":
                if self.status.get(pid) not in ("CRASHED",) and not str(self.status.get(pid, "")).startswith("EXITED"):
                    self.status[pid] = "STOPPED"

    def render(self) -> str:
        with self.lock:
            n = max(1, self.num_pieces)
            online = sum(1 for s in self.status.values() if s in ("DOWNLOADING", "SEEDING", "COMPLETED"))
            w = 96
            out = []
            out.append("=" * w)
            out.append(self.c(" DS02 BitTorrent P2P – LIVE SWARM DASHBOARD (Role C · C7)", "bold"))
            out.append("=" * w)
            out.append(
                f" t = {time.time() - self.t0:5.1f}s | active peers {online}/{len(self.roles)} | tracker {self.tracker_state} | "
                f"pieces {self.num_pieces} | info_hash {self.info_hash[:12]}…"
            )
            out.append("-" * w)
            out.append(f" {'PEER':<12} {'ROLE':<6} {'STATUS':<14} {'BITFIELD':<{min(n, 32) + 2}} {'PROGRESS':<14} {'UP':>4} {'SRC':>4}")
            out.append("-" * w)
            order = sorted(self.roles, key=lambda p: (self.roles[p] != "seed", p))
            for pid in order:
                have = self.have.get(pid, set())
                bits = "".join("#" if i in have else "." for i in range(n))[:32]
                pct = int(100 * len(have) / n)
                status = self.status.get(pid, "?")
                st_col = {"CRASHED": ("red",), "COMPLETED": ("green",), "SEEDING": ("cyan",), "DOWNLOADING": ("yellow",)}.get(status, ("dim",))
                out.append(
                    f" {pid:<12} {self.roles[pid]:<6} {self.c(f'{status:<14}', *st_col)} [{bits}] "
                    f"{len(have):>2}/{n:<2} ({pct:>3}%) {self.uploaded.get(pid, 0):>5} {len(self.sources.get(pid, set())):>4}"
                )
            out.append("-" * w)
            out.append(self.c(" MANDATORY DEMONSTRATION CHECKLIST (spec 6.4)", "bold"))
            for key, label in CHECKS:
                if key in self.checks:
                    out.append(f"  {self.c('[x]', 'green', 'bold')} {label}")
                    out.append(self.c(f"        ↳ {self.checks[key]}", "dim"))
                else:
                    out.append(f"  [ ] {label}")
            out.append("-" * w)
            out.append(self.c(" RECENT SWARM EVENTS", "bold"))
            for e in list(self.events)[-8:]:
                out.append(f"   > {e[: w - 6]}")
            out.append("=" * w)
            return "\n".join(out)


def enable_ansi() -> None:
    if os.name == "nt":
        os.system("")  # turns on VT100 escape processing in the Windows console


def main(argv: list[str] | None = None) -> int:
    configure_utf8_stdio()
    p = argparse.ArgumentParser(description="DS02 single-command live demo (C7)")
    p.add_argument("--scenario", default=str(REPO_ROOT / "scenarios" / "demo_live.yaml"))
    p.add_argument("--peer-script", default=DEFAULT_PEER)
    p.add_argument("--tracker-script", default=DEFAULT_TRACKER)
    p.add_argument("--tracker-args", default="", help='extra tracker args, e.g. "--interval 2 --ttl 6"')
    p.add_argument("--refresh", type=float, default=0.3, help="dashboard refresh period (s)")
    p.add_argument("--no-clear", action="store_true", help="do not clear the screen (logs / CI)")
    p.add_argument("--no-color", action="store_true")
    args = p.parse_args(argv)

    interactive = sys.stdout.isatty() and not args.no_clear
    enable_ansi()
    dash = Dashboard(color=not args.no_color and sys.stdout.isatty())
    opts = LaunchOptions(peer=args.peer_script, tracker=args.tracker_script, tracker_extra_args=args.tracker_args, keep_dirs=True, echo=False, on_event=dash.on_event)
    runner = ScenarioRunner(args.scenario, opts)
    holder: dict[str, RunResult | BaseException] = {}

    def _run() -> None:
        try:
            holder["result"] = runner.run()
        except BaseException as exc:  # surface errors in the main thread
            holder["error"] = exc

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    last_plain = 0.0
    try:
        while t.is_alive():
            if interactive:
                sys.stdout.write("\033[H\033[2J" + dash.render() + "\n")
                sys.stdout.flush()
            elif time.time() - last_plain > 3.0:
                print(dash.render(), flush=True)
                last_plain = time.time()
            t.join(args.refresh)
    except KeyboardInterrupt:
        print("\n[demo] interrupted – stopping all peers ...", flush=True)
        runner.stop_event.set()
        t.join(15)

    if "error" in holder:
        print(f"[demo] scenario failed: {holder['error']}", file=sys.stderr)
        return 1
    res = holder.get("result")
    assert isinstance(res, RunResult)

    # final on-disk verification of item 7
    ok_files = []
    for node, d in res.leech_dirs().items():
        f = d / res.torrent.name
        ok_files.append((node, f.exists() and sha256_file(f) == res.source_sha256))
    if ok_files and all(ok for _, ok in ok_files):
        with dash.lock:
            dash._tick("verify", f"{len(ok_files)} files on disk match SHA-256 {res.source_sha256[:16]}…")
    elif "verify" in dash.checks:
        with dash.lock:
            dash.checks.pop("verify")
    robust_rmtree(res.work_dir)

    if interactive:
        sys.stdout.write("\033[H\033[2J")
    print(dash.render())
    passed = sum(1 for k, _ in CHECKS if k in dash.checks)
    print(f"\n Source SHA-256 : {res.source_sha256}")
    for node, ok in ok_files:
        print(f"   {node:<12} file hash {'OK' if ok else 'MISMATCH'}")
    print(f"\n RESULT: {passed}/{len(CHECKS)} mandatory scenarios demonstrated in {res.duration_sec:.1f}s")
    print(f" Event log: {res.log_path}")
    return 0 if passed == len(CHECKS) else 1


if __name__ == "__main__":
    sys.exit(main())
