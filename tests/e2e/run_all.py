"""C5 – Full automated test suite with one command and a PASS/FAIL table.

    py tests/e2e/run_all.py                     # everything (unit + E2E), about 2-3 minutes
    py tests/e2e/run_all.py --quick             # unit tests + fast fault-injection E2E only
    py tests/e2e/run_all.py --peer-script "-m peer" --tracker-script "-m tracker"   # against A + B

Categories covered (spec section 9): Functional, Concurrency, Fault & Recovery,
Security / invalid data, Performance. Every E2E test starts real OS processes,
then checks the Contract 5 log, the exit codes (Contract 4) and the SHA-256 /
SHA-1 of every downloaded file against the seeder's original.

Logs of the E2E runs go to ``logs/e2e/<test>/``; CSVs to ``results/e2e/``.
Exit code 0 when every test passes.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import subprocess
import sys
import time
import traceback
import unittest
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "src"))

from src.sim.common import configure_utf8_stdio, read_jsonl, robust_rmtree, validate_event  # noqa: E402
from src.sim.launcher import DEFAULT_PEER, DEFAULT_TRACKER, LaunchOptions, RunResult, ScenarioRunner  # noqa: E402
from src.sim.metrics import analyze_and_export  # noqa: E402
from src.sim.procutil import child_env  # noqa: E402

E2E_LOGS = REPO / "logs" / "e2e"
E2E_RESULTS = REPO / "results" / "e2e"


class Check(Exception):
    pass


def require(cond: bool, msg: str) -> None:
    if not cond:
        raise Check(msg)


@dataclass
class Outcome:
    name: str
    category: str
    ok: bool
    seconds: float
    detail: str


class Suite:
    def __init__(self, peer: str, tracker: str, verbose: bool, tracker_args: str = ""):
        self.peer = peer
        self.tracker = tracker
        self.tracker_args = tracker_args
        self.verbose = verbose
        self.outcomes: list[Outcome] = []
        self.cache: dict[str, dict] = {}

    # ----------------------------------------------------------- helpers
    def run(self, scenario, keep: bool = True) -> RunResult:
        opts = LaunchOptions(peer=self.peer, tracker=self.tracker, tracker_extra_args=self.tracker_args, keep_dirs=keep, echo=self.verbose, logs_root=E2E_LOGS)
        return ScenarioRunner(scenario, opts).run()

    @staticmethod
    def events(res: RunResult) -> list[dict]:
        return read_jsonl(res.log_path)

    @staticmethod
    def count(evts: list[dict], name: str, **meta) -> int:
        return sum(1 for e in evts if e.get("event") == name and all((e.get("meta") or {}).get(k) == v for k, v in meta.items()))

    @staticmethod
    def verify_files(res: RunResult) -> tuple[int, list[str]]:
        """Return (#verified, problems): every completed leecher's file vs source SHA-256 and per-piece SHA-1."""
        problems = []
        verified = 0
        src = res.source_path.read_bytes()
        dirs = res.leech_dirs()
        for node in res.completed_nodes:
            if node not in dirs:
                continue
            f = dirs[node] / res.torrent.name
            if not f.exists():
                problems.append(f"{node}: file missing")
                continue
            data = f.read_bytes()
            if hashlib.sha256(data).hexdigest() != res.source_sha256 or data != src:
                problems.append(f"{node}: SHA-256 mismatch")
                continue
            for i in range(res.torrent.num_pieces):
                piece = data[i * res.torrent.piece_length:(i + 1) * res.torrent.piece_length]
                if hashlib.sha1(piece).hexdigest() != res.torrent.pieces[i]:
                    problems.append(f"{node}: piece {i} SHA-1 mismatch")
                    break
            else:
                verified += 1
        return verified, problems

    @staticmethod
    def schema_problems(res: RunResult) -> list[str]:
        out = []
        for i, rec in enumerate(read_jsonl(res.log_path, strict=True)):
            for p in validate_event(rec):
                out.append(f"line {i + 1}: {p}")
        return out

    def leech_rows(self, res: RunResult) -> list[dict]:
        return [p for p in res.processes if p["role"] == "leech"]

    def add(self, name: str, category: str, fn: Callable[[], str]) -> None:
        t0 = time.time()
        print(f"  ... {name:<52}", end="", flush=True)
        try:
            detail = fn() or ""
            ok = True
        except Check as exc:
            ok, detail = False, str(exc)
        except Exception as exc:  # unexpected crash of the test itself
            ok, detail = False, f"{type(exc).__name__}: {exc}"
            if self.verbose:
                traceback.print_exc()
        dt = time.time() - t0
        print(f"\r  {'PASS' if ok else 'FAIL'} {name:<52} {dt:6.1f}s", flush=True)
        self.outcomes.append(Outcome(name, category, ok, dt, detail))

    # ----------------------------------------------------------- unit tests
    def unit(self, module: str, label: str) -> str:
        suite = unittest.defaultTestLoader.loadTestsFromName(module)
        buf = io.StringIO()
        result = unittest.TextTestRunner(stream=buf, verbosity=0).run(suite)
        require(result.wasSuccessful(), f"{len(result.failures)} failure(s), {len(result.errors)} error(s): " + buf.getvalue()[-400:].replace("\n", " "))
        return f"{result.testsRun} {label} unit tests passed"

    # ----------------------------------------------------------- E2E tests
    def t_contract4_exit_code(self) -> str:
        cfg = {
            "name": "e2e_exit_code",
            "torrent": {"file_name": "exit.bin", "file_size_bytes": 600_000, "piece_length": 65536},
            "swarm": {"num_seeds": 1, "num_leechers": 2, "base_port": 9401, "max_up_kbps": 0, "leech_exit_on_complete": True},
            "simulation": {"duration_sec": 30, "random_seed": 1},
        }
        res = self.run(cfg)
        try:
            leeches = self.leech_rows(res)
            require(len(leeches) == 2, "expected 2 leechers")
            for p in leeches:
                require(p["returncode"] == 0 and p["end_reason"] == "self_exit", f"{p['node']} rc={p['returncode']} reason={p['end_reason']} (expected self exit 0)")
            n, probs = self.verify_files(res)
            require(not probs and n == 2, f"hash check: {probs or n}")
            return "leechers exited by themselves with code 0; files match SHA-256"
        finally:
            robust_rmtree(res.work_dir)

    def t_scenario(self, name: str, extra: Callable[[RunResult, list[dict], dict], str]) -> str:
        res = self.run(REPO / "scenarios" / f"{name}.yaml")
        try:
            probs = self.schema_problems(res)
            require(not probs, f"Contract 5 violations: {probs[:3]}")
            n, hp = self.verify_files(res)
            require(not hp, f"file verification failed: {hp[:3]}")
            require(n == len([c for c in res.completed_nodes if c in res.leech_dirs()]), "not every completed leecher was verified")
            summary = analyze_and_export(name, res.log_path, E2E_RESULTS, quiet=True)
            self.cache[name] = summary
            more = extra(res, self.events(res), summary)
            return f"{n} file(s) SHA-256+SHA-1 OK; {more}"
        finally:
            robust_rmtree(res.work_dir)

    def x_baseline(self, res, ev, s) -> str:
        require(s["completion_ratio_pct"] == 100.0, f"completion {s['completion_ratio_pct']}%")
        require(self.count(ev, "piece_uploaded") > 0, "no uploads logged")
        return f"100% in {s['mean_download_time_sec']}s"

    def x_multi_seed(self, res, ev, s) -> str:
        require(s["completion_ratio_pct"] == 100.0, f"completion {s['completion_ratio_pct']}%")
        base = self.cache.get("baseline", {}).get("mean_download_time_sec")
        if base:
            require(float(s["mean_download_time_sec"]) < float(base), f"multi_seed {s['mean_download_time_sec']}s not faster than baseline {base}s")
            return f"{s['mean_download_time_sec']}s vs baseline {base}s ({float(base) / float(s['mean_download_time_sec']):.2f}x faster)"
        return f"{s['mean_download_time_sec']}s"

    def x_high_churn(self, res, ev, s) -> str:
        static = {p["node"] for p in res.processes if p["role"] == "leech" and p["port"] < 9100}
        require(static and static <= set(res.completed_nodes), f"static leechers not all complete: {static - set(res.completed_nodes)}")
        crashes = self.count(ev, "churn_crash")
        leaves = self.count(ev, "churn_leave")
        require(crashes > 0 and leaves > 0, f"expected both crashes and graceful leaves (got {crashes}/{leaves})")
        return f"{crashes} crash + {leaves} graceful leave, completion {s['completion_ratio_pct']}%, peer_lost {s['peer_lost_events']}"

    def x_flash_crowd(self, res, ev, s) -> str:
        joins = self.count(ev, "churn_join", cause="flash_crowd")
        require(joins == 8, f"expected 8 flash-crowd joins, got {joins}")
        require(s["completed_leechers"] >= 1, "nobody completed")
        return f"8 burst joins, {s['completed_leechers']}/{s['total_leechers']} completed, peak {s['peak_tracker_announces_per_bucket']} announces/s"

    def x_tracker_crash(self, res, ev, s) -> str:
        crashed = [e for e in ev if e["event"] == "tracker_crashed"]
        restarted = [e for e in ev if e["event"] == "tracker_restarted" and (e.get("meta") or {}).get("ok")]
        require(crashed and restarted, "tracker crash/restart events missing")
        trackers = [p for p in res.processes if p["role"] == "tracker"]
        require(len(trackers) == 2 and trackers[0]["end_reason"] == "fault_tracker_crash", "tracker OS process was not killed and respawned")
        after = [e for e in ev if e["event"] == "announce_ok" and e["ts"] > restarted[0]["ts"]]
        require(after, "no announce after the tracker restart")
        require(s["completion_ratio_pct"] == 100.0, f"completion {s['completion_ratio_pct']}%")
        require(s["total_tracker_announces_failed"] > 0, "no failed announce while tracker was down")
        return f"down {s['tracker_downtime_sec']}s, {s['total_tracker_announces_failed']} failed announces, recovered in {s['tracker_recovery_sec']}s, 100% complete"

    def t_invalid_chunk(self) -> str:
        cfg = {
            "name": "e2e_invalid_chunk",
            "torrent": {"file_name": "bad.bin", "file_size_bytes": 1_048_576, "piece_length": 131072},
            "swarm": {"num_seeds": 2, "num_leechers": 2, "base_port": 9421, "max_up_kbps": 512},
            "faults": {"corrupt_peers": {"seed-9422": "all"}},
            "simulation": {"duration_sec": 40, "random_seed": 3},
        }
        res = self.run(cfg)
        try:
            ev = self.events(res)
            inv = [e for e in ev if e["event"] == "invalid_chunk"]
            require(inv, "corrupted chunks were not detected")
            require(all((e.get("meta") or {}).get("from_peer") == "seed-9422" for e in inv), "invalid_chunk blamed the wrong peer")
            done_bad = [e for e in ev if e["event"] == "piece_done" and (e.get("meta") or {}).get("from_peer") == "seed-9422"]
            require(not done_bad, "a corrupted piece was accepted")
            n, probs = self.verify_files(res)
            require(n == 2 and not probs, f"after rejection files must still be correct: {probs or n}")
            banned = sum(1 for e in inv if (e.get("meta") or {}).get("banned"))
            return f"{len(inv)} corrupted chunk(s) rejected, {banned} ban(s), 0 accepted; final files OK"
        finally:
            robust_rmtree(res.work_dir)

    def t_failover(self) -> str:
        cfg = {
            "name": "e2e_failover",
            "torrent": {"file_name": "fo.bin", "file_size_bytes": 1_048_576, "piece_length": 131072},
            "swarm": {"num_seeds": 2, "num_leechers": 2, "base_port": 9431, "max_up_kbps": 128},
            "faults": {"kill_peers": [{"target": "seed-9432", "at_sec": 2.0, "mode": "crash"}]},
            "simulation": {"duration_sec": 45, "random_seed": 4},
        }
        res = self.run(cfg)
        try:
            ev = self.events(res)
            lost = [e for e in ev if e["event"] == "peer_lost" and (e.get("meta") or {}).get("peer") == "seed-9432"]
            sw = [e for e in ev if e["event"] == "switch_source" and (e.get("meta") or {}).get("old_peer") == "seed-9432"]
            require(lost, "no peer_lost for the killed source")
            require(sw, "no switch_source away from the killed source")
            require(min(e["ts"] for e in sw) >= min(e["ts"] for e in lost) - 0.01, "switch_source happened before peer_lost")
            n, probs = self.verify_files(res)
            require(n == 2 and not probs, f"downloads did not finish correctly: {probs or n}")
            return f"{len(lost)} peer_lost -> {len(sw)} switch_source; both leechers finished with correct hash"
        finally:
            robust_rmtree(res.work_dir)

    def t_graceful_vs_crash(self) -> str:
        cfg = {
            "name": "e2e_leave_types",
            "torrent": {"file_name": "lv.bin", "file_size_bytes": 2_097_152, "piece_length": 131072},
            "swarm": {"num_seeds": 1, "num_leechers": 2, "base_port": 9441, "max_up_kbps": 64},
            "faults": {"kill_peers": [{"target": "leech-9442", "at_sec": 2.0, "mode": "graceful"}, {"target": "leech-9443", "at_sec": 2.0, "mode": "crash"}]},
            "simulation": {"duration_sec": 6, "random_seed": 5, "stop_when_complete": False},
        }
        res = self.run(cfg, keep=False)
        ev = self.events(res)
        procs = {p["node"]: p for p in res.processes}
        g_stopped = [e for e in ev if e["event"] == "stopped" and e["peer_id"] == "leech-9442"]
        c_stopped = [e for e in ev if e["event"] == "stopped" and e["peer_id"] == "leech-9443"]
        require(g_stopped, "graceful leave did not log 'stopped'")
        require((g_stopped[0].get("meta") or {}).get("reason") in ("signal", "stop_file"), f"unexpected stop reason {g_stopped[0].get('meta')}")
        require(procs["leech-9442"]["returncode"] == 130, f"incomplete graceful leech should exit 130, got {procs['leech-9442']['returncode']}")
        require(not c_stopped, "crashed peer must not log 'stopped'")
        require(procs["leech-9443"]["returncode"] not in (0, 130), f"crashed peer exit code {procs['leech-9443']['returncode']}")
        return f"graceful: 'stopped' + exit 130; crash: no 'stopped', exit {procs['leech-9443']['returncode']}"

    def t_bandwidth(self) -> str:
        size = 786_432  # 768 KiB
        def one(limit: int, port: int) -> float:
            cfg = {
                "name": f"e2e_bw_{limit}",
                "torrent": {"file_name": "bw.bin", "file_size_bytes": size, "piece_length": 65536},
                "swarm": {"num_seeds": 1, "num_leechers": 1, "base_port": port, "max_up_kbps": limit},
                "simulation": {"duration_sec": 40, "random_seed": 6},
            }
            res = self.run(cfg, keep=False)
            s = analyze_and_export(cfg["name"], res.log_path, E2E_RESULTS, quiet=True)
            require(s["completion_ratio_pct"] == 100.0, f"limit {limit}: download did not finish")
            return float(s["mean_download_time_sec"])
        limited = one(192, 9451)
        unlimited = one(0, 9461)
        expected = size / (192 * 1024)
        require(limited >= expected * 0.9, f"192 KiB/s run took {limited:.2f}s < expected {expected:.2f}s")
        require(limited <= expected * 1.6 + 1.0, f"192 KiB/s run too slow: {limited:.2f}s (expected ~{expected:.2f}s)")
        require(unlimited < limited / 2, f"unlimited {unlimited:.2f}s not clearly faster than limited {limited:.2f}s")
        return f"cap 192 KiB/s: {limited:.2f}s (theory {expected:.2f}s, {size / 1024 / limited:.0f} KiB/s measured); unlimited {unlimited:.2f}s"

    def t_churn_cli_determinism(self) -> str:
        cmd = [sys.executable, str(REPO / "src/sim/churn.py"), "--scenario", str(REPO / "scenarios/high_churn.yaml")]
        a = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", env=child_env(), timeout=30)
        b = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", env=child_env(), timeout=30)
        require(a.returncode == 0 and b.returncode == 0, f"churn CLI failed: {a.stderr[-200:]}")
        require(a.stdout == b.stdout and a.stdout.strip(), "two runs with the same seed produced different schedules")
        fp = json.loads(a.stderr.strip().splitlines()[-1])
        return f"{fp['events']} events, fingerprint {fp['fingerprint'][:16]}… identical twice"

    def t_metrics_csv(self) -> str:
        required_ts = {"rel_sec", "download_kbps", "upload_kbps", "tracker_announces", "avg_chunk_replication", "completion_ratio_pct", "chunk_0_replicas"}
        required_peer = {"peer_id", "role", "download_time_sec", "completed"}
        required_sum = {"completion_ratio_pct", "mean_download_time_sec", "total_tracker_announces", "avg_chunk_replication_mean"}
        names = sorted(self.cache) or []
        require(names, "no scenario CSVs were produced (run without --quick)")
        import csv

        for n in names:
            for suffix, req in (("timeseries", required_ts), ("peers", required_peer), ("summary", required_sum)):
                path = E2E_RESULTS / f"{n}_{suffix}.csv"
                require(path.exists(), f"missing {path.name}")
                with open(path, encoding="utf-8") as f:
                    header = set(next(csv.reader(f)))
                require(req <= header, f"{path.name} lacks columns {req - header}")
        return f"{len(names) * 3} CSV files with all 5 metric families"

    # ----------------------------------------------------------- driver
    def run_all(self, quick: bool, only: list[str] | None) -> None:
        plan: list[tuple[str, str, Callable[[], str]]] = [
            ("Unit: Role C modules (src/sim)", "Functional", lambda: self.unit("tests.test_sim_modules", "Role C")),
            ("Unit: Role B metainfo (B1)", "Functional", lambda: self.unit("tests.test_metainfo", "metainfo")),
            ("Unit: Role B storage (B2)", "Security", lambda: self.unit("tests.test_storage", "storage")),
            ("Contract 4: leecher exits 0 after completing", "Functional", self.t_contract4_exit_code),
            ("Churn: same seed => same schedule (CLI)", "Functional", self.t_churn_cli_determinism),
            ("Invalid chunk detected & rejected", "Security", self.t_invalid_chunk),
            ("Source killed: peer_lost -> switch_source", "Fault & Recovery", self.t_failover),
            ("Graceful leave vs crash semantics", "Fault & Recovery", self.t_graceful_vs_crash),
            ("Bandwidth cap enforced (--max-up-kbps)", "Performance", self.t_bandwidth),
        ]
        if not quick:
            plan += [
                ("Scenario baseline (1 seed, 3 leechers)", "Functional", lambda: self.t_scenario("baseline", self.x_baseline)),
                ("Scenario multi_seed (faster than baseline)", "Performance", lambda: self.t_scenario("multi_seed", self.x_multi_seed)),
                ("Scenario high_churn (Poisson+Pareto+crash)", "Concurrency", lambda: self.t_scenario("high_churn", self.x_high_churn)),
                ("Scenario flash_crowd (burst + mass departure)", "Concurrency", lambda: self.t_scenario("flash_crowd", self.x_flash_crowd)),
                ("Scenario tracker_crash (kill + respawn)", "Fault & Recovery", lambda: self.t_scenario("tracker_crash", self.x_tracker_crash)),
                ("Metrics CSV export (C4) for every scenario", "Performance", self.t_metrics_csv),
            ]
        for name, cat, fn in plan:
            if only and not any(o.lower() in name.lower() for o in only):
                continue
            self.add(name, cat, fn)

    def print_table(self) -> bool:
        w_name = max(len(o.name) for o in self.outcomes)
        print()
        print("=" * (w_name + 44))
        print(f" {'#':>2}  {'RESULT':<6} {'TEST':<{w_name}}  {'CATEGORY':<17} {'TIME':>6}")
        print("-" * (w_name + 44))
        for i, o in enumerate(self.outcomes, 1):
            print(f" {i:>2}  {'PASS' if o.ok else 'FAIL':<6} {o.name:<{w_name}}  {o.category:<17} {o.seconds:5.1f}s")
        print("-" * (w_name + 44))
        for i, o in enumerate(self.outcomes, 1):
            print(f" {i:>2}. {o.detail}")
        passed = sum(o.ok for o in self.outcomes)
        print("=" * (w_name + 44))
        print(f" RESULT: {passed}/{len(self.outcomes)} passed — {'ALL TESTS PASSED' if passed == len(self.outcomes) else 'SOME TESTS FAILED'}")
        print(f" E2E logs: {E2E_LOGS}   E2E CSVs: {E2E_RESULTS}")
        return passed == len(self.outcomes)


def main(argv: list[str] | None = None) -> int:
    configure_utf8_stdio()
    p = argparse.ArgumentParser(description="DS02 full test suite (C5)")
    p.add_argument("--peer-script", default=DEFAULT_PEER)
    p.add_argument("--tracker-script", default=DEFAULT_TRACKER)
    p.add_argument("--tracker-args", default="", help='extra tracker args, e.g. "--interval 2 --ttl 6"')
    p.add_argument("--quick", action="store_true", help="skip the 5 full scenarios")
    p.add_argument("--only", nargs="*", help="run tests whose name contains one of these words")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args(argv)

    print("=" * 72)
    print(" DS02 BitTorrent – automated test suite (C5)")
    print(f" peer: {args.peer_script} | tracker: {args.tracker_script}")
    print("=" * 72)
    suite = Suite(args.peer_script, args.tracker_script, args.verbose, args.tracker_args)
    t0 = time.time()
    suite.run_all(args.quick, args.only)
    ok = suite.print_table()
    print(f" Total time: {time.time() - t0:.1f}s")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
