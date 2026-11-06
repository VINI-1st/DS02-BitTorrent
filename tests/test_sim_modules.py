"""Unit tests for Role C modules (src/sim). Run with either:

    py -m pytest tests/test_sim_modules.py -q
    py -m unittest tests.test_sim_modules -v
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "src"))

from src.sim.churn import ChurnGenerator, build_schedule, schedule_fingerprint  # noqa: E402
from src.sim.common import (  # noqa: E402
    build_metainfo,
    deterministic_bytes,
    load_metainfo_any,
    make_event,
    piece_hashes,
    validate_event,
    write_deterministic_file,
)
from src.sim.launcher import load_scenario, validate_scenario  # noqa: E402
from src.sim.metrics import analyze  # noqa: E402
from src.sim.netem import NetworkEmulator  # noqa: E402
from src.sim.procutil import Entrypoint, child_env, find_free_port, graceful_stop, spawn, wait_for_port  # noqa: E402
from src.sim.stub_peer import decode_bitfield, encode_bitfield, parse_corrupt_spec  # noqa: E402

STANDARD = ["baseline", "multi_seed", "high_churn", "flash_crowd", "tracker_crash", "demo_live"]


class TestLogSchema(unittest.TestCase):
    def test_make_event_is_valid_contract5(self):
        rec = make_event("leech-9002", "piece_done", 3, {"bytes": 131072, "from_peer": "seed-9001"})
        self.assertEqual(validate_event(rec, allow_extensions=False), [])
        self.assertIsInstance(rec["ts"], float)
        json.dumps(rec)  # must be serialisable

    def test_validate_event_rejects_bad_records(self):
        self.assertTrue(validate_event({"peer_id": "x", "event": "started", "chunk_index": None}))  # no ts
        self.assertTrue(validate_event({"ts": 1.0, "peer_id": "x", "event": "warp", "chunk_index": None}))
        self.assertTrue(validate_event({"ts": 1.0, "peer_id": "x", "event": "piece_done", "chunk_index": -1}))
        self.assertTrue(validate_event({"ts": "1", "peer_id": "x", "event": "started", "chunk_index": None}))
        self.assertTrue(validate_event({"ts": 1.0, "peer_id": "x", "event": "started", "chunk_index": None, "meta": []}))


class TestDataAndMetainfo(unittest.TestCase):
    def test_deterministic_bytes_reproducible(self):
        a = deterministic_bytes("f.bin", 300_000)
        self.assertEqual(a, deterministic_bytes("f.bin", 300_000))
        self.assertNotEqual(a, deterministic_bytes("g.bin", 300_000))
        self.assertEqual(len(a), 300_000)

    def test_builtin_metainfo_accepted_by_role_b_reader(self):
        from torrent import load_metainfo  # Role B's real reader

        with tempfile.TemporaryDirectory() as d:
            src = write_deterministic_file(Path(d) / "movie.bin", 1_000_003)
            info = build_metainfo(src, "http://127.0.0.1:8000/announce", 262144, Path(d) / "m.torrent.json")
            meta = load_metainfo(str(Path(d) / "m.torrent.json"))
            self.assertEqual(meta.info_hash, info.info_hash)
            self.assertEqual(meta.num_pieces, 4)
            self.assertEqual(list(meta.pieces), piece_hashes(src.read_bytes(), 262144))

    def test_loader_reads_role_b_make_torrent_output(self):
        from torrent import create_metainfo, save_metainfo

        with tempfile.TemporaryDirectory() as d:
            src = write_deterministic_file(Path(d) / "x.bin", 500_000)
            save_metainfo(create_metainfo(str(src), "http://127.0.0.1:8000/announce", 65536), str(Path(d) / "x.json"))
            ti = load_metainfo_any(Path(d) / "x.json")
            self.assertEqual(ti.num_pieces, 8)
            self.assertEqual(ti.piece_size(7), 500_000 - 7 * 65536)

    def test_loader_reads_flat_dummy_and_derives_hashes(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "dummy.json"
            p.write_text(json.dumps({"file_name": "a.bin", "file_size_bytes": 1048576, "piece_length": 262144, "tracker_url": "http://127.0.0.1:8000"}), encoding="utf-8")
            ti = load_metainfo_any(p)
            self.assertEqual(ti.num_pieces, 4)
            self.assertEqual(ti.pieces, piece_hashes(deterministic_bytes("a.bin", 1048576), 262144))
            self.assertTrue(ti.announce.endswith("/announce"))

    def test_loader_rejects_tampered_info_hash(self):
        with tempfile.TemporaryDirectory() as d:
            src = write_deterministic_file(Path(d) / "y.bin", 10_000)
            out = Path(d) / "y.json"
            build_metainfo(src, "http://h/announce", 4096, out)
            doc = json.loads(out.read_text(encoding="utf-8"))
            doc["info"]["length"] = 9999
            out.write_text(json.dumps(doc), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_metainfo_any(out)


class TestChurn(unittest.TestCase):
    def test_same_seed_same_schedule(self):
        a = ChurnGenerator(7).generate_poisson_churn(30, 0.8, 5, "pareto", 0.5, max_dynamic_peers=20)
        b = ChurnGenerator(7).generate_poisson_churn(30, 0.8, 5, "pareto", 0.5, max_dynamic_peers=20)
        self.assertEqual(a, b)
        self.assertEqual(schedule_fingerprint(a), schedule_fingerprint(b))

    def test_different_seed_different_schedule(self):
        a = ChurnGenerator(1).generate_poisson_churn(30, 0.8, 5)
        b = ChurnGenerator(2).generate_poisson_churn(30, 0.8, 5)
        self.assertNotEqual(schedule_fingerprint(a), schedule_fingerprint(b))

    def test_poisson_arrival_rate(self):
        ev = ChurnGenerator(3).generate_poisson_churn(2000, 0.5, 5, max_dynamic_peers=10_000, stop_arrivals_before_end_sec=0)
        joins = [e for e in ev if e.action == "join"]
        self.assertAlmostEqual(len(joins) / 2000, 0.5, delta=0.05)

    def test_lifetime_distribution_means(self):
        g = ChurnGenerator(11)
        exp = [g.sample_lifetime("exponential", 6.0) for _ in range(20000)]
        par = [g.sample_lifetime("pareto", 6.0, pareto_alpha=3.0) for _ in range(20000)]
        self.assertAlmostEqual(sum(exp) / len(exp), 6.0, delta=0.3)
        self.assertAlmostEqual(sum(par) / len(par), 6.0, delta=0.4)
        self.assertGreaterEqual(min(par), 6.0 * 2 / 3 - 1e-9)  # Pareto support starts at x_m

    def test_crash_probability_and_actions(self):
        ev = ChurnGenerator(5).generate_poisson_churn(5000, 1.0, 1.0, crash_prob=0.3, max_dynamic_peers=4000)
        leaves = [e for e in ev if e.action != "join"]
        crash = sum(1 for e in leaves if e.action == "crash")
        self.assertAlmostEqual(crash / len(leaves), 0.3, delta=0.04)

    def test_flash_crowd_burst_and_mass_departure(self):
        ev = ChurnGenerator(9).generate_flash_crowd(2.0, 8, 16.0, crash_prob=0.5, burst_window_sec=0.5)
        joins = [e for e in ev if e.action == "join"]
        leaves = [e for e in ev if e.action != "join"]
        self.assertEqual(len(joins), 8)
        self.assertTrue(all(2.0 <= e.timestamp <= 2.5 for e in joins))
        self.assertEqual(len(leaves), 8)
        self.assertTrue(all(16.0 <= e.timestamp <= 16.3 for e in leaves))
        self.assertEqual(len({e.port for e in joins}), 8)

    def test_build_schedule_from_scenarios_is_deterministic(self):
        cfg = load_scenario(REPO / "scenarios" / "high_churn.yaml")
        self.assertEqual(schedule_fingerprint(build_schedule(cfg)), schedule_fingerprint(build_schedule(cfg)))
        self.assertGreater(len(build_schedule(cfg)), 0)
        tc = load_scenario(REPO / "scenarios" / "tracker_crash.yaml")
        late = [e for e in build_schedule(tc) if e.cause == "late_join"]
        self.assertEqual(len(late), 2)


class TestNetemAndScenarios(unittest.TestCase):
    def test_profiles_and_flags(self):
        n = NetworkEmulator(
            {"default_max_up_kbps": 256, "latency_ms": 20, "role_profiles": {"seed": {"max_up_kbps": 128}}, "peer_profiles": {"leech-9005": {"latency_ms": 200}}},
            {"corrupt_peers": {"seed-9003": "all"}, "kill_peers": [{"target": "seed-9002", "at_sec": 4}]},
        )
        self.assertEqual(n.get_peer_profile("seed-9001", "seed").max_up_kbps, 128)
        self.assertEqual(n.get_peer_profile("leech-9004", "leech").max_up_kbps, 256)
        self.assertEqual(n.get_peer_profile("leech-9005", "leech").latency_ms, 200)
        self.assertEqual(n.peer_args("leech-9004", "leech", sim_flags=False), ["--max-up-kbps", "256"])
        self.assertIn("--corrupt-pieces", n.peer_args("seed-9003", "seed", sim_flags=True))
        self.assertEqual(n.peer_kills[0].target, "seed-9002")

    def test_tracker_fault_config(self):
        n = NetworkEmulator({"tracker_fault": {"enabled": True, "crash_at_sec": 3, "downtime_sec": 5}})
        self.assertTrue(n.tracker_fault.enabled)
        self.assertEqual((n.tracker_fault.crash_at_sec, n.tracker_fault.downtime_sec), (3.0, 5.0))
        self.assertFalse(NetworkEmulator({}).tracker_fault.enabled)

    def test_all_standard_scenarios_valid(self):
        for name in STANDARD:
            with self.subTest(name=name):
                cfg = load_scenario(REPO / "scenarios" / f"{name}.yaml")
                self.assertEqual(cfg["name"], name)
                self.assertEqual(validate_scenario(cfg), [])
        self.assertTrue(validate_scenario({"name": "", "swarm": {"num_seeds": 0}}))

    def test_entrypoint_parsing(self):
        self.assertEqual(Entrypoint.parse("-m peer").argv()[-2:], ["-m", "peer"])
        self.assertEqual(Entrypoint.parse("peer").kind, "module")
        ep = Entrypoint.parse("src/sim/stub_peer.py")
        self.assertEqual(ep.kind, "script")
        self.assertTrue(ep.is_stub_peer)
        self.assertIn(str(REPO / "src"), child_env()["PYTHONPATH"])


class TestStubWireHelpers(unittest.TestCase):
    def test_bitfield_roundtrip(self):
        have = [True, False, True, True, False, False, False, True, True, False, True]
        bf = encode_bitfield(have)
        self.assertEqual(decode_bitfield(bf, len(have)), {i for i, h in enumerate(have) if h})

    def test_corrupt_spec(self):
        self.assertEqual(parse_corrupt_spec("all", 4), {0, 1, 2, 3})
        self.assertEqual(parse_corrupt_spec("0, 2,9", 4), {0, 2})
        self.assertEqual(parse_corrupt_spec(None, 4), set())


class TestMetrics(unittest.TestCase):
    def _log(self):
        t = 1000.0
        E = lambda dt, pid, ev, ci=None, **m: make_event(pid, ev, ci, m, ts=t + dt)  # noqa: E731
        return [
            E(0.0, "simulator", "scenario_start"),
            E(0.1, "simulator", "spawned", target="seed-1", role="seed"),
            E(0.1, "simulator", "spawned", target="leech-2", role="leech"),
            E(0.1, "simulator", "spawned", target="leech-3", role="leech"),
            E(0.2, "seed-1", "started", role="seed", num_pieces=2),
            E(0.3, "seed-1", "announce_ok"),
            E(0.4, "leech-2", "started", role="leech", num_pieces=2),
            E(0.4, "leech-3", "started", role="leech", num_pieces=2),
            E(0.5, "leech-2", "announce_ok"),
            E(1.2, "leech-2", "piece_done", 0, bytes=1024, from_peer="seed-1"),
            E(1.2, "seed-1", "piece_uploaded", 0, bytes=1024, to_peer="leech-2"),
            E(1.5, "leech-3", "piece_done", 0, bytes=1024, from_peer="leech-2"),
            E(1.5, "leech-2", "piece_uploaded", 0, bytes=1024, to_peer="leech-3"),
            E(2.1, "tracker", "tracker_crashed"),
            E(2.2, "leech-3", "announce_failed"),
            E(2.4, "leech-2", "piece_done", 1, bytes=1024, from_peer="seed-1"),
            E(2.4, "leech-2", "completed", sha256="x"),
            E(3.1, "tracker", "tracker_restarted"),
            E(3.3, "leech-3", "announce_ok"),
            E(3.5, "simulator", "process_exit", target="leech-3", role="leech", returncode=-9, end_reason="churn_crash"),
            E(4.0, "simulator", "scenario_end"),
        ]

    def test_summary_numbers(self):
        s = analyze(self._log())["summary"]
        self.assertEqual(s["total_leechers"], 2)
        self.assertEqual(s["completed_leechers"], 1)
        self.assertEqual(s["completion_ratio_pct"], 50.0)
        self.assertAlmostEqual(s["mean_download_time_sec"], 2.0, places=3)
        self.assertEqual(s["total_tracker_announces"], 3)
        self.assertEqual(s["total_tracker_announces_failed"], 1)
        self.assertAlmostEqual(s["tracker_downtime_sec"], 1.0, places=3)
        self.assertAlmostEqual(s["tracker_recovery_sec"], 0.2, places=3)
        self.assertEqual(s["leecher_upload_share_pct"], 50.0)

    def test_replication_timeseries(self):
        rows = analyze(self._log())["timeseries"]
        self.assertEqual(rows[0]["chunk_0_replicas"], 1)  # only the seed
        self.assertEqual(rows[1]["chunk_0_replicas"], 3)  # seed + both leechers
        self.assertEqual(rows[2]["chunk_1_replicas"], 2)  # seed + completed leech-2
        self.assertEqual(rows[3]["chunk_0_replicas"], 2)  # leech-3 crashed at 3.5s
        self.assertEqual(rows[2]["tracker_up"], 0)
        peers = {p["peer_id"]: p for p in analyze(self._log())["peers"]}
        self.assertEqual(peers["leech-3"]["exit_code"], -9)
        self.assertEqual(peers["leech-2"]["uploaded_before_complete"], 1)


class TestStubProcesses(unittest.TestCase):
    """Spawns the real StubTracker / StubPeer processes (fast, loopback only)."""

    def test_stub_tracker_announce_and_scrape(self):
        port = find_free_port()
        proc = spawn([sys.executable, str(REPO / "src/sim/stub_tracker.py"), "--port", str(port), "--interval", "1"], stderr=subprocess.DEVNULL)
        try:
            self.assertTrue(wait_for_port("127.0.0.1", port, 6, proc))
            base = f"http://127.0.0.1:{port}"
            ih = "a" * 40
            get = lambda q: json.loads(urllib.request.urlopen(base + q, timeout=3).read())  # noqa: E731
            get(f"/announce?info_hash={ih}&peer_id=p1&port=1&event=started&left=0")
            r = get(f"/announce?info_hash={ih}&peer_id=p2&port=2&event=started&left=100")
            self.assertEqual(r["interval"], 1)
            self.assertEqual([p["peer_id"] for p in r["peers"]], ["p1"])  # caller excluded
            sc = get(f"/scrape?info_hash={ih}")["files"][ih]
            self.assertEqual((sc["complete"], sc["incomplete"]), (1, 1))
            get(f"/announce?info_hash={ih}&peer_id=p1&port=1&event=stopped&left=0")
            self.assertEqual(get(f"/announce?info_hash={ih}&peer_id=p2&port=2&left=100")["peers"], [])
        finally:
            how = graceful_stop(proc, None, 5)
            out = proc.stdout.read() if proc.stdout else ""
        events = [json.loads(line)["event"] for line in out.splitlines() if line.strip()]
        self.assertEqual(events[0], "started")
        if os.name != "nt":
            self.assertEqual(how, "graceful")
            self.assertEqual(events[-1], "stopped")

    def test_stub_peer_rejects_invalid_metainfo_exit_1(self):
        with tempfile.TemporaryDirectory() as d:
            bad = Path(d) / "bad.json"
            bad.write_text("{not json", encoding="utf-8")
            r = subprocess.run([sys.executable, str(REPO / "src/sim/stub_peer.py"), "--torrent", str(bad), "--port", str(find_free_port()), "--dir", d, "--role", "leech"],
                               capture_output=True, text=True, timeout=20, env=child_env())
            self.assertEqual(r.returncode, 1)
            self.assertEqual(json.loads(r.stdout.strip().splitlines()[-1])["meta"]["reason"], "invalid_metainfo")

    def test_stub_seed_with_wrong_data_exits_2(self):
        with tempfile.TemporaryDirectory() as d:
            src = write_deterministic_file(Path(d) / "src" / "f.bin", 50_000)
            build_metainfo(src, "http://127.0.0.1:1/announce", 16384, Path(d) / "t.json")
            seed_dir = Path(d) / "seed"
            seed_dir.mkdir()
            (seed_dir / "f.bin").write_bytes(b"\x00" * 50_000)
            r = subprocess.run([sys.executable, str(REPO / "src/sim/stub_peer.py"), "--torrent", str(Path(d) / "t.json"), "--port", str(find_free_port()), "--dir", str(seed_dir), "--role", "seed"],
                               capture_output=True, text=True, timeout=20, env=child_env())
            self.assertEqual(r.returncode, 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
