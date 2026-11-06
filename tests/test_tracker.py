"""Comprehensive tests for the tracker module (SwarmManager + TrackerServer).

Covers tasks A1, A2, and A3:
- SwarmManager unit tests (announce, scrape, TTL cleanup, concurrency)
- TrackerServer integration tests (real HTTP requests)
"""

from __future__ import annotations

import json
import socket
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from src.tracker.swarm import PeerInfo, SwarmManager
from src.tracker.server import TrackerServer


# ======================================================================
# helpers
# ======================================================================

def _free_port() -> int:
    """Return an unused TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _get_json(url: str) -> dict:
    """GET *url* and parse the response as JSON."""
    with urlopen(url, timeout=5) as resp:
        return json.loads(resp.read().decode())


# ======================================================================
# SwarmManager unit tests
# ======================================================================

class TestSwarmManagerAnnounce(unittest.TestCase):
    """Tests for SwarmManager.announce()."""

    def setUp(self) -> None:
        self.sm = SwarmManager(default_interval=30, ttl=90)

    # --- started ---
    def test_announce_started_registers_peer(self) -> None:
        result = self.sm.announce(
            info_hash="aaa", peer_id="p1", ip="1.2.3.4", port=6881,
            event="started", left=1000,
        )
        self.assertEqual(result["interval"], 30)
        self.assertEqual(result["incomplete"], 1)
        self.assertEqual(result["complete"], 0)
        # The announcing peer should NOT appear in its own peer list.
        self.assertEqual(len(result["peers"]), 0)

    # --- completed ---
    def test_announce_completed_marks_seeder(self) -> None:
        self.sm.announce("aaa", "p1", "1.2.3.4", 6881, "started", left=1000)
        result = self.sm.announce("aaa", "p1", "1.2.3.4", 6881, "completed", left=0)
        self.assertEqual(result["complete"], 1)
        self.assertEqual(result["incomplete"], 0)

    # --- stopped ---
    def test_announce_stopped_removes_peer(self) -> None:
        self.sm.announce("aaa", "p1", "1.2.3.4", 6881, "started", left=100)
        self.sm.announce("aaa", "p1", "1.2.3.4", 6881, "stopped")
        scrape = self.sm.scrape("aaa")
        stats = scrape["files"]["aaa"]
        self.assertEqual(stats["complete"], 0)
        self.assertEqual(stats["incomplete"], 0)

    # --- periodic re-announce ---
    def test_periodic_announce_updates_last_announce(self) -> None:
        self.sm.announce("aaa", "p1", "1.2.3.4", 6881, "started", left=100)
        # Grab the initial timestamp.
        peer_before = self.sm._swarms["aaa"]["p1"]
        ts_before = peer_before.last_announce
        time.sleep(0.05)
        self.sm.announce("aaa", "p1", "1.2.3.4", 6881, "", left=50)
        ts_after = self.sm._swarms["aaa"]["p1"].last_announce
        self.assertGreater(ts_after, ts_before)

    # --- peer excluded from own response ---
    def test_peer_excluded_from_own_peer_list(self) -> None:
        self.sm.announce("aaa", "p1", "1.2.3.4", 6881, "started", left=0)
        self.sm.announce("aaa", "p2", "5.6.7.8", 6882, "started", left=100)
        result = self.sm.announce("aaa", "p1", "1.2.3.4", 6881, "")
        peer_ids = [p["peer_id"] for p in result["peers"]]
        self.assertNotIn("p1", peer_ids)
        self.assertIn("p2", peer_ids)

    # --- max 50 peers ---
    def test_max_50_peers_returned(self) -> None:
        for i in range(80):
            self.sm.announce("aaa", f"peer-{i}", "10.0.0.1", 7000 + i,
                             "started", left=100)
        result = self.sm.announce("aaa", "requester", "10.0.0.2", 9999,
                                  "started", left=100)
        self.assertLessEqual(len(result["peers"]), 50)
        # The requester must not be in the list.
        self.assertTrue(
            all(p["peer_id"] != "requester" for p in result["peers"])
        )


class TestSwarmManagerScrape(unittest.TestCase):
    """Tests for SwarmManager.scrape()."""

    def setUp(self) -> None:
        self.sm = SwarmManager(default_interval=30, ttl=90)

    def test_scrape_single_swarm(self) -> None:
        self.sm.announce("h1", "p1", "1.1.1.1", 6881, "started", left=0)
        self.sm.announce("h1", "p2", "2.2.2.2", 6882, "started", left=100)
        result = self.sm.scrape("h1")
        self.assertIn("h1", result["files"])
        self.assertEqual(result["files"]["h1"]["complete"], 1)
        self.assertEqual(result["files"]["h1"]["incomplete"], 1)

    def test_scrape_all_swarms(self) -> None:
        self.sm.announce("h1", "p1", "1.1.1.1", 6881, "started", left=0)
        self.sm.announce("h2", "p2", "2.2.2.2", 6882, "started", left=100)
        result = self.sm.scrape()
        self.assertIn("h1", result["files"])
        self.assertIn("h2", result["files"])

    def test_scrape_downloaded_counter(self) -> None:
        self.sm.announce("h1", "p1", "1.1.1.1", 6881, "started", left=100)
        self.sm.announce("h1", "p1", "1.1.1.1", 6881, "completed", left=0)
        result = self.sm.scrape("h1")
        self.assertEqual(result["files"]["h1"]["downloaded"], 1)

    def test_scrape_unknown_hash(self) -> None:
        result = self.sm.scrape("nonexistent")
        self.assertEqual(result["files"]["nonexistent"]["complete"], 0)
        self.assertEqual(result["files"]["nonexistent"]["incomplete"], 0)
        self.assertEqual(result["files"]["nonexistent"]["downloaded"], 0)


class TestSwarmManagerCleanup(unittest.TestCase):
    """Tests for SwarmManager.cleanup_expired()."""

    def test_cleanup_removes_stale_peers(self) -> None:
        sm = SwarmManager(default_interval=30, ttl=1)
        sm.announce("aaa", "p1", "1.2.3.4", 6881, "started", left=100)
        # Nothing expired yet.
        self.assertEqual(sm.cleanup_expired(), [])
        # Wait for the TTL to expire.
        time.sleep(1.1)
        removed = sm.cleanup_expired()
        self.assertEqual(len(removed), 1)
        self.assertEqual(removed[0], ("aaa", "p1"))

    def test_cleanup_keeps_fresh_peers(self) -> None:
        sm = SwarmManager(default_interval=30, ttl=5)
        sm.announce("aaa", "p1", "1.2.3.4", 6881, "started", left=100)
        removed = sm.cleanup_expired()
        self.assertEqual(removed, [])


class TestSwarmManagerConcurrency(unittest.TestCase):
    """Concurrent access to SwarmManager (A3)."""

    def test_concurrent_100_peers(self) -> None:
        sm = SwarmManager(default_interval=30, ttl=90)
        errors: list[Exception] = []

        def _announce(i: int) -> dict:
            return sm.announce(
                info_hash="hash1",
                peer_id=f"peer-{i}",
                ip="10.0.0.1",
                port=7000 + i,
                event="started",
                left=1000,
            )

        with ThreadPoolExecutor(max_workers=20) as pool:
            futures = [pool.submit(_announce, i) for i in range(100)]
            for f in as_completed(futures):
                try:
                    result = f.result()
                    self.assertIn("interval", result)
                    self.assertIn("peers", result)
                except Exception as exc:
                    errors.append(exc)

        self.assertEqual(errors, [])

        # After all 100 have announced, scrape should show 100 leechers.
        scrape = sm.scrape("hash1")
        self.assertEqual(
            scrape["files"]["hash1"]["complete"]
            + scrape["files"]["hash1"]["incomplete"],
            100,
        )


# ======================================================================
# TrackerServer integration tests (real HTTP)
# ======================================================================

class TestTrackerServerHTTP(unittest.TestCase):
    """Integration tests that start a real TrackerServer on a random port."""

    server: TrackerServer
    port: int
    base: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.port = _free_port()
        cls.server = TrackerServer(
            host="127.0.0.1", port=cls.port, interval=30, ttl=2,
        )
        cls.server.start()
        cls.base = f"http://127.0.0.1:{cls.port}"
        # Give the server a moment to bind.
        time.sleep(0.3)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.stop()

    # --- /announce ---
    def test_announce_valid(self) -> None:
        url = (
            f"{self.base}/announce?"
            "info_hash=abc&peer_id=peer1&port=6881&event=started&left=1000"
        )
        data = _get_json(url)
        self.assertIn("interval", data)
        self.assertIn("peers", data)
        self.assertEqual(data["interval"], 30)

    def test_announce_missing_params_returns_400(self) -> None:
        url = f"{self.base}/announce?info_hash=abc"
        with self.assertRaises(HTTPError) as ctx:
            _get_json(url)
        self.assertEqual(ctx.exception.code, 400)

    def test_announce_uses_client_ip_when_no_ip_param(self) -> None:
        url = (
            f"{self.base}/announce?"
            "info_hash=def&peer_id=p2&port=6882&event=started&left=500"
        )
        _get_json(url)
        # The peer should have been registered with 127.0.0.1.
        swarm = self.server.swarm_manager._swarms.get("def", {})
        self.assertIn("p2", swarm)
        self.assertEqual(swarm["p2"].ip, "127.0.0.1")

    def test_announce_explicit_ip_param(self) -> None:
        url = (
            f"{self.base}/announce?"
            "info_hash=ghi&peer_id=p3&port=6883&event=started&left=500"
            "&ip=192.168.1.100"
        )
        _get_json(url)
        swarm = self.server.swarm_manager._swarms.get("ghi", {})
        self.assertIn("p3", swarm)
        self.assertEqual(swarm["p3"].ip, "192.168.1.100")

    def test_announce_stopped_removes_peer(self) -> None:
        base_url = (
            f"{self.base}/announce?"
            "info_hash=hhh&peer_id=p_stop&port=6890"
        )
        _get_json(base_url + "&event=started&left=1000")
        _get_json(base_url + "&event=stopped")
        swarm = self.server.swarm_manager._swarms.get("hhh", {})
        self.assertNotIn("p_stop", swarm)

    # --- /scrape ---
    def test_scrape(self) -> None:
        # Seed some data.
        _get_json(
            f"{self.base}/announce?"
            "info_hash=xyz&peer_id=s1&port=7001&event=started&left=0"
        )
        data = _get_json(f"{self.base}/scrape?info_hash=xyz")
        self.assertIn("files", data)
        self.assertIn("xyz", data["files"])
        self.assertEqual(data["files"]["xyz"]["complete"], 1)

    def test_scrape_all(self) -> None:
        _get_json(
            f"{self.base}/announce?"
            "info_hash=aaa_scrape&peer_id=sa&port=7010&event=started&left=0"
        )
        _get_json(
            f"{self.base}/announce?"
            "info_hash=bbb_scrape&peer_id=sb&port=7011&event=started&left=100"
        )
        data = _get_json(f"{self.base}/scrape")
        self.assertIn("aaa_scrape", data["files"])
        self.assertIn("bbb_scrape", data["files"])

    # --- 404 ---
    def test_unknown_path_returns_404(self) -> None:
        with self.assertRaises(HTTPError) as ctx:
            _get_json(f"{self.base}/nonexistent")
        self.assertEqual(ctx.exception.code, 404)

    # --- TTL expiration over HTTP ---
    def test_ttl_expiration(self) -> None:
        # The server has ttl=2.  Register a peer and wait for it to expire.
        _get_json(
            f"{self.base}/announce?"
            "info_hash=ttl_test&peer_id=ttl_peer&port=7777"
            "&event=started&left=100"
        )
        # Confirm the peer is there.
        scrape1 = _get_json(f"{self.base}/scrape?info_hash=ttl_test")
        self.assertEqual(scrape1["files"]["ttl_test"]["incomplete"], 1)

        # Wait for TTL + a cleanup cycle.  Cleanup runs every `interval`
        # seconds but we set interval=30 which is too long for a test.
        # Instead, manually trigger cleanup.
        time.sleep(2.2)
        self.server.swarm_manager.cleanup_expired()

        scrape2 = _get_json(f"{self.base}/scrape?info_hash=ttl_test")
        self.assertEqual(scrape2["files"]["ttl_test"]["incomplete"], 0)
        self.assertEqual(scrape2["files"]["ttl_test"]["complete"], 0)

    # --- concurrent HTTP requests ---
    def test_concurrent_http_announces(self) -> None:
        errors: list[Exception] = []

        def _do_announce(i: int) -> dict:
            url = (
                f"{self.base}/announce?"
                f"info_hash=conc_http&peer_id=cp{i}&port={8000 + i}"
                f"&event=started&left=999"
            )
            return _get_json(url)

        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = [pool.submit(_do_announce, i) for i in range(50)]
            for f in as_completed(futures):
                try:
                    result = f.result()
                    self.assertIn("peers", result)
                except Exception as exc:
                    errors.append(exc)

        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
