"""
Tests for the Tracker Client library (Task A6).

Tests the TrackerClient against a real (local) tracker server,
and also tests backoff/retry behavior with a flaky server.
"""

import asyncio
import json
import threading
import time
import unittest
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from src.trackerclient.client import TrackerClient, TrackerClientConfig, AnnounceResponse


class MockTrackerHandler(BaseHTTPRequestHandler):
    """Simple mock tracker for testing."""

    # Class-level state shared across requests
    peers: list[dict] = []
    interval: int = 5
    fail_count: int = 0  # number of requests to fail before succeeding
    request_log: list[dict] = []
    _request_counter: int = 0

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        # Log the request
        self.__class__.request_log.append({
            "path": parsed.path,
            "params": {k: v[0] for k, v in params.items()},
            "time": time.time(),
        })

        if parsed.path == "/announce":
            self._handle_announce(params)
        elif parsed.path == "/scrape":
            self._handle_scrape(params)
        else:
            self.send_error(404)

    def _handle_announce(self, params):
        # Check if we should fail this request
        self.__class__._request_counter += 1
        if self.__class__.fail_count > 0:
            self.__class__.fail_count -= 1
            self.send_error(500, "Simulated failure")
            return

        # Validate required params
        for required in ("info_hash", "peer_id", "port"):
            if required not in params:
                self.send_error(400, f"Missing {required}")
                return

        response = {
            "interval": self.__class__.interval,
            "complete": 1,
            "incomplete": 2,
            "peers": self.__class__.peers,
        }

        body = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_scrape(self, params):
        response = {"files": {}}
        body = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        """Suppress request logging during tests."""
        pass


class TestAnnounceResponse(unittest.TestCase):
    """Test AnnounceResponse parsing."""

    def test_from_dict_full(self):
        data = {
            "interval": 60,
            "complete": 5,
            "incomplete": 10,
            "peers": [
                {"peer_id": "p1", "ip": "1.2.3.4", "port": 9001},
                {"peer_id": "p2", "ip": "5.6.7.8", "port": 9002},
            ],
        }
        resp = AnnounceResponse.from_dict(data)
        self.assertEqual(resp.interval, 60)
        self.assertEqual(resp.complete, 5)
        self.assertEqual(resp.incomplete, 10)
        self.assertEqual(len(resp.peers), 2)

    def test_from_dict_defaults(self):
        resp = AnnounceResponse.from_dict({})
        self.assertEqual(resp.interval, 30)
        self.assertEqual(resp.complete, 0)
        self.assertEqual(resp.incomplete, 0)
        self.assertEqual(resp.peers, [])


class TestTrackerClient(unittest.IsolatedAsyncioTestCase):
    """Integration tests for TrackerClient against a mock tracker."""

    @classmethod
    def setUpClass(cls):
        """Start a mock tracker server."""
        # Reset handler state
        MockTrackerHandler.peers = [
            {"peer_id": "seed-1", "ip": "127.0.0.1", "port": 9001},
            {"peer_id": "seed-2", "ip": "127.0.0.1", "port": 9002},
        ]
        MockTrackerHandler.interval = 5
        MockTrackerHandler.fail_count = 0
        MockTrackerHandler.request_log = []
        MockTrackerHandler._request_counter = 0

        cls.server = HTTPServer(("127.0.0.1", 0), MockTrackerHandler)
        cls.port = cls.server.server_address[1]
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def _make_config(self, **overrides) -> TrackerClientConfig:
        defaults = {
            "tracker_url": f"http://127.0.0.1:{self.port}/announce",
            "info_hash": "a" * 40,
            "peer_id": "test-peer-001",
            "port": 9999,
            "initial_backoff": 0.1,
            "max_backoff": 1.0,
            "request_timeout": 5.0,
        }
        defaults.update(overrides)
        return TrackerClientConfig(**defaults)

    def setUp(self):
        # Reset state before each test
        MockTrackerHandler.fail_count = 0
        MockTrackerHandler.request_log = []
        MockTrackerHandler._request_counter = 0

    async def test_start_sends_started_event(self):
        """Starting the client sends an announce with event=started."""
        config = self._make_config()
        client = TrackerClient(config)

        response = await client.start(left=1000)
        self.assertIsNotNone(response)
        self.assertEqual(response.interval, 5)
        self.assertEqual(len(response.peers), 2)

        # Check the request was logged
        self.assertTrue(len(MockTrackerHandler.request_log) >= 1)
        first_req = MockTrackerHandler.request_log[0]
        self.assertEqual(first_req["params"]["event"], "started")
        self.assertEqual(first_req["params"]["left"], "1000")

        await client.stop()

    async def test_stop_sends_stopped_event(self):
        """Stopping the client sends an announce with event=stopped."""
        config = self._make_config()
        client = TrackerClient(config)

        await client.start(left=500)
        initial_count = len(MockTrackerHandler.request_log)

        await client.stop()

        # Should have sent a "stopped" announce
        stopped_reqs = [
            r for r in MockTrackerHandler.request_log[initial_count:]
            if r["params"].get("event") == "stopped"
        ]
        self.assertEqual(len(stopped_reqs), 1)

    async def test_announce_completed(self):
        """announce_completed sends event=completed."""
        config = self._make_config()
        client = TrackerClient(config)

        await client.start(left=0)
        response = await client.announce_completed()
        self.assertIsNotNone(response)

        completed_reqs = [
            r for r in MockTrackerHandler.request_log
            if r["params"].get("event") == "completed"
        ]
        self.assertEqual(len(completed_reqs), 1)

        await client.stop()

    async def test_force_announce(self):
        """force_announce sends an immediate periodic announce."""
        config = self._make_config()
        client = TrackerClient(config)

        await client.start(left=100)
        response = await client.force_announce()
        self.assertIsNotNone(response)
        self.assertEqual(len(response.peers), 2)

        await client.stop()

    async def test_left_property(self):
        """Setting left updates the value sent in announces."""
        config = self._make_config()
        client = TrackerClient(config)

        await client.start(left=5000)
        client.left = 2500

        await client.force_announce()

        # Find the last announce request
        last_req = MockTrackerHandler.request_log[-1]
        self.assertEqual(last_req["params"]["left"], "2500")

        await client.stop()

    async def test_on_peers_received_callback(self):
        """Callback is invoked with peer list on successful announce."""
        config = self._make_config()
        client = TrackerClient(config)

        received_peers = []

        async def on_peers(peers):
            received_peers.extend(peers)

        client.on_peers_received = on_peers
        await client.start(left=100)

        self.assertTrue(len(received_peers) > 0)
        self.assertEqual(received_peers[0]["peer_id"], "seed-1")

        await client.stop()

    async def test_retry_with_backoff(self):
        """Client retries with backoff when tracker fails."""
        MockTrackerHandler.fail_count = 2  # Fail first 2 requests

        config = self._make_config()
        client = TrackerClient(config)

        response = await client.start(left=100)

        # Should still succeed after retries
        self.assertIsNotNone(response)
        # At least 3 requests: 2 failures + 1 success
        self.assertGreaterEqual(len(MockTrackerHandler.request_log), 3)

        await client.stop()

    async def test_survives_tracker_downtime(self):
        """Client survives when tracker is temporarily down."""
        config = self._make_config()
        client = TrackerClient(config)

        # Start successfully
        response = await client.start(left=100)
        self.assertIsNotNone(response)

        # Make tracker fail
        MockTrackerHandler.fail_count = 100  # Many failures

        # Force announce should handle failures gracefully
        response = await client.force_announce()
        self.assertIsNone(response)  # Failed but no crash

        # Restore tracker
        MockTrackerHandler.fail_count = 0

        # Should work again
        response = await client.force_announce()
        self.assertIsNotNone(response)

        await client.stop()

    async def test_last_response_property(self):
        """last_response returns the most recent successful response."""
        config = self._make_config()
        client = TrackerClient(config)

        self.assertIsNone(client.last_response)

        await client.start(left=100)
        self.assertIsNotNone(client.last_response)
        self.assertEqual(client.last_response.interval, 5)

        await client.stop()

    async def test_double_start_ignored(self):
        """Starting an already-running client is a no-op."""
        config = self._make_config()
        client = TrackerClient(config)

        await client.start(left=100)
        initial_count = len(MockTrackerHandler.request_log)

        # Start again — should be no-op
        await client.start(left=200)
        # Should not have sent another started announce
        started_after = [
            r for r in MockTrackerHandler.request_log[initial_count:]
            if r["params"].get("event") == "started"
        ]
        self.assertEqual(len(started_after), 0)

        await client.stop()

    async def test_double_stop_safe(self):
        """Stopping an already-stopped client is safe."""
        config = self._make_config()
        client = TrackerClient(config)

        await client.start(left=0)
        await client.stop()
        await client.stop()  # Should not raise


class TestTrackerClientNoServer(unittest.IsolatedAsyncioTestCase):
    """Test client behavior when the tracker is completely unreachable."""

    async def test_start_with_unreachable_tracker(self):
        """Client handles unreachable tracker gracefully."""
        config = TrackerClientConfig(
            tracker_url="http://127.0.0.1:1/announce",  # port 1 = unlikely to be open
            info_hash="b" * 40,
            peer_id="test-lonely-peer",
            port=9999,
            initial_backoff=0.05,
            max_backoff=0.1,
            request_timeout=0.5,
        )
        client = TrackerClient(config)

        # Should not crash, just return None
        response = await client.start(left=1000)
        self.assertIsNone(response)

        await client.stop()


if __name__ == "__main__":
    unittest.main()

