import io
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from unittest import mock

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
sys.path.insert(0, SRC)

from peer.downloader import Downloader
from peer.peer import EventLogger, PeerNode, parse_args
from peer.session import CHOKED, DONE, LOST, PeerSession, PieceTransfer
from peer.wire import WireError, do_handshake
from piece_picker import PiecePicker
from storage import Bitfield
from torrent import load_metainfo
from torrent.metainfo import create_metainfo, save_metainfo
from tracker.server import TrackerServer

HASH = b"h" * 20
PID = b"p" * 20


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def downloader(size, **peers):
    dl = Downloader(PiecePicker(Bitfield(size), rng=random.Random(1)))
    for pid, have in peers.items():
        rp = dl.add_or_update(pid, "127.0.0.1", 1)
        rp.bitfield = set(have)
        dl.picker.peer_bitfield(pid, Bitfield(size, have))
    return dl


class DownloaderTest(unittest.TestCase):
    def test_not_starved_with_a_useful_peer(self):
        self.assertFalse(downloader(2, a=[0]).starved())

    def test_starved_without_peers(self):
        self.assertTrue(downloader(2).starved())

    def test_starved_when_peer_only_has_pieces_we_own(self):
        dl = downloader(2, a=[0])
        dl.picker.own.set(0)
        self.assertTrue(dl.starved())

    def test_starved_when_only_holder_served_corrupt_copy(self):
        dl = downloader(1, a=[0])
        dl.bad_sources[0] = {"a"}
        self.assertTrue(dl.starved())

    def test_starved_when_only_holder_is_cooling_down(self):
        dl = downloader(1, a=[0])
        dl.peers["a"].dead_until = time.time() + 60
        self.assertTrue(dl.starved())

    def test_corrupt_rarest_piece_does_not_hide_other_pieces(self):
        # piece 0 is rarest (one holder), piece 1 has two holders
        dl = downloader(2, a=[0, 1], b=[1])
        dl.peers["b"].dead_until = time.time() + 60
        dl.bad_sources[0] = {"a"}
        index, rp = dl.pick_work()
        self.assertEqual((index, rp.peer_id), (1, "a"))
        self.assertEqual(dl.picker.in_flight(0), set())


class SessionTest(unittest.TestCase):
    def setUp(self):
        self.a, self.b = socket.socketpair()
        self.session = PeerSession(self.a)
        self.addCleanup(self.b.close)
        self.addCleanup(self.session.close)

    def queue(self, index, size=4):
        t = PieceTransfer(index, size)
        self.session.request(t)
        return t

    def test_blocks_assemble_the_piece(self):
        t = self.queue(0)
        self.session.on_piece(0, 2, b"cd")
        self.assertFalse(t.finished.is_set())
        self.session.on_piece(0, 0, b"ab")
        self.assertEqual((t.outcome, bytes(t.buf)), (DONE, b"abcd"))

    def test_out_of_range_block_cannot_grow_the_buffer(self):
        t = self.queue(0)
        self.session.on_piece(0, 3, b"xxxx")
        self.assertEqual((len(t.buf), t.received), (4, 0))

    def test_choke_refuses_only_the_oldest_transfer(self):
        first, second = self.queue(0), self.queue(1)
        self.session.on_choke()
        self.assertEqual(first.outcome, CHOKED)
        self.assertFalse(second.finished.is_set())

    def test_close_fails_queued_transfers_and_new_requests(self):
        t = self.queue(0)
        self.session.close()
        self.assertEqual(t.outcome, LOST)
        with self.assertRaises(ConnectionError):
            self.queue(1)

    def test_stalled_only_with_pending_work(self):
        self.assertFalse(self.session.stalled(-1))
        self.queue(0)
        self.assertTrue(self.session.stalled(-1))


class HandshakeTest(unittest.TestCase):
    def test_malformed_handshake_is_a_wire_error(self):
        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        b.sendall(b"\x00" * 68)
        with self.assertRaises(WireError):
            do_handshake(a, HASH, PID, client=False)


class PeerProcessTest(unittest.TestCase):
    """Real `python -m peer` processes on loopback, with the real tracker."""

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.procs = []
        self.addCleanup(self.kill_all)
        self.data = os.urandom(512 * 1024)
        self.src = os.path.join(self.dir, "f.bin")
        with open(self.src, "wb") as f:
            f.write(self.data)
        self.tracker_port = free_port()
        meta = create_metainfo(self.src, f"http://127.0.0.1:{self.tracker_port}/announce", 64 * 1024)
        self.info_hash = meta.info_hash
        self.torrent = os.path.join(self.dir, "t.json")
        save_metainfo(meta, self.torrent)

    def kill_all(self):
        for p in self.procs:
            if p.poll() is None:
                p.kill()
                p.wait()

    def start_tracker(self):
        tracker = TrackerServer(host="127.0.0.1", port=self.tracker_port, interval=30, ttl=90)
        tracker.start()
        self.addCleanup(tracker.stop)
        return tracker

    def peer(self, name, role, *extra, full=False):
        d = os.path.join(self.dir, name)
        os.makedirs(d, exist_ok=True)
        if full:
            shutil.copy(self.src, d)
        env = dict(os.environ, PYTHONPATH=SRC)
        p = subprocess.Popen(
            [sys.executable, "-m", "peer", "--torrent", self.torrent, "--port", str(free_port()), "--dir", d,
             "--role", role, "--peer-id", name, "--log", os.path.join(self.dir, name + ".jsonl"), *extra],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.procs.append(p)
        return p

    def events(self, name):
        with open(os.path.join(self.dir, name + ".jsonl"), encoding="utf-8") as f:
            return [json.loads(line) for line in f]

    def names(self, name):
        return [e["event"] for e in self.events(name)]

    def scrape(self):
        url = f"http://127.0.0.1:{self.tracker_port}/scrape?info_hash={self.info_hash}"
        with urllib.request.urlopen(url, timeout=2) as r:
            return json.load(r)["files"][self.info_hash]

    def test_leech_that_already_has_the_file_reports_completed(self):
        p = self.peer("L", "leech", "--exit-on-complete", full=True)
        self.assertEqual(p.wait(20), 0)
        names = self.names("L")
        self.assertLess(names.index("started"), names.index("completed"))
        self.assertLess(names.index("completed"), names.index("stopped"))

    def test_readback_failure_stops_once_with_exit_3(self):
        d = os.path.join(self.dir, "L")
        os.makedirs(d)
        shutil.copy(self.src, d)
        log = os.path.join(self.dir, "L.jsonl")
        args = parse_args(["--torrent", self.torrent, "--port", str(free_port()), "--dir", d,
                           "--role", "leech", "--peer-id", "L", "--log", log])
        logger = EventLogger("L", log)
        self.addCleanup(logger.close)
        node = PeerNode(args, load_metainfo(self.torrent), logger)
        with mock.patch.object(node.storage, "read_piece", side_effect=OSError("disk error")), \
                mock.patch("sys.stderr", new_callable=io.StringIO):
            self.assertEqual(node.run(), 3)
        names = self.names("L")
        self.assertNotIn("completed", names)
        self.assertEqual(names.count("stopped"), 1)
        self.assertEqual(self.events("L")[-1]["meta"]["reason"], "readback_failed")

    def test_gives_up_with_exit_3_when_no_source_exists(self):
        self.assertEqual(self.peer("L", "leech", "--give-up-sec", "1").wait(20), 3)
        self.assertEqual(self.events("L")[-1]["meta"]["reason"], "gave_up")

    def test_starved_leech_finds_late_seed_before_the_interval(self):
        self.start_tracker()
        leech = self.peer("L", "leech", "--exit-on-complete")
        time.sleep(1.0)
        self.peer("S", "seed", full=True)
        self.assertEqual(leech.wait(15), 0)
        with open(os.path.join(self.dir, "L", "f.bin"), "rb") as f:
            self.assertEqual(f.read(), self.data)

    def test_exit_on_complete_leaves_the_swarm_and_counts_once(self):
        self.start_tracker()
        self.peer("S", "seed", full=True)
        self.assertEqual(self.peer("L", "leech", "--exit-on-complete").wait(20), 0)
        stats = self.scrape()
        self.assertEqual((stats["complete"], stats["downloaded"]), (1, 1))

    def test_leech_downloads_a_piece_another_leech_got_after_connecting(self):
        # A and B start empty, so A's pieces can only reach B through `have`.
        self.start_tracker()
        self.peer("S", "seed", "--max-up-kbps", "64", full=True)  # slow, so B's idle workers turn to A
        first = self.peer("A", "leech")
        second = self.peer("B", "leech", "--exit-on-complete")
        self.assertEqual(second.wait(40), 0)
        self.assertIsNone(first.poll())
        sources = {e["meta"]["from_peer"] for e in self.events("B") if e["event"] == "piece_done"}
        self.assertIn("A", sources)


if __name__ == "__main__":
    unittest.main()
