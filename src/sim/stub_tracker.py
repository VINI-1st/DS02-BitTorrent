"""StubTracker – lightweight stand-in for Role A's tracker (Contract 2).

Run:
    py src/sim/stub_tracker.py --host 127.0.0.1 --port 8000 [--interval 2] [--ttl 6]

HTTP API (JSON responses, UTF-8):
    GET /announce?info_hash=<hex40>&peer_id=<id>&ip=<ip>&port=<int>&event=<started|completed|stopped|>&left=<int>
        -> {"interval": <sec>, "peers": [{"peer_id": ..., "ip": ..., "port": ...}, ...]}
           (the caller itself is never included; peers are shuffled)
    GET /scrape[?info_hash=<hex40>]
        -> {"files": {"<info_hash>": {"complete": n, "incomplete": n, "downloaded": n}}}
    GET /health -> {"ok": true}

Behaviour that matches what Role A must implement (A1/A2/A3):
    * swarm registry keyed by info_hash,
    * peers that stop announcing disappear after ``--ttl`` seconds,
    * ``stopped`` removes the peer immediately, ``completed`` turns it into a seed,
    * state is in memory only: after a crash + restart the swarm is rebuilt
      as peers re-announce.

Event log (Contract 5) on stdout: ``started`` and ``stopped`` with peer_id "tracker".
Optional ``--log-requests`` additionally logs one ``announce_ok`` per request
(peer_id "tracker", meta.from_peer) – off by default so tracker load is counted
from the peer side only.
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.sim.common import EventLogger, configure_utf8_stdio  # noqa: E402

configure_utf8_stdio()


@dataclass
class PeerEntry:
    peer_id: str
    ip: str
    port: int
    left: int
    last_seen: float
    completed: bool = False


class SwarmRegistry:
    def __init__(self, ttl: float):
        self.ttl = ttl
        self._lock = threading.Lock()
        self._swarms: dict[str, dict[str, PeerEntry]] = {}
        self._downloaded: dict[str, int] = {}
        self.requests = 0

    def _expire(self, now: float) -> None:
        for swarm in self._swarms.values():
            for pid in [p for p, e in swarm.items() if now - e.last_seen > self.ttl]:
                del swarm[pid]

    def announce(self, info_hash: str, peer_id: str, ip: str, port: int, event: str, left: int, numwant: int) -> list[dict]:
        now = time.time()
        with self._lock:
            self.requests += 1
            self._expire(now)
            swarm = self._swarms.setdefault(info_hash, {})
            if event == "stopped":
                swarm.pop(peer_id, None)
                return []
            entry = swarm.get(peer_id)
            if entry is None:
                entry = PeerEntry(peer_id, ip, port, left, now)
                swarm[peer_id] = entry
            entry.ip, entry.port, entry.left, entry.last_seen = ip, port, left, now
            if event == "completed" and not entry.completed:
                entry.completed = True
                self._downloaded[info_hash] = self._downloaded.get(info_hash, 0) + 1
            if left == 0:
                entry.completed = True
            others = [e for p, e in swarm.items() if p != peer_id]
        random.shuffle(others)
        return [{"peer_id": e.peer_id, "ip": e.ip, "port": e.port} for e in others[:numwant]]

    def scrape(self, info_hash: str | None) -> dict:
        now = time.time()
        with self._lock:
            self.requests += 1
            self._expire(now)
            keys = [info_hash] if info_hash else list(self._swarms)
            out = {}
            for ih in keys:
                swarm = self._swarms.get(ih, {})
                complete = sum(1 for e in swarm.values() if e.completed)
                out[ih] = {
                    "complete": complete,
                    "incomplete": len(swarm) - complete,
                    "downloaded": self._downloaded.get(ih, 0),
                }
            return {"files": out}


def make_handler(registry: SwarmRegistry, interval: int, logger: EventLogger, log_requests: bool):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.0"

        def log_message(self, fmt, *args):  # silence default stderr access log
            return

        def _send(self, code: int, payload: dict) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802 (http.server API)
            url = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(url.query).items()}
            try:
                if url.path == "/announce":
                    info_hash = q["info_hash"].lower()
                    peer_id = q["peer_id"]
                    port = int(q["port"])
                    ip = q.get("ip") or self.client_address[0]
                    event = q.get("event", "")
                    left = int(q.get("left", "0"))
                    numwant = int(q.get("numwant", "50"))
                    if event not in ("", "started", "completed", "stopped"):
                        raise ValueError(f"bad event {event!r}")
                    peers = registry.announce(info_hash, peer_id, ip, port, event, left, numwant)
                    if log_requests:
                        logger.emit("announce_ok", None, from_peer=peer_id, announce_event=event, left=left, peers_returned=len(peers))
                    self._send(200, {"interval": interval, "peers": peers})
                elif url.path == "/scrape":
                    self._send(200, registry.scrape(q.get("info_hash")))
                elif url.path == "/health":
                    self._send(200, {"ok": True, "requests": registry.requests})
                else:
                    self._send(404, {"failure_reason": "not found"})
            except (KeyError, ValueError) as exc:
                self._send(400, {"failure_reason": f"invalid request: {exc}"})

    return Handler


def main() -> int:
    parser = argparse.ArgumentParser(description="DS02 StubTracker (Contract 2 stand-in)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--interval", type=int, default=2, help="announce interval returned to peers (s)")
    parser.add_argument("--ttl", type=float, default=None, help="peer expiry (s), default 3*interval")
    parser.add_argument("--peer-id", default="tracker", help="peer_id used in the event log")
    parser.add_argument("--log", default=None, help="write JSONL here instead of stdout")
    parser.add_argument("--stop-file", default=None, help="exit gracefully when this file appears")
    parser.add_argument("--log-requests", action="store_true", help="log one announce_ok per request")
    args = parser.parse_args()

    logger = EventLogger(args.peer_id, args.log)
    registry = SwarmRegistry(ttl=args.ttl if args.ttl else 3.0 * args.interval)
    try:
        server = ThreadingHTTPServer((args.host, args.port), make_handler(registry, args.interval, logger, args.log_requests))
    except OSError as exc:
        logger.emit("stopped", None, reason="bind_failed", error=str(exc), port=args.port)
        return 1
    server.daemon_threads = True

    stop = threading.Event()

    def _on_signal(signum, _frame):
        stop.set()

    for sig_name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                signal.signal(sig, _on_signal)
            except (OSError, ValueError):
                pass

    t = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.2}, daemon=True)
    t.start()
    logger.emit("started", None, role="tracker", host=args.host, port=args.port, interval=args.interval, ttl=registry.ttl)

    stop_file = Path(args.stop_file) if args.stop_file else None
    while not stop.is_set():
        if stop_file is not None and stop_file.exists():
            break
        stop.wait(0.2)

    server.shutdown()
    server.server_close()
    logger.emit("stopped", None, reason="graceful_shutdown", total_requests=registry.requests)
    return 0


if __name__ == "__main__":
    sys.exit(main())
