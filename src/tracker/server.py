"""HTTP tracker server for the BitTorrent-like P2P system.

Uses :class:`http.server.ThreadingHTTPServer` (stdlib) so that each
incoming request is handled in its own thread.  The tracker exposes two
endpoints:

* ``GET /announce`` – peer registration / heartbeat
* ``GET /scrape``   – swarm statistics

All responses are JSON-encoded.  A background thread periodically runs
:meth:`SwarmManager.cleanup_expired` to evict peers that have stopped
announcing.
"""

from __future__ import annotations

import json
import logging
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .swarm import SwarmManager

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Request handler
# ------------------------------------------------------------------

class TrackerHandler(BaseHTTPRequestHandler):
    """Handle ``/announce`` and ``/scrape`` HTTP requests.

    The handler accesses the :class:`SwarmManager` instance stored on the
    *server* object (``self.server.swarm_manager``).
    """

    # Silence per-request log lines from BaseHTTPRequestHandler.
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        logger.info(
            json.dumps({
                "ts": time.time(),
                "event": "http_request",
                "client": self.client_address[0],
                "request": args[0] if args else "",
            })
        )

    # ------------------------------------------------------------------
    # routing
    # ------------------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802
        """Route GET requests to the appropriate handler."""
        parsed = urlparse(self.path)
        if parsed.path == "/announce":
            self._handle_announce(parse_qs(parsed.query))
        elif parsed.path == "/scrape":
            self._handle_scrape(parse_qs(parsed.query))
        else:
            self.send_error(404, "Not Found")

    # ------------------------------------------------------------------
    # /announce
    # ------------------------------------------------------------------
    def _handle_announce(self, params: dict[str, list[str]]) -> None:
        """Process an announce request.

        Required query parameters: ``info_hash``, ``peer_id``, ``port``.
        Optional: ``ip`` (defaults to the requesting client's IP),
        ``event``, ``left``.
        """
        # --- validate required params ---
        missing = [
            p for p in ("info_hash", "peer_id", "port") if p not in params
        ]
        if missing:
            self._send_json(
                {"error": f"Missing required parameters: {', '.join(missing)}"},
                status=400,
            )
            return

        info_hash: str = params["info_hash"][0]
        peer_id: str = params["peer_id"][0]

        try:
            port = int(params["port"][0])
        except (ValueError, IndexError):
            self._send_json({"error": "Invalid port"}, status=400)
            return

        ip: str = params.get("ip", [self.client_address[0]])[0]
        event: str = params.get("event", [""])[0]

        try:
            left = int(params.get("left", ["0"])[0])
        except ValueError:
            left = 0

        swarm_manager: SwarmManager = self.server.swarm_manager  # type: ignore[attr-defined]
        response = swarm_manager.announce(
            info_hash=info_hash,
            peer_id=peer_id,
            ip=ip,
            port=port,
            event=event,
            left=left,
        )
        self._send_json(response)

    # ------------------------------------------------------------------
    # /scrape
    # ------------------------------------------------------------------
    def _handle_scrape(self, params: dict[str, list[str]]) -> None:
        """Process a scrape request.

        Optional query parameter: ``info_hash``.  If omitted, stats for
        all tracked swarms are returned.
        """
        info_hash: str | None = params.get("info_hash", [None])[0]  # type: ignore[assignment]
        swarm_manager: SwarmManager = self.server.swarm_manager  # type: ignore[attr-defined]
        response = swarm_manager.scrape(info_hash=info_hash)
        self._send_json(response)

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _send_json(self, data: Any, *, status: int = 200) -> None:
        """Serialise *data* as JSON and send it with the given HTTP status."""
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


# ------------------------------------------------------------------
# Server wrapper
# ------------------------------------------------------------------

class TrackerServer:
    """Lifecycle wrapper around :class:`ThreadingHTTPServer`.

    Parameters
    ----------
    host:
        Interface to bind to.
    port:
        TCP port to listen on.
    interval:
        Announce interval (seconds) communicated to peers and also used
        as the period between TTL cleanup sweeps.
    ttl:
        Time-to-live (seconds) for peer registrations.  Peers that do not
        re-announce within this window are pruned.
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8000,
        interval: int = 30,
        ttl: int = 90,
    ) -> None:
        self.host = host
        self.port = port
        self.interval = interval
        self.swarm_manager = SwarmManager(default_interval=interval, ttl=ttl)

        self._httpd: ThreadingHTTPServer | None = None
        self._server_thread: threading.Thread | None = None
        self._cleanup_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------
    def start(self) -> None:
        """Start the HTTP server and the background cleanup thread."""
        self._stop_event.clear()

        self._httpd = ThreadingHTTPServer(
            (self.host, self.port), TrackerHandler
        )
        # Attach the swarm manager so the handler can access it.
        self._httpd.swarm_manager = self.swarm_manager  # type: ignore[attr-defined]

        self._server_thread = threading.Thread(
            target=self._httpd.serve_forever,
            name="tracker-http",
            daemon=True,
        )
        self._server_thread.start()

        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            name="tracker-cleanup",
            daemon=True,
        )
        self._cleanup_thread.start()

        logger.info(
            json.dumps({
                "ts": time.time(),
                "event": "tracker_started",
                "host": self.host,
                "port": self.port,
                "interval": self.interval,
            })
        )

    def stop(self) -> None:
        """Gracefully shut down the server and background threads."""
        self._stop_event.set()
        if self._httpd is not None:
            self._httpd.shutdown()
        if self._server_thread is not None:
            self._server_thread.join(timeout=5)
        if self._cleanup_thread is not None:
            self._cleanup_thread.join(timeout=5)
        logger.info(
            json.dumps({"ts": time.time(), "event": "tracker_stopped"})
        )

    # ------------------------------------------------------------------
    # background cleanup
    # ------------------------------------------------------------------
    def _cleanup_loop(self) -> None:
        """Periodically evict expired peers until :meth:`stop` is called."""
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=self.interval)
            if self._stop_event.is_set():
                break
            removed = self.swarm_manager.cleanup_expired()
            if removed:
                logger.info(
                    json.dumps({
                        "ts": time.time(),
                        "event": "peers_expired",
                        "removed": [
                            {"info_hash": ih, "peer_id": pid}
                            for ih, pid in removed
                        ],
                    })
                )
