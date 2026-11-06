"""
Tracker client library for peers.

Provides periodic announce with exponential backoff on failure.
This is what peers use to register with the tracker and get peer lists.

Task A6: Tracker client lib for peer.
"""

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Callable, Awaitable
from urllib.parse import urlencode
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

logger = logging.getLogger(__name__)


@dataclass
class AnnounceResponse:
    """Parsed response from a tracker announce request."""
    interval: int
    complete: int
    incomplete: int
    peers: list[dict]  # [{"peer_id": str, "ip": str, "port": int}, ...]

    @classmethod
    def from_dict(cls, data: dict) -> "AnnounceResponse":
        return cls(
            interval=data.get("interval", 30),
            complete=data.get("complete", 0),
            incomplete=data.get("incomplete", 0),
            peers=data.get("peers", []),
        )


@dataclass
class TrackerClientConfig:
    """Configuration for the tracker client."""
    tracker_url: str              # e.g. "http://127.0.0.1:8000/announce"
    info_hash: str                # hex-encoded, 40 chars
    peer_id: str                  # unique peer identifier
    port: int                     # port this peer listens on
    ip: str = ""                  # optional; tracker defaults to requester IP
    initial_backoff: float = 2.0  # seconds for first retry
    max_backoff: float = 60.0     # max seconds between retries
    request_timeout: float = 10.0 # HTTP request timeout in seconds


class TrackerClient:
    """
    Async tracker client that handles periodic announces with exponential backoff.

    Usage:
        client = TrackerClient(config)
        client.on_peers_received = my_callback  # async def my_callback(peers: list[dict])
        await client.start()
        ...
        await client.stop()
    """

    def __init__(self, config: TrackerClientConfig):
        self._config = config
        self._running = False
        self._task: asyncio.Task | None = None
        self._left: int = 0  # bytes remaining; set by peer core
        self._current_backoff: float = config.initial_backoff
        self._consecutive_failures: int = 0
        self._last_response: AnnounceResponse | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

        # Callback for when new peers are received
        self.on_peers_received: Callable[[list[dict]], Awaitable[None]] | None = None

    @property
    def last_response(self) -> AnnounceResponse | None:
        """Last successful announce response."""
        return self._last_response

    @property
    def left(self) -> int:
        """Bytes remaining to download."""
        return self._left

    @left.setter
    def left(self, value: int) -> None:
        """Update bytes remaining. Called by peer core as download progresses."""
        self._left = max(0, value)

    async def start(self, left: int = 0) -> AnnounceResponse | None:
        """
        Start the tracker client. Sends initial 'started' announce and
        begins periodic re-announce loop.

        Args:
            left: Initial bytes remaining to download. 0 = seeder.

        Returns:
            The initial announce response, or None if it failed.
        """
        if self._running:
            logger.warning("TrackerClient already running")
            return self._last_response

        self._left = left
        self._running = True
        self._loop = asyncio.get_running_loop()

        # Send initial "started" announce
        response = await self._do_announce(event="started")

        # Start the periodic announce loop
        self._task = asyncio.create_task(self._announce_loop())

        return response

    async def stop(self) -> None:
        """
        Stop the tracker client. Sends 'stopped' announce to deregister.
        """
        if not self._running:
            return

        self._running = False

        # Cancel the periodic loop
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        # Send "stopped" announce (best effort)
        try:
            await self._do_announce(event="stopped")
        except Exception as e:
            logger.debug("Failed to send 'stopped' announce: %s", e)

    async def announce_completed(self) -> AnnounceResponse | None:
        """
        Send a 'completed' event announce. Called when download finishes.
        """
        return await self._do_announce(event="completed")

    async def force_announce(self) -> AnnounceResponse | None:
        """
        Force an immediate re-announce (e.g., when peer needs more sources).
        """
        return await self._do_announce(event="")

    async def _announce_loop(self) -> None:
        """Background loop that periodically re-announces to the tracker."""
        while self._running:
            # Determine wait interval
            if self._last_response:
                interval = self._last_response.interval
            else:
                interval = 30  # default

            # Add small jitter to avoid thundering herd
            jitter = random.uniform(0, interval * 0.1)

            try:
                await asyncio.sleep(interval + jitter)
            except asyncio.CancelledError:
                return

            if not self._running:
                return

            await self._do_announce(event="")

    async def _do_announce(self, event: str = "") -> AnnounceResponse | None:
        """
        Perform a single announce request with retry on failure.

        Uses exponential backoff on consecutive failures.
        Resets backoff on success.
        """
        params = {
            "info_hash": self._config.info_hash,
            "peer_id": self._config.peer_id,
            "port": str(self._config.port),
            "left": str(self._left),
        }
        if event:
            params["event"] = event
        if self._config.ip:
            params["ip"] = self._config.ip

        url = f"{self._config.tracker_url}?{urlencode(params)}"

        # Retry loop with exponential backoff
        max_retries = 5 if event != "stopped" else 1
        for attempt in range(max_retries):
            if not self._running and event != "stopped":
                return None

            try:
                response = await self._http_get(url)
                data = json.loads(response)

                announce_resp = AnnounceResponse.from_dict(data)
                self._last_response = announce_resp
                self._consecutive_failures = 0
                self._current_backoff = self._config.initial_backoff

                logger.info(
                    "Announce success (event=%s): %d peers, interval=%ds",
                    event or "periodic",
                    len(announce_resp.peers),
                    announce_resp.interval,
                )

                # Notify callback
                if self.on_peers_received and announce_resp.peers:
                    try:
                        await self.on_peers_received(announce_resp.peers)
                    except Exception as e:
                        logger.error("on_peers_received callback error: %s", e)

                return announce_resp

            except Exception as e:
                self._consecutive_failures += 1
                logger.warning(
                    "Announce failed (attempt %d/%d, event=%s): %s",
                    attempt + 1,
                    max_retries,
                    event or "periodic",
                    e,
                )

                if attempt < max_retries - 1:
                    backoff = min(
                        self._current_backoff * (2 ** attempt),
                        self._config.max_backoff,
                    )
                    # Add jitter to backoff
                    backoff += random.uniform(0, backoff * 0.25)
                    logger.info("Retrying in %.1fs...", backoff)

                    try:
                        await asyncio.sleep(backoff)
                    except asyncio.CancelledError:
                        return None

        logger.error(
            "Announce gave up after %d attempts (event=%s)",
            max_retries,
            event or "periodic",
        )
        return None

    async def _http_get(self, url: str) -> str:
        """
        Perform an HTTP GET request asynchronously.

        Runs the blocking urllib call in a thread executor to avoid
        blocking the event loop.
        """
        loop = asyncio.get_running_loop()

        def _blocking_get() -> str:
            req = Request(url)
            req.add_header("User-Agent", "DS02-BitTorrent/1.0")
            with urlopen(req, timeout=self._config.request_timeout) as resp:
                return resp.read().decode("utf-8")

        return await loop.run_in_executor(None, _blocking_get)

