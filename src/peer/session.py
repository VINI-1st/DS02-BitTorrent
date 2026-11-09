"""B4/B6 - one persistent outgoing connection to a remote peer.

A reader thread (``PeerNode.session_loop``) owns the receive side and feeds
``on_piece`` / ``on_choke``; worker threads queue piece downloads with
``request`` and wait on the transfer's ``finished`` event. The uploader answers
one connection's requests in order, so a choke always refers to the oldest
transfer still queued here.
"""

from __future__ import annotations

import socket
import threading
import time
from collections import deque
from dataclasses import dataclass, field

try:
    from .wire import BLOCK_SIZE, send_msg
except ImportError:  # pragma: no cover
    from peer.wire import BLOCK_SIZE, send_msg  # type: ignore[no-redef]

try:
    from protocol.messages import KeepAlive, Request
except ImportError:  # pragma: no cover
    from src.protocol.messages import KeepAlive, Request  # type: ignore[no-redef]

DONE = "done"
CHOKED = "choked"
LOST = "lost"


@dataclass(eq=False)
class PieceTransfer:
    index: int
    size: int
    buf: bytearray = field(init=False)
    received: int = 0
    outcome: str | None = None
    finished: threading.Event = field(default_factory=threading.Event)

    def __post_init__(self) -> None:
        self.buf = bytearray(self.size)

    def finish(self, outcome: str) -> None:
        self.outcome = outcome
        self.finished.set()


class PeerSession:
    def __init__(self, sock: socket.socket):
        self.sock = sock
        self._send_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._pending: deque[PieceTransfer] = deque()
        self._closed = False
        self._last_sent = time.monotonic()
        self._last_progress = time.monotonic()

    def send(self, msg) -> None:
        with self._send_lock:
            send_msg(self.sock, msg)
            self._last_sent = time.monotonic()

    def request(self, transfer: PieceTransfer) -> None:
        """Queue a piece and pipeline all of its block requests."""
        # Held across the whole batch: the queue order must match the wire order.
        with self._send_lock:
            with self._state_lock:
                if self._closed:
                    raise ConnectionError("session closed")
                if not self._pending:
                    self._last_progress = time.monotonic()
                self._pending.append(transfer)
            for begin in range(0, transfer.size, BLOCK_SIZE):
                length = min(BLOCK_SIZE, transfer.size - begin)
                send_msg(self.sock, Request(index=transfer.index, begin=begin, length=length))
            self._last_sent = time.monotonic()

    def on_piece(self, index: int, begin: int, block: bytes) -> None:
        with self._state_lock:
            transfer = next((t for t in self._pending if t.index == index), None)
            if transfer is None or begin + len(block) > transfer.size:
                return
            transfer.buf[begin:begin + len(block)] = block
            transfer.received += len(block)
            self._last_progress = time.monotonic()
            if transfer.received >= transfer.size:
                self._pending.remove(transfer)
                transfer.finish(DONE)

    def on_choke(self) -> None:
        with self._state_lock:
            self._last_progress = time.monotonic()
            transfer = self._pending.popleft() if self._pending else None
        if transfer is not None:
            transfer.finish(CHOKED)

    def oldest_index(self) -> int | None:
        with self._state_lock:
            return self._pending[0].index if self._pending else None

    def stalled(self, timeout: float) -> bool:
        with self._state_lock:
            return bool(self._pending) and time.monotonic() - self._last_progress > timeout

    def keepalive_if_idle(self, interval: float) -> None:
        if time.monotonic() - self._last_sent < interval:
            return
        try:
            self.send(KeepAlive())
        except OSError:
            pass  # the reader thread reports the broken connection

    def close(self) -> None:
        """Fail queued transfers and wake the reader. Safe to call repeatedly."""
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
            pending = list(self._pending)
            self._pending.clear()
        for transfer in pending:
            transfer.finish(LOST)
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass
