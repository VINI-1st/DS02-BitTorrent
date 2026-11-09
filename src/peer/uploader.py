"""B5 – upload path: token bucket throttle + choke slots + corrupt injection."""

from __future__ import annotations

import threading
import time

try:
    from .wire import BLOCK_SIZE, send_msg  # noqa: F401  (BLOCK_SIZE re-exported for peer.py)
except ImportError:  # pragma: no cover
    from peer.wire import BLOCK_SIZE, send_msg  # type: ignore[no-redef]


class FrameWriter:
    """Keeps whole frames intact when several threads write to one connection."""

    def __init__(self, sock):
        self._sock = sock
        self._lock = threading.Lock()

    def send(self, msg) -> None:
        with self._lock:
            send_msg(self._sock, msg)


class TokenBucket:
    """Caps total upload bandwidth of one peer (shared by all connections)."""

    def __init__(self, kib_per_sec: int):
        self.rate = float(kib_per_sec) * 1024.0
        self._lock = threading.Lock()
        self._next_free = time.monotonic()

    def consume(self, nbytes: int) -> None:
        if self.rate <= 0:
            return
        with self._lock:
            now = time.monotonic()
            start = max(now, self._next_free)
            self._next_free = start + nbytes / self.rate
            wait = self._next_free - now
        if wait > 0:
            time.sleep(wait)


def parse_corrupt_spec(spec: str | None, n: int) -> set[int]:
    if not spec:
        return set()
    if spec.strip().lower() == "all":
        return set(range(n))
    out: set[int] = set()
    for part in spec.split(","):
        part = part.strip()
        if part:
            try:
                idx = int(part)
            except ValueError:
                continue
            if 0 <= idx < n:
                out.add(idx)
    return out


def corrupt_block(block: bytes) -> bytes:
    n = min(len(block), 64)
    return bytes(b ^ 0xA5 for b in block[:n]) + block[n:]
