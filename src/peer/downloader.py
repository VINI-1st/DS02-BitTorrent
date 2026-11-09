"""B4 – parallel downloader: rarest-first via PiecePicker, endgame, per-peer pipeline.

Pure picking logic lives in :class:`piece_picker.PiecePicker`; this module
adds the runtime bookkeeping the picker cannot do alone:

* which remote peers are usable right now (banned / cooling down / busy),
* per-peer in-flight caps (pipeline depth),
* ``bad_sources`` – peers that already served a corrupt copy of a piece,
* ``last_failure`` – remembers why a piece was released so the next pick from
  a different source can be logged as ``switch_source`` (Contract 5, B6).
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass


@dataclass
class RemotePeer:
    peer_id: str
    ip: str
    port: int
    bitfield: set[int] | None = None
    session: object | None = None  # live PeerSession, if connected
    session_active: bool = False  # a thread is connecting or running the session
    dead_until: float = 0.0
    ever_connected: bool = False
    lost_logged: bool = False
    invalid_count: int = 0
    banned: bool = False
    busy_until: float = 0.0


class Downloader:
    """Rarest-first work scheduler shared by the fetch worker threads.

    Not thread-safe: callers serialise every method with ``PeerNode.lock``.
    """

    def __init__(self, picker, max_parallel: int = 4, rng: random.Random | None = None):
        self.picker = picker
        self.max_parallel = max(1, max_parallel)
        self._rng = rng or random.Random()
        self.peers: dict[str, RemotePeer] = {}
        # piece index -> peers holding a picker reservation for it
        # (endgame may reserve the same piece on several peers at once)
        self.inflight: dict[int, set[str]] = {}
        self.inflight_per_peer: dict[str, int] = {}
        self.bad_sources: dict[int, set[str]] = {}
        self.last_failure: dict[int, tuple[str, str]] = {}
        self.sources_used: set[str] = set()

    # -- peer bookkeeping -------------------------------------------------
    def add_or_update(self, peer_id: str, ip: str, port: int) -> RemotePeer:
        rp = self.peers.get(peer_id)
        if rp is None:
            rp = RemotePeer(peer_id, ip, port)
            self.peers[peer_id] = rp
        else:
            rp.ip, rp.port = ip, port
        return rp

    def usable(self) -> list[RemotePeer]:
        now = time.time()
        return [
            rp for rp in self.peers.values()
            if not rp.banned and rp.dead_until <= now and rp.busy_until <= now and rp.bitfield
        ]

    def starved(self) -> bool:
        """True when no reachable peer can supply any piece we still need."""
        missing = list(self.picker.own.missing())
        now = time.time()
        for rp in self.peers.values():
            if rp.banned or rp.dead_until > now or not rp.bitfield:
                continue
            if any(i in rp.bitfield and rp.peer_id not in self.bad_sources.get(i, ()) for i in missing):
                return False
        return True

    # -- picking ------------------------------------------------------------
    def pick_work(self) -> tuple[int, RemotePeer] | None:
        usable = self.usable()
        if not usable:
            return None
        # Try peers in lightly-loaded, random order so parallel workers spread.
        order = sorted(usable, key=lambda rp: (self.inflight_per_peer.get(rp.peer_id, 0), self._rng.random()))
        for rp in order:
            if self.inflight_per_peer.get(rp.peer_id, 0) >= 2:
                continue
            # Reserve exactly one piece: reserving more would leak picker
            # in-flight slots for pieces we never fetch (B4 pipeline = 1
            # piece per worker at a time; blocks are pipelined inside fetch).
            index = self._pick_trusted(rp.peer_id)
            if index is None:
                continue
            self.inflight.setdefault(index, set()).add(rp.peer_id)
            self.inflight_per_peer[rp.peer_id] = self.inflight_per_peer.get(rp.peer_id, 0) + 1
            return index, rp
        # picker found nothing for this peer (all in flight / not held)
        return None

    def _pick_trusted(self, peer_id: str) -> int | None:
        """Best piece this peer has not already corrupted, so one bad piece
        (however rare) does not hide the peer's good ones."""
        rejected = []
        chosen = None
        while chosen is None:
            got = self.picker.pick(peer_id, 1)
            if not got:
                break
            if peer_id in self.bad_sources.get(got[0], ()):
                rejected.append(got[0])
            else:
                chosen = got[0]
        for index in rejected:
            self.picker.release(peer_id, index)
        return chosen

    def release(self, peer_id: str, index: int) -> None:
        """A request failed or was cancelled; the piece may be picked again."""
        self.picker.release(peer_id, index)
        holders = self.inflight.get(index)
        if holders is not None:
            holders.discard(peer_id)
            if not holders:
                del self.inflight[index]
        self.inflight_per_peer[peer_id] = max(0, self.inflight_per_peer.get(peer_id, 1) - 1)

    def piece_done(self, index: int) -> list:
        self.inflight.pop(index, None)
        cancelled = self.picker.piece_done(index)
        for pid in cancelled:
            self.inflight_per_peer[pid] = max(0, self.inflight_per_peer.get(pid, 1) - 1)
        return cancelled
