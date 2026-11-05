import random
from typing import Dict, Hashable, List, Optional, Set

from storage import Bitfield


class PiecePicker:
    """Decides which piece to request next from which peer.

    Pure logic with no I/O. The downloader (B4) feeds it what peers advertise
    (`peer_bitfield` / `peer_have` / `peer_gone`) and tells it what it did
    (`pick` reserves a piece for a peer; `piece_done` / `release` end that).

    Normal mode: among pieces we lack, the peer has, and nobody is fetching,
    choose the one held by the fewest known peers (rarest-first), breaking
    ties randomly so peers do not all chase the same piece.

    Endgame: once every missing piece is already in flight (or at most
    `endgame_threshold` pieces are missing), the same piece may also be
    requested from other peers, so one slow peer cannot stall the tail.
    `piece_done` returns the peers whose duplicate requests should be cancelled.
    """

    def __init__(
        self,
        own: Bitfield,
        endgame_threshold: int = 0,
        rng: Optional[random.Random] = None,
    ):
        self.own = own
        self.endgame_threshold = endgame_threshold
        self._rng = rng or random.Random()
        self._peers: Dict[Hashable, Bitfield] = {}
        self._availability = [0] * len(own)
        # piece index -> peers we have an outstanding request to for it
        self._in_flight: Dict[int, Set[Hashable]] = {}

    # ---- what peers advertise -------------------------------------------

    def peer_bitfield(self, peer: Hashable, bitfield: Bitfield) -> None:
        """Record (or replace) a peer's full bitfield."""
        if len(bitfield) != len(self.own):
            raise ValueError("bitfield size does not match torrent")
        self._remove_availability(peer)
        self._peers[peer] = bitfield.copy()
        for i in bitfield:
            self._availability[i] += 1

    def peer_have(self, peer: Hashable, index: int) -> None:
        """A peer announced a new piece (`have`)."""
        bf = self._peers.setdefault(peer, Bitfield(len(self.own)))
        if not bf.has(index):
            bf.set(index)
            self._availability[index] += 1

    def peer_gone(self, peer: Hashable) -> List[int]:
        """Forget a peer; returns pieces that were in flight to it (now free to re-pick)."""
        self._remove_availability(peer)
        self._peers.pop(peer, None)
        freed = []
        for index in list(self._in_flight):
            if peer in self._in_flight[index]:
                freed.append(index)
                self.release(peer, index)
        return sorted(freed)

    def _remove_availability(self, peer: Hashable) -> None:
        old = self._peers.get(peer)
        if old is not None:
            for i in old:
                self._availability[i] -= 1

    def availability(self, index: int) -> int:
        """Number of known peers that have this piece."""
        return self._availability[index]

    # ---- picking ----------------------------------------------------------

    def is_endgame(self) -> bool:
        missing = [i for i in self.own.missing()]
        if not missing:
            return False
        if len(missing) <= self.endgame_threshold:
            return True
        return all(i in self._in_flight for i in missing)

    def pick(self, peer: Hashable, count: int = 1) -> List[int]:
        """Reserve up to `count` pieces to request from `peer`, best first."""
        remote = self._peers.get(peer)
        if remote is None or count <= 0:
            return []
        endgame = self.is_endgame()
        candidates = []
        for i in self.own.missing():
            if not remote.has(i):
                continue
            holders = self._in_flight.get(i)
            if holders is None:
                candidates.append(i)
            elif endgame and peer not in holders:
                candidates.append(i)
        # rarest first; fewer existing requests next (spreads endgame); random tie-break
        self._rng.shuffle(candidates)
        candidates.sort(
            key=lambda i: (self._availability[i], len(self._in_flight.get(i, ())))
        )
        chosen = candidates[:count]
        for i in chosen:
            self._in_flight.setdefault(i, set()).add(peer)
        return chosen

    # ---- what we did --------------------------------------------------------

    def release(self, peer: Hashable, index: int) -> None:
        """A request failed or was cancelled; the piece may be picked again."""
        holders = self._in_flight.get(index)
        if holders is None:
            return
        holders.discard(peer)
        if not holders:
            del self._in_flight[index]

    def piece_done(self, index: int) -> List[Hashable]:
        """Piece verified and stored. Returns peers with duplicate requests to cancel."""
        self.own.set(index)
        return sorted(self._in_flight.pop(index, set()), key=repr)

    def in_flight(self, index: int) -> Set[Hashable]:
        return set(self._in_flight.get(index, ()))

    def interested_in(self, peer: Hashable) -> bool:
        """True if the peer has any piece we still lack."""
        remote = self._peers.get(peer)
        return remote is not None and any(
            remote.has(i) for i in self.own.missing()
        )
