"""Swarm manager for the BitTorrent tracker.

Manages peer registrations organized by ``info_hash``.  Each swarm tracks
which peers are present, whether they are seeders or leechers, and exposes
announce / scrape semantics that mirror the BitTorrent HTTP tracker protocol.

Thread-safety is guaranteed by a single :class:`threading.Lock` that
serialises all mutations.
"""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field


@dataclass
class PeerInfo:
    """State kept for a single peer inside a swarm."""

    peer_id: str
    ip: str
    port: int
    left: int  # bytes remaining to download; 0 → seeder
    last_announce: float = field(default_factory=time.time)
    event: str = "started"


class SwarmManager:
    """Thread-safe swarm registry keyed by ``info_hash``.

    Parameters
    ----------
    default_interval:
        Seconds the tracker tells peers to wait between announces.
    ttl:
        Seconds after which a peer that has not re-announced is considered
        dead and eligible for removal by :meth:`cleanup_expired`.
    """

    _MAX_PEERS_RETURNED = 50

    def __init__(self, default_interval: int = 30, ttl: int = 90) -> None:
        self._interval: int = default_interval
        self._ttl: int = ttl
        # {info_hash: {peer_id: PeerInfo}}
        self._swarms: dict[str, dict[str, PeerInfo]] = {}
        # {info_hash: int} – total number of "completed" events ever seen
        self._downloaded: dict[str, int] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # announce
    # ------------------------------------------------------------------
    def announce(
        self,
        info_hash: str,
        peer_id: str,
        ip: str,
        port: int,
        event: str = "",
        left: int = 0,
    ) -> dict:
        """Process an announce request and return a tracker response dict.

        Parameters
        ----------
        info_hash:
            SHA-1 hash identifying the torrent.
        peer_id:
            Unique identifier for this peer.
        ip:
            IP address of the peer.
        port:
            Port the peer is listening on.
        event:
            One of ``"started"``, ``"completed"``, ``"stopped"``, or ``""``
            (empty string / periodic re-announce).
        left:
            Number of bytes the peer still needs.  ``0`` means the peer is a
            seeder.

        Returns
        -------
        dict
            ``{"interval", "complete", "incomplete", "peers"}`` where
            *peers* is a list of ``{"peer_id", "ip", "port"}`` dicts
            containing up to 50 random peers (excluding the requester).
        """
        with self._lock:
            swarm = self._swarms.setdefault(info_hash, {})
            self._downloaded.setdefault(info_hash, 0)

            if event == "stopped":
                swarm.pop(peer_id, None)
            else:
                existing = swarm.get(peer_id)
                if existing is not None:
                    # Update existing registration.
                    existing.ip = ip
                    existing.port = port
                    existing.left = left
                    existing.last_announce = time.time()
                    existing.event = event if event else existing.event
                else:
                    swarm[peer_id] = PeerInfo(
                        peer_id=peer_id,
                        ip=ip,
                        port=port,
                        left=left,
                        last_announce=time.time(),
                        event=event if event else "started",
                    )

                if event == "completed":
                    self._downloaded[info_hash] += 1
                    # Ensure the peer is marked as seeder.
                    swarm[peer_id].left = 0

            # Build response.
            complete, incomplete = self._swarm_stats(swarm)

            # Peers list – exclude the requester.
            candidates = [
                p for pid, p in swarm.items() if pid != peer_id
            ]
            if len(candidates) > self._MAX_PEERS_RETURNED:
                candidates = random.sample(candidates, self._MAX_PEERS_RETURNED)

            peers = [
                {"peer_id": p.peer_id, "ip": p.ip, "port": p.port}
                for p in candidates
            ]

            return {
                "interval": self._interval,
                "complete": complete,
                "incomplete": incomplete,
                "peers": peers,
            }

    # ------------------------------------------------------------------
    # scrape
    # ------------------------------------------------------------------
    def scrape(self, info_hash: str | None = None) -> dict:
        """Return scrape statistics.

        Parameters
        ----------
        info_hash:
            If given, return stats for just that swarm.  Otherwise return
            stats for every tracked swarm.

        Returns
        -------
        dict
            ``{"files": {info_hash: {"complete", "incomplete", "downloaded"}}}``
        """
        with self._lock:
            if info_hash is not None:
                swarm = self._swarms.get(info_hash, {})
                complete, incomplete = self._swarm_stats(swarm)
                return {
                    "files": {
                        info_hash: {
                            "complete": complete,
                            "incomplete": incomplete,
                            "downloaded": self._downloaded.get(info_hash, 0),
                        }
                    }
                }

            files: dict[str, dict] = {}
            for ih, swarm in self._swarms.items():
                complete, incomplete = self._swarm_stats(swarm)
                files[ih] = {
                    "complete": complete,
                    "incomplete": incomplete,
                    "downloaded": self._downloaded.get(ih, 0),
                }
            return {"files": files}

    # ------------------------------------------------------------------
    # cleanup
    # ------------------------------------------------------------------
    def cleanup_expired(self) -> list[tuple[str, str]]:
        """Remove peers whose ``last_announce`` is older than the TTL.

        Returns
        -------
        list[tuple[str, str]]
            ``(info_hash, peer_id)`` pairs for every removed peer.
        """
        now = time.time()
        removed: list[tuple[str, str]] = []
        with self._lock:
            for info_hash, swarm in list(self._swarms.items()):
                expired_ids = [
                    pid
                    for pid, peer in swarm.items()
                    if now - peer.last_announce > self._ttl
                ]
                for pid in expired_ids:
                    del swarm[pid]
                    removed.append((info_hash, pid))
                # Optionally prune empty swarms.
                if not swarm:
                    del self._swarms[info_hash]
        return removed

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _swarm_stats(swarm: dict[str, PeerInfo]) -> tuple[int, int]:
        """Return ``(seeders, leechers)`` counts for a swarm dict."""
        complete = sum(1 for p in swarm.values() if p.left == 0)
        incomplete = len(swarm) - complete
        return complete, incomplete
