"""C3 – Network emulation and fault injection.

* **Bandwidth** – every peer gets ``--max-up-kbps`` (Contract 4, KiB/s). This
  works for the StubPeer *and* Role B's real peer because it is a contract flag.
* **Latency** – ``--latency-ms`` is a StubPeer-only flag (added to every
  handshake and piece request). For the real peer, latency is not injected
  (Windows has no ``tc netem``); the launcher simply does not pass the flag.
* **Tracker faults** – :class:`TrackerFaultInjector` kills the *real* tracker
  OS process at ``at_sec`` and respawns it after ``downtime_sec`` through a
  :class:`TrackerControl` object provided by the launcher.
* **Peer faults** – :class:`PeerKill` entries kill (crash) or stop (graceful)
  a named peer at a given time, e.g. "stop one source peer during download".

Scenario keys (all optional)::

    netem:
      default_max_up_kbps: 256        # falls back to swarm.max_up_kbps
      latency_ms: 20
      role_profiles: {seed: {max_up_kbps: 128}, leech: {max_up_kbps: 64}}
      peer_profiles: {seed-9002: {max_up_kbps: 32, latency_ms: 150}}
      tracker_fault:                  # single crash (legacy form) ...
        enabled: true
        crash_at_sec: 3.0
        downtime_sec: 4.0
      # ... or several crashes:
      #   crashes: [{at_sec: 3, downtime_sec: 2}, {at_sec: 12, downtime_sec: 2}]
    faults:
      kill_peers: [{target: seed-9002, at_sec: 3.0, mode: crash}]   # or mode: graceful
      corrupt_peers: {seed-9003: all}                                # StubPeer only
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol


@dataclass(frozen=True)
class PeerNetProfile:
    max_up_kbps: int = 0
    latency_ms: int = 0


@dataclass(frozen=True)
class TrackerCrash:
    at_sec: float
    downtime_sec: float


@dataclass
class TrackerFaultConfig:
    enabled: bool = False
    crashes: list[TrackerCrash] = field(default_factory=list)

    @property
    def crash_at_sec(self) -> float:
        return self.crashes[0].at_sec if self.crashes else 0.0

    @property
    def downtime_sec(self) -> float:
        return self.crashes[0].downtime_sec if self.crashes else 0.0


@dataclass(frozen=True)
class PeerKill:
    target: str
    at_sec: float
    mode: str = "crash"  # crash | graceful


class NetworkEmulator:
    def __init__(self, net_cfg: dict[str, Any] | None = None, faults_cfg: dict[str, Any] | None = None):
        net_cfg = net_cfg or {}
        faults_cfg = faults_cfg or {}
        self.default_profile = PeerNetProfile(
            max_up_kbps=int(net_cfg.get("default_max_up_kbps", 0) or 0),
            latency_ms=int(net_cfg.get("latency_ms", 0) or 0),
        )
        self.role_profiles: dict[str, PeerNetProfile] = {
            role: self._merge(self.default_profile, p) for role, p in (net_cfg.get("role_profiles") or {}).items()
        }
        self.peer_overrides: dict[str, dict] = dict(net_cfg.get("peer_profiles") or {})

        tf = net_cfg.get("tracker_fault") or {}
        crashes: list[TrackerCrash] = []
        if tf.get("crashes"):
            crashes = [TrackerCrash(float(c["at_sec"]), float(c.get("downtime_sec", 3.0))) for c in tf["crashes"]]
        elif tf.get("enabled", False):
            crashes = [TrackerCrash(float(tf.get("crash_at_sec", 5.0)), float(tf.get("downtime_sec", 4.0)))]
        self.tracker_fault = TrackerFaultConfig(enabled=bool(tf.get("enabled", bool(crashes))) and bool(crashes), crashes=sorted(crashes, key=lambda c: c.at_sec))

        self.peer_kills: list[PeerKill] = sorted(
            (PeerKill(str(k["target"]), float(k["at_sec"]), str(k.get("mode", "crash"))) for k in faults_cfg.get("kill_peers") or []),
            key=lambda k: k.at_sec,
        )
        for k in self.peer_kills:
            if k.mode not in ("crash", "graceful"):
                raise ValueError(f"kill_peers mode must be crash|graceful, got {k.mode!r}")
        self.corrupt_peers: dict[str, str] = {str(k): str(v) for k, v in (faults_cfg.get("corrupt_peers") or {}).items()}

    @staticmethod
    def _merge(base: PeerNetProfile, override: dict | None) -> PeerNetProfile:
        override = override or {}
        return PeerNetProfile(
            max_up_kbps=int(override.get("max_up_kbps", base.max_up_kbps) or 0),
            latency_ms=int(override.get("latency_ms", base.latency_ms) or 0),
        )

    def get_peer_profile(self, peer_id: str, role: str | None = None) -> PeerNetProfile:
        base = self.role_profiles.get(role or ("seed" if peer_id.startswith("seed") else "leech"), self.default_profile)
        if peer_id in self.peer_overrides:
            return self._merge(base, self.peer_overrides[peer_id])
        return base

    def peer_args(self, peer_id: str, role: str, sim_flags: bool) -> list[str]:
        """Extra CLI args for one peer: contract flag always, sim flags only for the stub."""
        prof = self.get_peer_profile(peer_id, role)
        args = ["--max-up-kbps", str(prof.max_up_kbps)]
        if sim_flags:
            if prof.latency_ms > 0:
                args += ["--latency-ms", str(prof.latency_ms)]
            if peer_id in self.corrupt_peers:
                args += ["--corrupt-pieces", self.corrupt_peers[peer_id]]
        return args

    @staticmethod
    def expected_transfer_sec(nbytes: int, max_up_kbps: int) -> float:
        """Lower bound for serving ``nbytes`` from one peer capped at ``max_up_kbps`` KiB/s."""
        if max_up_kbps <= 0:
            return 0.0
        return nbytes / (max_up_kbps * 1024.0)


class TrackerControl(Protocol):
    """Implemented by the launcher's tracker supervisor."""

    def crash(self) -> bool: ...
    def restart(self) -> bool: ...


class TrackerFaultInjector:
    """Background thread: kill the real tracker process and bring it back."""

    def __init__(
        self,
        cfg: TrackerFaultConfig,
        control: TrackerControl,
        log_fn: Callable[[str, dict], None],
        stop_event: threading.Event,
    ):
        self.cfg = cfg
        self.control = control
        self.log_fn = log_fn
        self.stop_event = stop_event
        self.timeline: list[tuple[str, float]] = []

    def run(self, start_time: float) -> None:
        if not self.cfg.enabled:
            return
        for crash in self.cfg.crashes:
            if self._sleep_until(start_time + crash.at_sec):
                return
            if self.control.crash():
                self.timeline.append(("crashed", time.time()))
                self.log_fn("tracker_crashed", {"downtime_sec": crash.downtime_sec, "planned_at_sec": crash.at_sec})
            if self._sleep_until(start_time + crash.at_sec + crash.downtime_sec):
                return
            ok = self.control.restart()
            self.timeline.append(("restarted", time.time()))
            self.log_fn("tracker_restarted", {"ok": ok, "planned_at_sec": round(crash.at_sec + crash.downtime_sec, 3)})

    def _sleep_until(self, deadline: float) -> bool:
        """Return True if the scenario was stopped while waiting."""
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                return self.stop_event.is_set()
            if self.stop_event.wait(min(remaining, 0.2)):
                return True
