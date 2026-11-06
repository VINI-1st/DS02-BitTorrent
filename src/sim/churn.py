"""C2 – Churn engine: deterministic schedules of peer joins, graceful leaves and crashes.

All randomness comes from ``random.Random(seed)`` instances owned by the
generator, so the same ``random_seed`` always produces the same schedule
(verified by ``tests/test_sim_modules.py``). Independent sub-streams are used
for arrivals, lifetimes and leave-type so that changing e.g. ``crash_prob``
does not shift the arrival times.

Supported models
----------------
* ``poisson``      – arrivals form a Poisson process of rate λ (exponential
                     inter-arrival times); each peer lives for a random time
                     drawn from an Exponential or Pareto distribution.
* ``flash_crowd``  – ``burst_size`` peers join inside a short window, optionally
                     followed by a ``mass_departure``.
* ``mass_departure`` can also be added to the Poisson model.
* ``late_joins``   – fixed joins at given times (used e.g. while the tracker is down).

Leave types
-----------
* ``leave_graceful`` – SIGTERM / CTRL_BREAK / stop file: the peer announces
  ``stopped`` and logs ``stopped``.
* ``crash``          – SIGKILL / TerminateProcess: no goodbye, other peers must
  detect it (``peer_lost``) and the tracker must expire it by TTL.

CLI (prints the schedule as JSON Lines):
    py src/sim/churn.py --seed 42 --mode poisson
    py src/sim/churn.py --scenario scenarios/high_churn.yaml
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

Action = Literal["join", "leave_graceful", "crash"]
ACTION_ORDER = {"join": 0, "leave_graceful": 1, "crash": 2}


@dataclass(frozen=True)
class ChurnEvent:
    timestamp: float  # seconds since scenario start
    action: Action
    peer_id: str
    port: int
    role: Literal["seed", "leech"] = "leech"
    cause: str = "churn"  # poisson | flash_crowd | mass_departure | late_join | lifetime

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def schedule_fingerprint(events: list[ChurnEvent]) -> str:
    """SHA-256 over the canonical schedule – equal fingerprints == identical runs."""
    blob = json.dumps([e.to_dict() for e in events], sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class ChurnGenerator:
    def __init__(self, seed: int = 42):
        self.seed = int(seed)
        self.rng_arrival = random.Random(f"{self.seed}:arrival")
        self.rng_lifetime = random.Random(f"{self.seed}:lifetime")
        self.rng_leave = random.Random(f"{self.seed}:leave")
        self.rng_jitter = random.Random(f"{self.seed}:jitter")

    # ---------------------------------------------------------------- samplers
    def sample_lifetime(self, dist: str, mean: float, pareto_alpha: float = 2.0) -> float:
        """Exponential(mean) or Pareto(alpha) with the same mean (alpha > 1)."""
        mean = max(float(mean), 0.1)
        if dist == "pareto":
            alpha = float(pareto_alpha)
            if alpha <= 1.0:
                raise ValueError("pareto_alpha must be > 1 for a finite mean")
            xm = mean * (alpha - 1.0) / alpha  # E[X] = alpha*xm/(alpha-1) = mean
            return xm * self.rng_lifetime.paretovariate(alpha)
        if dist == "exponential":
            return self.rng_lifetime.expovariate(1.0 / mean)
        raise ValueError(f"unknown lifetime distribution {dist!r}")

    def _leave_action(self, crash_prob: float) -> Action:
        return "crash" if self.rng_leave.random() < crash_prob else "leave_graceful"

    # ---------------------------------------------------------------- models
    def generate_poisson_churn(
        self,
        duration_sec: float,
        arrival_rate_per_sec: float,
        mean_lifetime_sec: float,
        lifetime_dist: str = "exponential",
        crash_prob: float = 0.4,
        base_port: int = 9100,
        max_dynamic_peers: int = 20,
        pareto_alpha: float = 2.0,
        start_after_sec: float = 0.5,
        stop_arrivals_before_end_sec: float = 1.0,
    ) -> list[ChurnEvent]:
        if arrival_rate_per_sec <= 0:
            return []
        events: list[ChurnEvent] = []
        t = float(start_after_sec)
        for idx in range(int(max_dynamic_peers)):
            t += self.rng_arrival.expovariate(arrival_rate_per_sec)
            if t >= duration_sec - stop_arrivals_before_end_sec:
                break
            port = base_port + idx
            pid = f"leech-{port}"
            events.append(ChurnEvent(round(t, 3), "join", pid, port, "leech", "poisson"))
            life = self.sample_lifetime(lifetime_dist, mean_lifetime_sec, pareto_alpha)
            leave_at = t + life
            action = self._leave_action(crash_prob)
            if leave_at < duration_sec:
                events.append(ChurnEvent(round(leave_at, 3), action, pid, port, "leech", "lifetime"))
        return sort_events(events)

    def generate_flash_crowd(
        self,
        burst_time_sec: float,
        burst_size: int,
        mass_depart_time_sec: float | None = None,
        crash_prob: float = 0.3,
        base_port: int = 9200,
        burst_window_sec: float = 0.5,
        depart_fraction: float = 1.0,
    ) -> list[ChurnEvent]:
        events: list[ChurnEvent] = []
        joined: list[tuple[str, int, float]] = []
        for i in range(int(burst_size)):
            port = base_port + i
            pid = f"leech-{port}"
            ts = round(burst_time_sec + self.rng_arrival.uniform(0.0, burst_window_sec), 3)
            events.append(ChurnEvent(ts, "join", pid, port, "leech", "flash_crowd"))
            joined.append((pid, port, ts))
        if mass_depart_time_sec is not None:
            events += self.generate_mass_departure(joined, mass_depart_time_sec, crash_prob, depart_fraction)
        return sort_events(events)

    def generate_mass_departure(
        self,
        targets: list[tuple[str, int, float]],
        at_sec: float,
        crash_prob: float = 0.3,
        fraction: float = 1.0,
        window_sec: float = 0.3,
    ) -> list[ChurnEvent]:
        """Make ``fraction`` of ``targets`` (peer_id, port, join_ts) leave around ``at_sec``."""
        chosen = [t for t in targets if t[2] < at_sec]
        k = round(len(chosen) * max(0.0, min(1.0, fraction)))
        chosen = sorted(self.rng_leave.sample(chosen, k), key=lambda t: t[1]) if k < len(chosen) else chosen
        out = []
        for pid, port, _ in chosen:
            ts = round(at_sec + self.rng_jitter.uniform(0.0, window_sec), 3)
            out.append(ChurnEvent(ts, self._leave_action(crash_prob), pid, port, "leech", "mass_departure"))
        return out

    @staticmethod
    def generate_late_joins(spec: list[dict], base_port: int) -> list[ChurnEvent]:
        """``spec``: ``[{at_sec: 3.0, count: 2}, ...]`` – deterministic, no randomness."""
        events = []
        idx = 0
        for item in spec or []:
            for _ in range(int(item.get("count", 1))):
                port = base_port + idx
                idx += 1
                events.append(ChurnEvent(round(float(item["at_sec"]), 3), "join", f"leech-{port}", port, "leech", "late_join"))
        return events


def sort_events(events: list[ChurnEvent]) -> list[ChurnEvent]:
    return sorted(events, key=lambda e: (e.timestamp, ACTION_ORDER[e.action], e.port))


def build_schedule(cfg: dict[str, Any]) -> list[ChurnEvent]:
    """Build the full churn schedule for a scenario dict (see docs/log-and-scenario-spec.md)."""
    sim = cfg.get("simulation", {}) or {}
    swarm = cfg.get("swarm", {}) or {}
    churn = cfg.get("churn", {}) or {}
    seed = int(sim.get("random_seed", 42))
    duration = float(sim.get("duration_sec", 20))
    base_port = int(swarm.get("base_port", 9001))
    gen = ChurnGenerator(seed)
    events: list[ChurnEvent] = []

    late = swarm.get("late_leechers") or []
    if late:
        events += gen.generate_late_joins(late, base_port + 300)

    if churn.get("enabled", False):
        mode = churn.get("mode", "poisson")
        dyn_base = base_port + int(churn.get("port_offset", 100))
        if mode == "flash_crowd":
            events += gen.generate_flash_crowd(
                burst_time_sec=float(churn.get("burst_time_sec", 2.0)),
                burst_size=int(churn.get("burst_size", 5)),
                mass_depart_time_sec=churn.get("mass_depart_time_sec"),
                crash_prob=float(churn.get("crash_prob", 0.3)),
                base_port=dyn_base,
                burst_window_sec=float(churn.get("burst_window_sec", 0.5)),
                depart_fraction=float(churn.get("depart_fraction", 1.0)),
            )
        elif mode == "poisson":
            pois = gen.generate_poisson_churn(
                duration_sec=duration,
                arrival_rate_per_sec=float(churn.get("arrival_rate_per_sec", 0.4)),
                mean_lifetime_sec=float(churn.get("mean_lifetime_sec", 4.0)),
                lifetime_dist=str(churn.get("lifetime_dist", "exponential")),
                crash_prob=float(churn.get("crash_prob", 0.4)),
                base_port=dyn_base,
                max_dynamic_peers=int(churn.get("max_dynamic_peers", 10)),
                pareto_alpha=float(churn.get("pareto_alpha", 2.0)),
            )
            events += pois
            md = churn.get("mass_departure")
            if md:
                joined = [(e.peer_id, e.port, e.timestamp) for e in pois if e.action == "join"]
                leaving = {e.peer_id for e in pois if e.action != "join" and e.timestamp < float(md["at_sec"])}
                joined = [j for j in joined if j[0] not in leaving]
                events += gen.generate_mass_departure(
                    joined, float(md["at_sec"]), float(md.get("crash_prob", 0.5)), float(md.get("fraction", 1.0))
                )
        else:
            raise ValueError(f"unknown churn mode {mode!r}")
    return dedupe_leaves(sort_events(events))


def dedupe_leaves(events: list[ChurnEvent]) -> list[ChurnEvent]:
    """Keep only the first leave/crash of each peer."""
    left: set[str] = set()
    out = []
    for e in events:
        if e.action != "join":
            if e.peer_id in left:
                continue
            left.add(e.peer_id)
        out.append(e)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="C2 churn schedule generator")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--mode", choices=["poisson", "flash"], default="poisson")
    parser.add_argument("--scenario", default=None, help="build the schedule of a scenario YAML instead")
    args = parser.parse_args()

    if args.scenario:
        import yaml

        with open(args.scenario, "r", encoding="utf-8") as f:
            schedule = build_schedule(yaml.safe_load(f))
    elif args.mode == "poisson":
        schedule = ChurnGenerator(args.seed).generate_poisson_churn(
            duration_sec=20.0, arrival_rate_per_sec=0.5, mean_lifetime_sec=6.0, lifetime_dist="pareto", crash_prob=0.5
        )
    else:
        schedule = ChurnGenerator(args.seed).generate_flash_crowd(burst_time_sec=2.0, burst_size=5, mass_depart_time_sec=10.0)
    for ev in schedule:
        print(json.dumps(ev.to_dict()))
    print(json.dumps({"events": len(schedule), "fingerprint": schedule_fingerprint(schedule)}), file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    sys.exit(main())
