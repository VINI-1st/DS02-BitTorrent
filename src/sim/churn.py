import argparse
from dataclasses import dataclass, asdict
import json
import math
import random
from typing import Literal


@dataclass
class ChurnEvent:
    timestamp: float
    action: Literal["join", "leave_graceful", "crash"]
    peer_id: str
    port: int
    role: Literal["seed", "leech"] = "leech"


class ChurnGenerator:
    """
    C2 Churn Engine: Sinh lịch trình peer vào/ra/sập một cách tất định (deterministic)
    dựa trên random_seed để có thể lặp lại chính xác thí nghiệm.
    """

    def __init__(self, seed: int = 42):
        self.seed = seed
        self.rng = random.Random(seed)

    def _sample_lifetime(
        self, dist: str, mean_lifetime: float, pareto_alpha: float = 2.0
    ) -> float:
        if dist == "pareto":
            # Pareto(alpha) trên [x_m, inf), chọn x_m sao cho kỳ vọng xấp xỉ mean_lifetime
            xm = mean_lifetime * (pareto_alpha - 1.0) / pareto_alpha if pareto_alpha > 1 else mean_lifetime * 0.5
            return xm * self.rng.paretovariate(pareto_alpha)
        # Mặc định: Phân phối mũ (Exponential)
        rate = 1.0 / max(mean_lifetime, 0.1)
        return self.rng.expovariate(rate)

    def generate_poisson_churn(
        self,
        duration_sec: float,
        arrival_rate_per_sec: float,
        mean_lifetime_sec: float,
        lifetime_dist: Literal["exponential", "pareto"] = "exponential",
        crash_prob: float = 0.4,
        base_port: int = 9100,
        max_dynamic_peers: int = 20,
    ) -> list[ChurnEvent]:
        events: list[ChurnEvent] = []
        current_time = 0.5
        peer_idx = 0

        while current_time < duration_sec and peer_idx < max_dynamic_peers:
            # Khoảng cách giữa các lần đến trong quá trình Poisson tuân theo phân phối mũ
            inter_arrival = self.rng.expovariate(arrival_rate_per_sec)
            current_time += inter_arrival
            if current_time >= duration_sec - 1.0:
                break

            port = base_port + peer_idx
            peer_id = f"leech-{port}"
            peer_idx += 1

            events.append(
                ChurnEvent(
                    timestamp=round(current_time, 3),
                    action="join",
                    peer_id=peer_id,
                    port=port,
                    role="leech",
                )
            )

            lifetime = self._sample_lifetime(lifetime_dist, mean_lifetime_sec)
            leave_time = current_time + lifetime
            if leave_time < duration_sec:
                action: Literal["leave_graceful", "crash"] = (
                    "crash" if self.rng.random() < crash_prob else "leave_graceful"
                )
                events.append(
                    ChurnEvent(
                        timestamp=round(leave_time, 3),
                        action=action,
                        peer_id=peer_id,
                        port=port,
                        role="leech",
                    )
                )

        events.sort(key=lambda e: (e.timestamp, e.action))
        return events

    def generate_flash_crowd(
        self,
        burst_time_sec: float,
        burst_size: int,
        mass_depart_time_sec: float | None = None,
        crash_prob: float = 0.3,
        base_port: int = 9200,
    ) -> list[ChurnEvent]:
        events: list[ChurnEvent] = []
        for i in range(burst_size):
            port = base_port + i
            peer_id = f"leech-{port}"
            # Các peer ập vào gần như đồng thời (chênh lệch vài chục mili-giây)
            join_ts = round(burst_time_sec + self.rng.uniform(0.0, 0.25), 3)
            events.append(
                ChurnEvent(
                    timestamp=join_ts,
                    action="join",
                    peer_id=peer_id,
                    port=port,
                    role="leech",
                )
            )
            if mass_depart_time_sec is not None and mass_depart_time_sec > join_ts:
                action: Literal["leave_graceful", "crash"] = (
                    "crash" if self.rng.random() < crash_prob else "leave_graceful"
                )
                leave_ts = round(mass_depart_time_sec + self.rng.uniform(0.0, 0.25), 3)
                events.append(
                    ChurnEvent(
                        timestamp=leave_ts,
                        action=action,
                        peer_id=peer_id,
                        port=port,
                        role="leech",
                    )
                )

        events.sort(key=lambda e: (e.timestamp, e.action))
        return events


def main():
    parser = argparse.ArgumentParser(description="Test C2 Churn Schedule Generator")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--mode", choices=["poisson", "flash"], default="poisson")
    args = parser.parse_args()

    gen = ChurnGenerator(seed=args.seed)
    if args.mode == "poisson":
        schedule = gen.generate_poisson_churn(
            duration_sec=20.0,
            arrival_rate_per_sec=0.5,
            mean_lifetime_sec=6.0,
            lifetime_dist="pareto",
            crash_prob=0.5,
        )
    else:
        schedule = gen.generate_flash_crowd(
            burst_time_sec=2.0, burst_size=5, mass_depart_time_sec=10.0
        )

    for ev in schedule:
        print(json.dumps(asdict(ev)))


if __name__ == "__main__":
    main()