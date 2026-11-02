from dataclasses import dataclass
import subprocess
import sys
import time
from typing import Callable


@dataclass
class PeerNetProfile:
    max_up_kbps: int = 512
    latency_ms: int = 0
    drop_rate: float = 0.0


@dataclass
class TrackerFaultConfig:
    enabled: bool = False
    crash_at_sec: float = 5.0
    downtime_sec: float = 4.0


class NetworkEmulator:
    """
    C3 Network Emulator: Quản lý cấu hình băng thông/độ trễ cho từng peer
    và điều phối việc tắt/bật lại (crash & restart) tiến trình Tracker.
    """

    def __init__(self, net_cfg: dict | None = None):
        net_cfg = net_cfg or {}
        self.default_profile = PeerNetProfile(
            max_up_kbps=int(net_cfg.get("default_max_up_kbps", 512)),
            latency_ms=int(net_cfg.get("latency_ms", 0)),
            drop_rate=float(net_cfg.get("drop_rate", 0.0)),
        )
        self.peer_overrides: dict[str, PeerNetProfile] = {}
        for peer_id, p_cfg in net_cfg.get("peer_profiles", {}).items():
            self.peer_overrides[peer_id] = PeerNetProfile(
                max_up_kbps=int(p_cfg.get("max_up_kbps", self.default_profile.max_up_kbps)),
                latency_ms=int(p_cfg.get("latency_ms", self.default_profile.latency_ms)),
                drop_rate=float(p_cfg.get("drop_rate", self.default_profile.drop_rate)),
            )

        tf_cfg = net_cfg.get("tracker_fault", {})
        self.tracker_fault = TrackerFaultConfig(
            enabled=bool(tf_cfg.get("enabled", False)),
            crash_at_sec=float(tf_cfg.get("crash_at_sec", 5.0)),
            downtime_sec=float(tf_cfg.get("downtime_sec", 4.0)),
        )

    def get_peer_profile(self, peer_id: str) -> PeerNetProfile:
        return self.peer_overrides.get(peer_id, self.default_profile)

    def run_tracker_fault_injection(
        self,
        start_tracker_fn: Callable[[], subprocess.Popen],
        stop_tracker_fn: Callable[[subprocess.Popen], None],
        current_proc_Getter: Callable[[], subprocess.Popen | None],
        current_proc_setter: Callable[[subprocess.Popen | None], None],
        log_fn: Callable[[str, dict], None],
    ):
        """Chạy trong luồng nền để tắt Tracker tại crash_at_sec và bật lại sau downtime_sec."""
        if not self.tracker_fault.enabled:
            return

        time.sleep(max(0.0, self.tracker_fault.crash_at_sec))
        proc = current_proc_Getter()
        if proc is not None and proc.poll() is None:
            stop_tracker_fn(proc)
            current_proc_setter(None)
            log_fn(
                "tracker_crashed",
                {"downtime_sec": self.tracker_fault.downtime_sec},
            )

        time.sleep(max(0.0, self.tracker_fault.downtime_sec))
        new_proc = start_tracker_fn()
        current_proc_setter(new_proc)
        log_fn("tracker_restarted", {"pid": new_proc.pid if new_proc else None})