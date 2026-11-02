import argparse
import json
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
import yaml

# Cho phép import các module cùng thư mục khi chạy trực tiếp bằng lệnh py src/sim/launcher.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.sim.churn import ChurnGenerator, ChurnEvent
from src.sim.netem import NetworkEmulator


def stream_reader(proc: subprocess.Popen, log_file, lock: threading.Lock):
    assert proc.stdout is not None
    for line in proc.stdout:
        line_str = line.strip()
        if not line_str:
            continue
        with lock:
            log_file.write(line_str + "\n")
            log_file.flush()
            print(line_str)


def write_sim_event(log_file, lock: threading.Lock, peer_id: str, event: str, meta: dict):
    record = {
        "ts": round(time.time(), 3),
        "peer_id": peer_id,
        "event": event,
        "chunk_index": None,
        "meta": meta,
    }
    line_str = json.dumps(record)
    with lock:
        log_file.write(line_str + "\n")
        log_file.flush()
        print(line_str)


def create_dummy_torrent(work_dir: Path, torrent_cfg: dict, tracker_cfg: dict) -> Path:
    torrent_path = work_dir / "sample.torrent.json"
    metainfo = {
        "file_name": torrent_cfg.get("file_name", "sample_data.bin"),
        "file_size_bytes": torrent_cfg.get("file_size_bytes", 1048576),
        "piece_length": torrent_cfg.get("piece_length", 262144),
        "tracker_url": f"http://{tracker_cfg.get('host', '127.0.0.1')}:{tracker_cfg.get('port', 8000)}",
    }
    torrent_path.write_text(json.dumps(metainfo, indent=2), encoding="utf-8")
    return torrent_path


def spawn_peer(
    peer_cmd: str,
    torrent_path: Path,
    port: int,
    peer_dir: Path,
    role: str,
    max_up_kbps: int,
    latency_ms: int,
    log_file,
    log_lock: threading.Lock,
    reader_threads: list[threading.Thread],
) -> subprocess.Popen:
    cmd = [
        sys.executable,
        peer_cmd,
        "--torrent", str(torrent_path),
        "--port", str(port),
        "--dir", str(peer_dir),
        "--role", role,
        "--max-up-kbps", str(max_up_kbps),
    ]
    if "stub_peer.py" in peer_cmd and latency_ms > 0:
        cmd.extend(["--latency-ms", str(latency_ms)])

    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    t = threading.Thread(
        target=stream_reader, args=(proc, log_file, log_lock), daemon=True
    )
    t.start()
    reader_threads.append(t)
    return proc


def run_churn_worker(
    events: list[ChurnEvent],
    start_time: float,
    active_procs: dict[str, subprocess.Popen],
    proc_lock: threading.Lock,
    peer_cmd: str,
    torrent_path: Path,
    root_work_dir: Path,
    netem: NetworkEmulator,
    log_file,
    log_lock: threading.Lock,
    reader_threads: list[threading.Thread],
):
    for ev in events:
        wait_sec = (start_time + ev.timestamp) - time.time()
        if wait_sec > 0:
            time.sleep(wait_sec)

        with proc_lock:
            if ev.action == "join":
                prof = netem.get_peer_profile(ev.peer_id)
                peer_dir = root_work_dir / f"{ev.role}_{ev.port}"
                proc = spawn_peer(
                    peer_cmd,
                    torrent_path,
                    ev.port,
                    peer_dir,
                    ev.role,
                    prof.max_up_kbps,
                    prof.latency_ms,
                    log_file,
                    log_lock,
                    reader_threads,
                )
                active_procs[ev.peer_id] = proc
                write_sim_event(
                    log_file,
                    log_lock,
                    "simulator",
                    "churn_join",
                    {"target": ev.peer_id, "port": ev.port},
                )
            elif ev.action == "leave_graceful":
                proc = active_procs.get(ev.peer_id)
                if proc and proc.poll() is None:
                    proc.terminate()
                    write_sim_event(
                        log_file,
                        log_lock,
                        "simulator",
                        "churn_leave_graceful",
                        {"target": ev.peer_id},
                    )
            elif ev.action == "crash":
                proc = active_procs.get(ev.peer_id)
                if proc and proc.poll() is None:
                    proc.kill()
                    write_sim_event(
                        log_file,
                        log_lock,
                        "simulator",
                        "peer_lost",
                        {"target": ev.peer_id, "reason": "churn_crash"},
                    )


def run_scenario(scenario_path: str, peer_cmd: str, keep_dirs: bool = False):
    with open(scenario_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    scen_name = cfg.get("name", "default")
    swarm_cfg = cfg.get("swarm", {})
    sim_cfg = cfg.get("simulation", {})
    churn_cfg = cfg.get("churn", {})
    netem_cfg = cfg.get("netem", {})

    num_seeds = swarm_cfg.get("num_seeds", 1)
    num_leechers = swarm_cfg.get("num_leechers", 3)
    base_port = swarm_cfg.get("base_port", 9001)
    max_up_kbps = swarm_cfg.get("max_up_kbps", 512)
    duration_sec = sim_cfg.get("duration_sec", 20)
    random_seed = sim_cfg.get("random_seed", 42)

    netem_cfg.setdefault("default_max_up_kbps", max_up_kbps)
    netem = NetworkEmulator(netem_cfg)

    root_work_dir = Path("sim_workspace") / scen_name
    if root_work_dir.exists():
        shutil.rmtree(root_work_dir)
    root_work_dir.mkdir(parents=True, exist_ok=True)

    log_dir = Path("logs") / scen_name
    log_dir.mkdir(parents=True, exist_ok=True)
    unified_log_path = log_dir / "events.jsonl"

    torrent_path = create_dummy_torrent(
        root_work_dir, cfg.get("torrent", {}), cfg.get("tracker", {})
    )

    active_procs: dict[str, subprocess.Popen] = {}
    proc_lock = threading.Lock()
    reader_threads: list[threading.Thread] = []
    log_lock = threading.Lock()

    # Chuẩn bị lịch trình Churn (C2) nếu kịch bản có bật churn
    churn_events: list[ChurnEvent] = []
    if churn_cfg.get("enabled", False):
        churn_gen = ChurnGenerator(seed=random_seed)
        mode = churn_cfg.get("mode", "poisson")
        if mode == "flash_crowd":
            churn_events = churn_gen.generate_flash_crowd(
                burst_time_sec=float(churn_cfg.get("burst_time_sec", 2.0)),
                burst_size=int(churn_cfg.get("burst_size", 5)),
                mass_depart_time_sec=churn_cfg.get("mass_depart_time_sec"),
                crash_prob=float(churn_cfg.get("crash_prob", 0.3)),
                base_port=base_port + 100,
            )
        else:
            churn_events = churn_gen.generate_poisson_churn(
                duration_sec=duration_sec,
                arrival_rate_per_sec=float(churn_cfg.get("arrival_rate_per_sec", 0.4)),
                mean_lifetime_sec=float(churn_cfg.get("mean_lifetime_sec", 4.0)),
                lifetime_dist=churn_cfg.get("lifetime_dist", "exponential"),
                crash_prob=float(churn_cfg.get("crash_prob", 0.4)),
                base_port=base_port + 100,
                max_dynamic_peers=int(churn_cfg.get("max_dynamic_peers", 10)),
            )

    print(f"=== [Orchestrator] Starting scenario '{scen_name}' ===")
    with open(unified_log_path, "w", encoding="utf-8") as log_file:
        try:
            start_time = time.time()

            # 1. Khởi chạy các máy Seeder ban đầu
            for i in range(num_seeds):
                port = base_port + i
                peer_id = f"seed-{port}"
                prof = netem.get_peer_profile(peer_id)
                peer_dir = root_work_dir / f"seed_{port}"
                proc = spawn_peer(
                    peer_cmd,
                    torrent_path,
                    port,
                    peer_dir,
                    "seed",
                    prof.max_up_kbps,
                    prof.latency_ms,
                    log_file,
                    log_lock,
                    reader_threads,
                )
                with proc_lock:
                    active_procs[peer_id] = proc

            time.sleep(0.3)

            # 2. Khởi chạy các máy Leecher ban đầu
            for j in range(num_leechers):
                port = base_port + num_seeds + j
                peer_id = f"leech-{port}"
                prof = netem.get_peer_profile(peer_id)
                peer_dir = root_work_dir / f"leech_{port}"
                proc = spawn_peer(
                    peer_cmd,
                    torrent_path,
                    port,
                    peer_dir,
                    "leech",
                    prof.max_up_kbps,
                    prof.latency_ms,
                    log_file,
                    log_lock,
                    reader_threads,
                )
                with proc_lock:
                    active_procs[peer_id] = proc

            # 3. Kích hoạt luồng Churn (C2) nếu có
            if churn_events:
                churn_thread = threading.Thread(
                    target=run_churn_worker,
                    args=(
                        churn_events,
                        start_time,
                        active_procs,
                        proc_lock,
                        peer_cmd,
                        torrent_path,
                        root_work_dir,
                        netem,
                        log_file,
                        log_lock,
                        reader_threads,
                    ),
                    daemon=True,
                )
                churn_thread.start()

            # 4. Kích hoạt luồng giả lập sập Tracker (C3) nếu có
            if netem.tracker_fault.enabled:
                def _dummy_log(ev_name: str, meta_dict: dict):
                    write_sim_event(log_file, log_lock, "tracker", ev_name, meta_dict)

                tf_thread = threading.Thread(
                    target=netem.run_tracker_fault_injection,
                    args=(
                        lambda: None,
                        lambda p: None,
                        lambda: None,
                        lambda p: None,
                        _dummy_log,
                    ),
                    daemon=True,
                )
                tf_thread.start()

            # 5. Chờ hoàn tất kịch bản
            while time.time() - start_time < duration_sec:
                with proc_lock:
                    leechers = [
                        p for pid, p in active_procs.items() if pid.startswith("leech-")
                    ]
                if not churn_events and leechers and all(p.poll() is not None for p in leechers):
                    print("=== [Orchestrator] All static leechers finished! ===")
                    break
                time.sleep(0.3)

        finally:
            with proc_lock:
                procs_to_stop = list(active_procs.values())
            for p in procs_to_stop:
                if p and p.poll() is None:
                    p.terminate()
                    try:
                        p.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        p.kill()

            for t in reader_threads:
                t.join(timeout=1.0)

            if not keep_dirs and root_work_dir.exists():
                shutil.rmtree(root_work_dir)

    print(f"=== [Orchestrator] Logs saved to: {unified_log_path} ===")


def main():
    parser = argparse.ArgumentParser(description="DS02 Scenario Orchestrator (C1/C2/C3)")
    parser.add_argument("--scenario", default="scenarios/baseline.yaml")
    parser.add_argument("--peer-script", default="src/sim/stub_peer.py")
    parser.add_argument("--keep-dirs", action="store_true")
    args = parser.parse_args()
    run_scenario(args.scenario, args.peer_script, args.keep_dirs)


if __name__ == "__main__":
    main()