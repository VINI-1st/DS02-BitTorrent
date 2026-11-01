import argparse
import json
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
import yaml


def stream_reader(proc: subprocess.Popen, log_file, lock: threading.Lock):
    """Đọc từng dòng log từ một tiến trình Peer và ghi chung vào file events.jsonl"""
    assert proc.stdout is not None
    for line in proc.stdout:
        line_str = line.strip()
        if not line_str:
            continue
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


def run_scenario(scenario_path: str, peer_cmd: str, keep_dirs: bool = False):
    with open(scenario_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    scen_name = cfg.get("name", "default")
    swarm_cfg = cfg.get("swarm", {})
    sim_cfg = cfg.get("simulation", {})

    num_seeds = swarm_cfg.get("num_seeds", 1)
    num_leechers = swarm_cfg.get("num_leechers", 3)
    base_port = swarm_cfg.get("base_port", 9001)
    max_up_kbps = swarm_cfg.get("max_up_kbps", 512)
    duration_sec = sim_cfg.get("duration_sec", 20)

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

    seed_procs: list[subprocess.Popen] = []
    leech_procs: list[subprocess.Popen] = []
    reader_threads: list[threading.Thread] = []
    log_lock = threading.Lock()

    print(f"=== [C1 Launcher] Starting scenario '{scen_name}' ===")
    with open(unified_log_path, "w", encoding="utf-8") as log_file:
        try:
            # 1. Khởi chạy các máy Seeder
            for i in range(num_seeds):
                port = base_port + i
                peer_dir = root_work_dir / f"seed_{port}"
                cmd = [
                    sys.executable,
                    peer_cmd,
                    "--torrent", str(torrent_path),
                    "--port", str(port),
                    "--dir", str(peer_dir),
                    "--role", "seed",
                    "--max-up-kbps", str(max_up_kbps),
                ]
                proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
                )
                seed_procs.append(proc)
                t = threading.Thread(
                    target=stream_reader, args=(proc, log_file, log_lock), daemon=True
                )
                t.start()
                reader_threads.append(t)

            time.sleep(0.5)

            # 2. Khởi chạy các máy Leecher
            for j in range(num_leechers):
                port = base_port + num_seeds + j
                peer_dir = root_work_dir / f"leech_{port}"
                cmd = [
                    sys.executable,
                    peer_cmd,
                    "--torrent", str(torrent_path),
                    "--port", str(port),
                    "--dir", str(peer_dir),
                    "--role", "leech",
                    "--max-up-kbps", str(max_up_kbps),
                ]
                proc = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
                )
                leech_procs.append(proc)
                t = threading.Thread(
                    target=stream_reader, args=(proc, log_file, log_lock), daemon=True
                )
                t.start()
                reader_threads.append(t)

            # 3. Chờ tất cả Leecher tải xong hoặc hết thời gian kịch bản
            start_time = time.time()
            while time.time() - start_time < duration_sec:
                if all(p.poll() is not None for p in leech_procs):
                    print("=== [C1 Launcher] All leechers completed download! ===")
                    break
                time.sleep(0.3)

        finally:
            # 4. Tự động dọn dẹp và tắt các tiến trình còn sót
            for p in leech_procs + seed_procs:
                if p.poll() is None:
                    p.terminate()
                    try:
                        p.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        p.kill()

            for t in reader_threads:
                t.join(timeout=1.0)

            if not keep_dirs and root_work_dir.exists():
                shutil.rmtree(root_work_dir)

    print(f"=== [C1 Launcher] Logs saved to: {unified_log_path} ===")


def main():
    parser = argparse.ArgumentParser(description="DS02 Scenario Orchestrator (C1)")
    parser.add_argument(
        "--scenario",
        default="scenarios/baseline.yaml",
        help="Path to scenario YAML file",
    )
    parser.add_argument(
        "--peer-script",
        default="src/sim/stub_peer.py",
        help="Path to peer executable script (StubPeer or real Peer)",
    )
    parser.add_argument(
        "--keep-dirs",
        action="store_true",
        help="Keep temporary peer working directories after run",
    )
    args = parser.parse_args()
    run_scenario(args.scenario, args.peer_script, args.keep_dirs)


if __name__ == "__main__":
    main()