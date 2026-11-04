import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from collections import deque
from pathlib import Path
import yaml

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


class LiveDemoDashboard:
    """C7 Real-time Terminal Dashboard: Theo dõi trực tiếp trạng thái Swarm và 7 kịch bản bắt buộc."""

    def __init__(self, num_chunks: int = 4):
        self.num_chunks = num_chunks
        self.lock = threading.Lock()
        self.peer_roles: dict[str, str] = {}
        self.peer_status: dict[str, str] = {}
        self.peer_chunks: dict[str, set[int]] = {}
        self.peer_uploaded: dict[str, int] = {}
        self.recent_events: deque[str] = deque(maxlen=8)
        self.checklist = {
            "1_metadata_shared": False,
            "2_parallel_download": False,
            "3_upload_while_downloading": False,
            "4_stop_one_source": False,
            "5_switch_source": False,
            "6_detect_invalid_chunk": False,
            "7_verify_final_hash": False,
        }
        self.sources_used_by_peer: dict[str, set[str]] = {}

    def record_event(self, record: dict):
        with self.lock:
            pid = str(record.get("peer_id", ""))
            ev = str(record.get("event", ""))
            c_idx = record.get("chunk_index")
            meta = record.get("meta") or {}

            if ev == "started":
                role = meta.get("role", "seed" if pid.startswith("seed") else "leech")
                self.peer_roles[pid] = role
                self.peer_status[pid] = "ONLINE"
                self.peer_uploaded.setdefault(pid, 0)
                if role == "seed":
                    self.peer_chunks[pid] = set(range(self.num_chunks))
                else:
                    self.peer_chunks.setdefault(pid, set())
                self.recent_events.append(f"[{pid}] Started as {role.upper()}")

            elif ev == "announce_ok":
                self.checklist["1_metadata_shared"] = True
                self.recent_events.append(f"[{pid}] Registered/announced with Tracker")

            elif ev == "piece_done" and isinstance(c_idx, int):
                self.peer_chunks.setdefault(pid, set()).add(c_idx)
                src = str(meta.get("from_peer", "seed"))
                srcs = self.sources_used_by_peer.setdefault(pid, set())
                srcs.add(src)
                if len(srcs) >= 2:
                    self.checklist["2_parallel_download"] = True
                self.recent_events.append(f"[{pid}] Downloaded chunk #{c_idx} from {src} (SHA-1 OK)")

            elif ev == "piece_uploaded":
                self.peer_uploaded[pid] = self.peer_uploaded.get(pid, 0) + 1
                if self.peer_roles.get(pid) == "leech" and len(self.peer_chunks.get(pid, set())) < self.num_chunks:
                    self.checklist["3_upload_while_downloading"] = True
                self.recent_events.append(f"[{pid}] Uploaded chunk #{c_idx} to swarm")

            elif ev == "invalid_chunk":
                self.checklist["6_detect_invalid_chunk"] = True
                self.recent_events.append(f"[ALERT] {pid} rejected corrupted chunk #{c_idx} (Hash mismatch!)")

            elif ev == "switch_source":
                self.checklist["5_switch_source"] = True
                self.recent_events.append(
                    f"[FAILOVER] {pid} switched chunk #{c_idx} source: {meta.get('old_peer')} -> {meta.get('new_peer')}"
                )

            elif ev == "peer_lost":
                target = str(meta.get("target", pid))
                self.peer_status[target] = "OFFLINE (STOPPED)"
                self.checklist["4_stop_one_source"] = True
                self.recent_events.append(f"[FAULT] Source peer {target} abruptly stopped!")

            elif ev == "completed":
                self.peer_status[pid] = "COMPLETED (100%)"
                self.recent_events.append(f"[SUCCESS] {pid} merged all chunks & verified file hash!")

            elif ev == "stopped" and self.peer_status.get(pid) == "ONLINE":
                self.peer_status[pid] = "STOPPED"

    def render_text(self) -> str:
        with self.lock:
            lines = []
            lines.append("======================================================================")
            lines.append("        DS02 BITTORRENT P2P LIVE MONITOR DASHBOARD (ROLE C - C7)      ")
            lines.append("======================================================================")
            online_cnt = sum(1 for s in self.peer_status.values() if "ONLINE" in s or "COMPLETED" in s)
            lines.append(f" Active Peers in Swarm: {online_cnt}/{len(self.peer_roles)}   |   Total Chunks: {self.num_chunks}")
            lines.append("----------------------------------------------------------------------")
            lines.append(f" {'PEER ID':<12} | {'ROLE':<6} | {'STATUS':<18} | {'BITFIELD & PROGRESS':<22} | {'UP'}")
            lines.append("----------------------------------------------------------------------")

            for pid in sorted(self.peer_roles.keys()):
                role = self.peer_roles[pid]
                status = self.peer_status.get(pid, "UNKNOWN")
                owned = self.peer_chunks.get(pid, set())
                bitfield = "".join("#" if i in owned else "." for i in range(self.num_chunks))
                pct = int(len(owned) / max(1, self.num_chunks) * 100)
                prog_str = f"[{bitfield}] {len(owned)}/{self.num_chunks} ({pct:>3}%)"
                up_cnt = self.peer_uploaded.get(pid, 0)
                lines.append(f" {pid:<12} | {role:<6} | {status:<18} | {prog_str:<22} | {up_cnt}")

            lines.append("----------------------------------------------------------------------")
            lines.append(" MANDATORY DEMO SCENARIOS CHECKLIST (Section 6.4):")
            labels = [
                ("1_metadata_shared", "1. Create metadata & register file with Tracker"),
                ("2_parallel_download", "2. Parallel download from >= 2 peers"),
                ("3_upload_while_downloading", "3. Leecher uploads chunk before completing download"),
                ("4_stop_one_source", "4. Stop one source peer during downloading"),
                ("5_switch_source", "5. Automatically switch to another source peer"),
                ("6_detect_invalid_chunk", "6. Detect & reject invalid chunk (hash mismatch)"),
                ("7_verify_final_hash", "7. Merge chunks & verify final SHA-256 file hash"),
            ]
            for key, desc in labels:
                mark = "[x]" if self.checklist[key] else "[ ]"
                lines.append(f"   {mark} {desc}")

            lines.append("----------------------------------------------------------------------")
            lines.append(" RECENT SWARM EVENTS:")
            for ev_str in self.recent_events:
                lines.append(f"   > {ev_str}")
            lines.append("======================================================================")
            return "\n".join(lines)


def run_live_demo(scenario_path: str, peer_cmd: str):
    with open(scenario_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    scen_name = cfg.get("name", "demo_live")
    swarm_cfg = cfg.get("swarm", {})
    torrent_cfg = cfg.get("torrent", {})
    num_seeds = int(swarm_cfg.get("num_seeds", 2))
    num_leechers = int(swarm_cfg.get("num_leechers", 3))
    base_port = int(swarm_cfg.get("base_port", 9001))
    max_up_kbps = int(swarm_cfg.get("max_up_kbps", 256))

    file_size = int(torrent_cfg.get("file_size_bytes", 1048576))
    piece_len = int(torrent_cfg.get("piece_length", 262144))
    num_chunks = (file_size + piece_len - 1) // piece_len

    work_dir = Path("sim_workspace") / scen_name
    if work_dir.exists():
        shutil.rmtree(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    log_dir = Path("logs") / scen_name
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "events.jsonl"

    torrent_path = work_dir / "demo.torrent.json"
    torrent_path.write_text(
        json.dumps(
            {
                "file_name": torrent_cfg.get("file_name", "demo.bin"),
                "file_size_bytes": file_size,
                "piece_length": piece_len,
                "num_seeds": num_seeds,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    dashboard = LiveDemoDashboard(num_chunks=num_chunks)
    log_lock = threading.Lock()
    procs: dict[str, subprocess.Popen] = {}

    def _reader(proc: subprocess.Popen, lf):
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            with log_lock:
                lf.write(line + "\n")
                lf.flush()
            try:
                rec = json.loads(line)
                dashboard.record_event(rec)
            except json.JSONDecodeError:
                pass

    with open(log_path, "w", encoding="utf-8") as lf:
        try:
            for i in range(num_seeds):
                port = base_port + i
                pid = f"seed-{port}"
                p_dir = work_dir / f"seed_{port}"
                p = subprocess.Popen(
                    [
                        sys.executable,
                        peer_cmd,
                        "--torrent", str(torrent_path),
                        "--port", str(port),
                        "--dir", str(p_dir),
                        "--role", "seed",
                        "--max-up-kbps", str(max_up_kbps),
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                procs[pid] = p
                threading.Thread(target=_reader, args=(p, lf), daemon=True).start()

            time.sleep(0.3)

            for j in range(num_leechers):
                port = base_port + num_seeds + j
                pid = f"leech-{port}"
                p_dir = work_dir / f"leech_{port}"
                p = subprocess.Popen(
                    [
                        sys.executable,
                        peer_cmd,
                        "--torrent", str(torrent_path),
                        "--port", str(port),
                        "--dir", str(p_dir),
                        "--role", "leech",
                        "--max-up-kbps", str(max_up_kbps),
                    ],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
                procs[pid] = p
                threading.Thread(target=_reader, args=(p, lf), daemon=True).start()

            start_ts = time.time()
            killed_seed = False

            while time.time() - start_ts < 15.0:
                if not killed_seed and time.time() - start_ts >= 0.8:
                    victim = f"seed-{base_port + 1}"
                    if victim in procs and procs[victim].poll() is None:
                        procs[victim].kill()
                        ev_rec = {
                            "ts": round(time.time(), 3),
                            "peer_id": "simulator",
                            "event": "peer_lost",
                            "chunk_index": None,
                            "meta": {"target": victim, "reason": "demo_stop_source"},
                        }
                        with log_lock:
                            lf.write(json.dumps(ev_rec) + "\n")
                            lf.flush()
                        dashboard.record_event(ev_rec)
                        killed_seed = True

                os.system("cls" if os.name == "nt" else "clear")
                print(dashboard.render_text(), flush=True)

                leech_list = [p for k, p in procs.items() if k.startswith("leech-")]
                if leech_list and all(p.poll() is not None for p in leech_list):
                    expected_hash = hashlib.sha256(b"0" * (num_chunks * piece_len)).hexdigest()
                    all_match = True
                    for j in range(num_leechers):
                        port = base_port + num_seeds + j
                        f_out = work_dir / f"leech_{port}" / "downloaded_file.bin"
                        if not f_out.exists() or hashlib.sha256(f_out.read_bytes()).hexdigest() != expected_hash:
                            all_match = False
                    if all_match:
                        dashboard.checklist["7_verify_final_hash"] = True
                        os.system("cls" if os.name == "nt" else "clear")
                        print(dashboard.render_text(), flush=True)
                        print(f"\n[FINAL HASH VERIFIED] SHA-256: {expected_hash}")
                    break

                time.sleep(0.35)

        finally:
            for p in procs.values():
                if p.poll() is None:
                    p.terminate()
            if work_dir.exists():
                shutil.rmtree(work_dir)


def main():
    parser = argparse.ArgumentParser(description="DS02 Single-Command Live Demo (C7)")
    parser.add_argument("--scenario", default="scenarios/demo_live.yaml")
    parser.add_argument("--peer-script", default="src/sim/stub_peer.py")
    args = parser.parse_args()
    run_live_demo(args.scenario, args.peer_script)


if __name__ == "__main__":
    main()