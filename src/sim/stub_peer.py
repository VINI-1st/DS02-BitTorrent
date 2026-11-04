import argparse
import json
import math
import os
import random
import signal
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

running = True


def handle_sigterm(signum, frame):
    global running
    running = False


signal.signal(signal.SIGINT, handle_sigterm)
signal.signal(signal.SIGTERM, handle_sigterm)


def log_event(peer_id: str, event: str, chunk_index=None, meta=None):
    record = {
        "ts": round(time.time(), 3),
        "peer_id": peer_id,
        "event": event,
        "chunk_index": chunk_index,
        "meta": meta or {},
    }
    print(json.dumps(record), flush=True)


def parse_args():
    parser = argparse.ArgumentParser(description="StubPeer for DS02 Simulation")
    parser.add_argument("--torrent", required=True, help="Path to torrent metainfo JSON")
    parser.add_argument("--port", type=int, required=True, help="Listening port")
    parser.add_argument("--dir", required=True, help="Working directory for peer")
    parser.add_argument("--role", choices=["seed", "leech"], required=True, help="Peer role")
    parser.add_argument("--max-up-kbps", type=int, default=512, help="Max upload bandwidth in kbps")
    parser.add_argument("--latency-ms", type=int, default=0, help="Simulated network latency in ms")
    parser.add_argument("--peer-id", default=None, help="Optional explicit peer ID")
    return parser.parse_args()


def load_torrent_info(torrent_path: str) -> tuple[int, int, int]:
    if os.path.exists(torrent_path):
        with open(torrent_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            file_size = data.get("file_size_bytes", 1048576)
            piece_length = data.get("piece_length", 262144)
            num_seeds = max(1, int(data.get("num_seeds", 1)))
            return math.ceil(file_size / piece_length), piece_length, num_seeds
    return 4, 262144, 1


def compute_transfer_delay(
    piece_length_bytes: int, max_up_kbps: int, latency_ms: int, jitter: float, num_seeds: int = 1
) -> float:
    effective_kbps = max(max_up_kbps, 64)
    parallel_factor = min(4, num_seeds)
    transfer_sec = (piece_length_bytes * 8 / 1024.0) / (effective_kbps * (1.0 + 0.45 * (parallel_factor - 1)))
    scaled_sec = transfer_sec * 0.25 + (latency_ms / 1000.0) + jitter
    return max(0.12, scaled_sec)


def main():
    args = parse_args()
    peer_id = args.peer_id or f"{args.role}-{args.port}"
    Path(args.dir).mkdir(parents=True, exist_ok=True)

    num_chunks, piece_length, num_seeds = load_torrent_info(args.torrent)
    rng = random.Random(args.port)

    log_event(
        peer_id,
        "started",
        meta={
            "role": args.role,
            "port": args.port,
            "max_up_kbps": args.max_up_kbps,
            "latency_ms": args.latency_ms,
        },
    )
    time.sleep(0.15 + args.latency_ms / 1000.0)
    log_event(peer_id, "announce_ok", meta={"num_chunks": num_chunks})

    if args.role == "seed":
        chunk_order = list(range(num_chunks))
        while running:
            delay = compute_transfer_delay(
                piece_length, args.max_up_kbps, args.latency_ms, rng.uniform(0.0, 0.12), 1
            )
            time.sleep(delay)
            if not running:
                break
            c_idx = rng.choice(chunk_order)
            log_event(
                peer_id,
                "piece_uploaded",
                chunk_index=c_idx,
                meta={"bytes": piece_length},
            )
    else:
        chunks_to_download = list(range(num_chunks))
        rng.shuffle(chunks_to_download)
        seed_sources = [f"seed-{9001 + i}" for i in range(num_seeds)]

        for step_idx, c_idx in enumerate(chunks_to_download):
            if not running:
                log_event(peer_id, "stopped", meta={"reason": "interrupted"})
                sys.exit(1)

            delay = compute_transfer_delay(
                piece_length, args.max_up_kbps, args.latency_ms, rng.uniform(0.0, 0.12), num_seeds
            )
            time.sleep(delay)
            if not running:
                log_event(peer_id, "stopped", meta={"reason": "interrupted"})
                sys.exit(1)

            src_peer = seed_sources[step_idx % len(seed_sources)]

            # Mô phỏng phát hiện chunk sai hash và chuyển nguồn ở leecher đầu tiên
            if args.port == 9001 + num_seeds and step_idx == 1:
                log_event(
                    peer_id,
                    "invalid_chunk",
                    chunk_index=c_idx,
                    meta={"from_peer": src_peer, "reason": "sha1_mismatch"},
                )
                alt_peer = seed_sources[(step_idx + 1) % len(seed_sources)]
                log_event(
                    peer_id,
                    "switch_source",
                    chunk_index=c_idx,
                    meta={"old_peer": src_peer, "new_peer": alt_peer},
                )
                time.sleep(0.15)
                src_peer = alt_peer

            log_event(
                peer_id,
                "piece_done",
                chunk_index=c_idx,
                meta={"bytes": piece_length, "from_peer": src_peer},
            )

            # Ngay khi có ít nhất 1 chunk (chưa tải xong cả file), leecher upload cho peer khác
            if step_idx == 0:
                log_event(
                    peer_id,
                    "piece_uploaded",
                    chunk_index=c_idx,
                    meta={"bytes": piece_length, "note": "uploading_before_completion"},
                )

        out_file = Path(args.dir) / "downloaded_file.bin"
        out_file.write_bytes(b"0" * (num_chunks * piece_length))
        log_event(peer_id, "completed", meta={"total_chunks": num_chunks})
        log_event(peer_id, "stopped", meta={"reason": "completed"})
        sys.exit(0)

    log_event(peer_id, "stopped", meta={"reason": "graceful_shutdown"})
    sys.exit(0)


if __name__ == "__main__":
    main()