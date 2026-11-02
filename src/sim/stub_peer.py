import argparse
import json
import math
import os
import random
import signal
import sys
import time
from pathlib import Path

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


def load_num_chunks(torrent_path: str) -> tuple[int, int]:
    if os.path.exists(torrent_path):
        with open(torrent_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            file_size = data.get("file_size_bytes", 1048576)
            piece_length = data.get("piece_length", 262144)
            return math.ceil(file_size / piece_length), piece_length
    return 4, 262144


def compute_transfer_delay(piece_length_bytes: int, max_up_kbps: int, latency_ms: int, jitter: float) -> float:
    # Tính thời gian truyền dựa trên băng thông cấu hình (kbps = kilobits/sec) và độ trễ (ms)
    effective_kbps = max(max_up_kbps, 64)
    transfer_sec = (piece_length_bytes * 8 / 1024.0) / effective_kbps
    # Nhân hệ số 0.25 để mô phỏng tải songong nhanh gọn trong lúc test
    scaled_sec = transfer_sec * 0.25 + (latency_ms / 1000.0) + jitter
    return max(0.15, scaled_sec)


def main():
    args = parse_args()
    peer_id = args.peer_id or f"{args.role}-{args.port}"
    Path(args.dir).mkdir(parents=True, exist_ok=True)

    num_chunks, piece_length = load_num_chunks(args.torrent)
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
                piece_length, args.max_up_kbps, args.latency_ms, rng.uniform(0.0, 0.15)
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

        for c_idx in chunks_to_download:
            if not running:
                log_event(peer_id, "stopped", meta={"reason": "interrupted"})
                sys.exit(1)
            delay = compute_transfer_delay(
                piece_length, args.max_up_kbps, args.latency_ms, rng.uniform(0.0, 0.15)
            )
            time.sleep(delay)
            if not running:
                log_event(peer_id, "stopped", meta={"reason": "interrupted"})
                sys.exit(1)
            log_event(
                peer_id,
                "piece_done",
                chunk_index=c_idx,
                meta={"bytes": piece_length, "from_peer": "seed-9001"},
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