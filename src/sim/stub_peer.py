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
    parser.add_argument("--max-up-kbps", type=int, default=0, help="Max upload bandwidth in kbps")
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


def main():
    args = parse_args()
    peer_id = args.peer_id or f"{args.role}-{args.port}"
    Path(args.dir).mkdir(parents=True, exist_ok=True)

    num_chunks, piece_length = load_num_chunks(args.torrent)
    rng = random.Random(args.port)

    # 1. Báo hiệu bắt đầu khởi chạy và kết nối Tracker thành công
    log_event(
        peer_id,
        "started",
        meta={"role": args.role, "port": args.port, "max_up_kbps": args.max_up_kbps},
    )
    time.sleep(0.2)
    log_event(peer_id, "announce_ok", meta={"num_chunks": num_chunks})

    if args.role == "seed":
        # Seeder giữ kết nối và liên tục giả lập việc upload các mảnh (chunks)
        chunk_order = list(range(num_chunks))
        while running:
            time.sleep(rng.uniform(0.4, 0.8))
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
        # Leecher tải lần lượt các mảnh theo thứ tự ngẫu nhiên cho đến khi đủ
        chunks_to_download = list(range(num_chunks))
        rng.shuffle(chunks_to_download)

        for c_idx in chunks_to_download:
            if not running:
                log_event(peer_id, "stopped", meta={"reason": "interrupted"})
                sys.exit(1)
            delay = rng.uniform(0.3, 0.9)
            time.sleep(delay)
            log_event(
                peer_id,
                "piece_done",
                chunk_index=c_idx,
                meta={"bytes": piece_length, "from_peer": "seed-9001"},
            )

        # Tạo file giả sau khi tải xong và thoát với mã 0 (thành công)
        out_file = Path(args.dir) / "downloaded_file.bin"
        out_file.write_bytes(b"0" * (num_chunks * piece_length))
        log_event(peer_id, "completed", meta={"total_chunks": num_chunks})
        log_event(peer_id, "stopped", meta={"reason": "completed"})
        sys.exit(0)

    log_event(peer_id, "stopped", meta={"reason": "graceful_shutdown"})
    sys.exit(0)


if __name__ == "__main__":
    main()