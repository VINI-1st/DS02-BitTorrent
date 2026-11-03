import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path


def parse_Logs(log_path: Path) -> list[dict]:
    records = []
    if not log_path.exists():
        raise FileNotFoundError(f"Log file not found: {log_path}")
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    records.sort(key=lambda r: float(r.get("ts", 0.0)))
    return records


def analyze_and_export(scenario_name: str, log_path: Path, out_dir: Path):
    records = parse_Logs(log_path)
    if not records:
        print(f"[C4 Metrics] No valid JSON records found in {log_path}")
        return

    out_dir.mkdir(parents=True, exist_ok=True)
    t0 = float(records[0]["ts"])
    t_end = float(records[-1]["ts"])
    total_sec = max(1, math.ceil(t_end - t0) + 1)

    # 1. Theo dõi thời gian tải của từng peer
    peer_roles: dict[str, str] = {}
    start_ts: dict[str, float] = {}
    complete_ts: dict[str, float] = {}
    chunks_downloaded: dict[str, int] = defaultdict(int)
    bytes_downloaded: dict[str, int] = defaultdict(int)

    # 2. Theo dõi thông lượng và tải tracker theo từng giây (time bucket)
    bytes_per_sec: dict[int, int] = defaultdict(int)
    uploads_per_sec: dict[int, int] = defaultdict(int)
    tracker_reqs_per_sec: dict[int, int] = defaultdict(int)

    # 3. Theo dõi sự kiện phục vụ tính hệ số nhân bản (chunk replication)
    max_chunk_idx = 3
    for r in records:
        c_idx = r.get("chunk_index")
        if isinstance(c_idx, int) and c_idx > max_chunk_idx:
            max_chunk_idx = c_idx
        meta = r.get("meta") or {}
        if "num_chunks" in meta:
            max_chunk_idx = max(max_chunk_idx, int(meta["num_chunks"]) - 1)
    num_chunks = max_chunk_idx + 1

    for r in records:
        ts = float(r["ts"])
        rel_sec = int(max(0.0, ts - t0))
        pid = str(r.get("peer_id", ""))
        ev = str(r.get("event", ""))
        c_idx = r.get("chunk_index")
        meta = r.get("meta") or {}

        if ev == "started":
            role = meta.get("role", "seed" if pid.startswith("seed") else "leech")
            peer_roles[pid] = role
            start_ts.setdefault(pid, ts)
        elif ev == "announce_ok":
            tracker_reqs_per_sec[rel_sec] += 1
        elif ev == "piece_done":
            sz = int(meta.get("bytes", 262144))
            chunks_downloaded[pid] += 1
            bytes_downloaded[pid] += sz
            bytes_per_sec[rel_sec] += sz
        elif ev == "piece_uploaded":
            sz = int(meta.get("bytes", 262144))
            uploads_per_sec[rel_sec] += sz
        elif ev == "completed":
            complete_ts[pid] = ts

    # --- XUẤT CSV 1: Thống kê từng Peer và Tỉ lệ hoàn tất ---
    peer_csv = out_dir / f"{scenario_name}_peers.csv"
    leechers = [p for p, role in peer_roles.items() if role == "leech"]
    completed_leechers = 0

    with open(peer_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "peer_id",
            "role",
            "chunks_downloaded",
            "bytes_downloaded",
            "download_time_sec",
            "avg_throughput_kbps",
            "completed",
        ])
        for pid, role in sorted(peer_roles.items()):
            s_t = start_ts.get(pid, t0)
            is_done = pid in complete_ts
            if role == "leech" and is_done:
                completed_leechers += 1
            dl_time = round(complete_ts[pid] - s_t, 3) if is_done else round(t_end - s_t, 3)
            dl_bytes = bytes_downloaded[pid]
            avg_kbps = round((dl_bytes * 8 / 1024.0) / max(dl_time, 0.001), 2) if dl_bytes > 0 else 0.0
            writer.writerow([
                pid,
                role,
                chunks_downloaded[pid],
                dl_bytes,
                dl_time,
                avg_kbps,
                1 if is_done else 0,
            ])

    # --- XUẤT CSV 2: Chuỗi thời gian Thông lượng, Tải Tracker & Hệ số nhân bản Chunk ---
    timeseries_csv = out_dir / f"{scenario_name}_timeseries.csv"
    active_peers: set[str] = set()
    peer_chunks: dict[str, set[int]] = defaultdict(set)
    rec_idx = 0
    n_recs = len(records)

    header = [
        "rel_sec",
        "active_peers",
        "download_kbps",
        "upload_kbps",
        "tracker_announces",
        "avg_chunk_replication",
    ] + [f"chunk_{i}_replicas" for i in range(num_chunks)]

    with open(timeseries_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)

        for sec in range(total_sec):
            cutoff_ts = t0 + sec + 1.0
            while rec_idx < n_recs and float(records[rec_idx]["ts"]) < cutoff_ts:
                r = records[rec_idx]
                pid = str(r.get("peer_id", ""))
                ev = str(r.get("event", ""))
                c_idx = r.get("chunk_index")
                meta = r.get("meta") or {}

                if ev == "started":
                    active_peers.add(pid)
                    if meta.get("role") == "seed" or pid.startswith("seed"):
                        peer_chunks[pid] = set(range(num_chunks))
                elif ev == "piece_done" and isinstance(c_idx, int):
                    peer_chunks[pid].add(c_idx)
                elif ev == "stopped":
                    active_peers.discard(pid)
                elif ev in ("peer_lost", "churn_leave_graceful"):
                    target = meta.get("target")
                    if target:
                        active_peers.discard(str(target))
                rec_idx += 1

            # Đếm số bản sao của từng chunk trên các peer đang online
            chunk_counts = []
            for c in range(num_chunks):
                cnt = sum(1 for p in active_peers if c in peer_chunks[p])
                chunk_counts.append(cnt)

            avg_rep = round(sum(chunk_counts) / max(1, len(chunk_counts)), 2)
            dl_kbps = round(bytes_per_sec[sec] * 8 / 1024.0, 2)
            up_kbps = round(uploads_per_sec[sec] * 8 / 1024.0, 2)

            writer.writerow(
                [
                    sec,
                    len(active_peers),
                    dl_kbps,
                    up_kbps,
                    tracker_reqs_per_sec[sec],
                    avg_rep,
                ]
                + chunk_counts
            )

    # --- XUẤT CSV 3: Tổng hợp toàn kịch bản (Summary) ---
    summary_csv = out_dir / "summary_metrics.csv"
    completion_ratio = round(completed_leechers / max(1, len(leechers)) * 100.0, 2)
    dl_times = [
        complete_ts[p] - start_ts[p]
        for p in leechers
        if p in complete_ts and p in start_ts
    ]
    mean_dl_time = round(sum(dl_times) / len(dl_times), 3) if dl_times else 0.0
    total_announces = sum(tracker_reqs_per_sec.values())

    write_header = not summary_csv.exists()
    with open(summary_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow([
                "scenario",
                "total_leechers",
                "completed_leechers",
                "completion_ratio_pct",
                "mean_download_time_sec",
                "total_tracker_announces",
            ])
        writer.writerow([
            scenario_name,
            len(leechers),
            completed_leechers,
            completion_ratio,
            mean_dl_time,
            total_announces,
        ])

    print(f"=== [C4 Metrics] Exported CSVs for '{scenario_name}' to {out_dir} ===")
    print(f"    -> Completion ratio: {completion_ratio}% ({completed_leechers}/{len(leechers)} leechers)")
    print(f"    -> Mean download time: {mean_dl_time}s")


def main():
    parser = argparse.ArgumentParser(description="DS02 Metrics Extractor (C4)")
    parser.add_argument("--scenario", default="baseline", help="Scenario name")
    parser.add_argument("--log", default=None, help="Path to events.jsonl")
    parser.add_argument("--out-dir", default="results", help="Output directory for CSV files")
    args = parser.parse_args()

    log_path = Path(args.log) if args.log else Path("logs") / args.scenario / "events.jsonl"
    analyze_and_export(args.scenario, log_path, Path(args.out_dir))


if __name__ == "__main__":
    main()