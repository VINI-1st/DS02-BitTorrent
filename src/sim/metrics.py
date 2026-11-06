"""C4 – Metrics extraction from a Contract 5 event log.

    py src/sim/metrics.py --scenario baseline            # reads logs/baseline/events.jsonl
    py src/sim/metrics.py --log path/to/events.jsonl --scenario myrun --out-dir results

Outputs (in ``--out-dir``, default ``results/``):

* ``<scenario>_peers.csv``      – one row per peer: role, download_time_sec,
  bytes/pieces downloaded and uploaded, distinct sources, fault counters, exit code.
* ``<scenario>_timeseries.csv`` – one row per time bucket: active peers,
  ``download_kbps``, ``upload_kbps``, ``tracker_announces``,
  ``tracker_announces_failed``, ``tracker_up``, ``completion_ratio_pct``,
  ``avg_chunk_replication``, ``min_chunk_replication`` and ``chunk_<i>_replicas``.
* ``<scenario>_summary.csv``    – one row with the scenario-level numbers
  (also returned as a dict and aggregated by run_experiments into
  ``summary_metrics.csv``).

Units: ``*_kbps`` = KiB/s, the same unit as ``--max-up-kbps`` in Contract 4.

Identity of a node: ``meta.node`` (added by the launcher when a real peer uses
another id) or ``peer_id``. Role comes from the launcher's ``spawned`` event,
else ``started.meta.role``, else the ``seed-``/``leech-`` prefix.
A node is *online* from ``started`` until ``stopped`` or ``process_exit``.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.sim.common import REPO_ROOT, read_jsonl  # noqa: E402

SIM_IDS = {"simulator"}
TRACKER_IDS = {"tracker"}


def node_of(rec: dict) -> str:
    meta = rec.get("meta") or {}
    return str(meta.get("node") or rec.get("peer_id", ""))


def parse_logs(log_path: str | Path) -> list[dict]:
    path = Path(log_path)
    if not path.exists():
        raise FileNotFoundError(f"log file not found: {path}")
    recs = [r for r in read_jsonl(path) if isinstance(r.get("ts"), (int, float)) and "event" in r]
    recs.sort(key=lambda r: float(r["ts"]))
    return recs


# backward compatible name used by older scripts
parse_Logs = parse_logs


@dataclass
class PeerStats:
    node: str
    role: str = "leech"
    start_ts: float | None = None
    end_ts: float | None = None
    complete_ts: float | None = None
    pieces_down: int = 0
    bytes_down: int = 0
    pieces_up: int = 0
    bytes_up: int = 0
    sources: set[str] = field(default_factory=set)
    invalid: int = 0
    lost: int = 0
    switches: int = 0
    announces: int = 0
    announce_failures: int = 0
    exit_code: Any = ""
    end_reason: str = ""
    have: set[int] = field(default_factory=set)
    uploaded_before_complete: int = 0


def _role_hint(node: str) -> str:
    return "seed" if node.startswith("seed") else "leech"


def analyze(records: list[dict], bucket_sec: float = 1.0) -> dict[str, Any]:
    """Pure function: compute all metrics from parsed records."""
    if not records:
        raise ValueError("no records")
    t0 = float(records[0]["ts"])
    t_end = float(records[-1]["ts"])
    for r in records:
        if r.get("event") == "scenario_start":
            t0 = float(r["ts"])
            break

    num_pieces = 0
    for r in records:
        meta = r.get("meta") or {}
        for key in ("num_pieces", "total_pieces"):
            if isinstance(meta.get(key), int):
                num_pieces = max(num_pieces, int(meta[key]))
        ci = r.get("chunk_index")
        if isinstance(ci, int) and r.get("event") in ("piece_done", "piece_uploaded"):
            num_pieces = max(num_pieces, ci + 1)

    peers: dict[str, PeerStats] = {}
    roles: dict[str, str] = {}
    tracker_windows: list[tuple[float, float | None]] = []
    crash_open: float | None = None
    restart_ts: list[float] = []

    def peer(node: str) -> PeerStats:
        if node not in peers:
            peers[node] = PeerStats(node, roles.get(node, _role_hint(node)))
        return peers[node]

    for r in records:
        ev = r.get("event")
        meta = r.get("meta") or {}
        if r.get("peer_id") in SIM_IDS and ev == "spawned" and meta.get("role") in ("seed", "leech"):
            roles[str(meta["target"])] = str(meta["role"])
            if str(meta["target"]) in peers:
                peers[str(meta["target"])].role = str(meta["role"])

    nbuckets = max(1, math.ceil((t_end - t0) / bucket_sec) + 1)
    dl = [0] * nbuckets
    ul = [0] * nbuckets
    ann = [0] * nbuckets
    ann_fail = [0] * nbuckets

    def bucket(ts: float) -> int:
        return min(nbuckets - 1, max(0, int((ts - t0) // bucket_sec)))

    for r in records:
        ev = r.get("event")
        pid = str(r.get("peer_id", ""))
        meta = r.get("meta") or {}
        ts = float(r["ts"])
        if pid in SIM_IDS:
            if ev == "process_exit" and meta.get("role") in ("seed", "leech"):
                p = peer(str(meta["target"]))
                p.end_ts = ts if p.end_ts is None else min(p.end_ts, ts)
                p.exit_code = meta.get("returncode")
                p.end_reason = str(meta.get("end_reason", ""))
            continue
        if pid in TRACKER_IDS and not meta.get("node"):
            if ev == "tracker_crashed":
                crash_open = ts
            elif ev == "tracker_restarted":
                tracker_windows.append((crash_open if crash_open is not None else ts, ts))
                restart_ts.append(ts)
                crash_open = None
            continue
        node = node_of(r)
        p = peer(node)
        if ev == "started":
            if meta.get("role") in ("seed", "leech") and node not in roles:
                p.role = str(meta["role"])
            p.start_ts = ts if p.start_ts is None else p.start_ts
            if p.role == "seed":
                p.have = set(range(num_pieces))
        elif ev == "piece_done":
            sz = int(meta.get("bytes", 0) or 0)
            p.pieces_down += 1
            p.bytes_down += sz
            dl[bucket(ts)] += sz
            if meta.get("from_peer"):
                p.sources.add(str(meta["from_peer"]))
            if isinstance(r.get("chunk_index"), int):
                p.have.add(int(r["chunk_index"]))
        elif ev == "piece_uploaded":
            sz = int(meta.get("bytes", 0) or 0)
            p.pieces_up += 1
            p.bytes_up += sz
            ul[bucket(ts)] += sz
            if p.role == "leech" and p.complete_ts is None:
                p.uploaded_before_complete += 1
        elif ev == "completed":
            p.complete_ts = ts if p.complete_ts is None else p.complete_ts
        elif ev == "invalid_chunk":
            p.invalid += 1
        elif ev == "peer_lost":
            p.lost += 1
        elif ev == "switch_source":
            p.switches += 1
        elif ev == "announce_ok":
            p.announces += 1
            ann[bucket(ts)] += 1
        elif ev == "announce_failed":
            p.announce_failures += 1
            ann_fail[bucket(ts)] += 1
        elif ev == "stopped":
            p.end_ts = ts if p.end_ts is None else min(p.end_ts, ts)
    if crash_open is not None:
        tracker_windows.append((crash_open, None))

    peers = {k: v for k, v in peers.items() if v.start_ts is not None}

    # ------------------------------------------------------------------ time series
    by_ts: list[tuple[float, str, str, int | None]] = []
    for r in records:
        pid = str(r.get("peer_id", ""))
        ev = str(r.get("event"))
        meta = r.get("meta") or {}
        if pid in SIM_IDS and ev == "process_exit":
            by_ts.append((float(r["ts"]), str(meta.get("target")), "offline", None))
        elif pid not in SIM_IDS and pid not in TRACKER_IDS:
            node = node_of(r)
            if ev == "started":
                by_ts.append((float(r["ts"]), node, "online", None))
            elif ev == "stopped":
                by_ts.append((float(r["ts"]), node, "offline", None))
            elif ev == "piece_done" and isinstance(r.get("chunk_index"), int):
                by_ts.append((float(r["ts"]), node, "have", int(r["chunk_index"])))
            elif ev == "completed":
                by_ts.append((float(r["ts"]), node, "complete", None))

    online: set[str] = set()
    have: dict[str, set[int]] = defaultdict(set)
    completed: set[str] = set()
    leech_seen: set[str] = set()
    rows: list[dict[str, Any]] = []
    i = 0
    for b in range(nbuckets):
        cutoff = t0 + (b + 1) * bucket_sec
        while i < len(by_ts) and by_ts[i][0] < cutoff:
            _, node, kind, ci = by_ts[i]
            role = peers[node].role if node in peers else _role_hint(node)
            if kind == "online":
                online.add(node)
                if role == "seed":
                    have[node] = set(range(num_pieces))
                else:
                    leech_seen.add(node)
            elif kind == "offline":
                online.discard(node)
            elif kind == "have" and ci is not None:
                have[node].add(ci)
            elif kind == "complete":
                completed.add(node)
                have[node] = set(range(num_pieces))
            i += 1
        reps = [sum(1 for n in online if c in have[n]) for c in range(num_pieces)]
        mid = t0 + (b + 0.5) * bucket_sec
        up = 1
        for start, end in tracker_windows:
            if start <= mid and (end is None or mid < end):
                up = 0
        row = {
            "rel_sec": round(b * bucket_sec, 3),
            "active_peers": len(online),
            "active_seeds": sum(1 for n in online if (peers[n].role if n in peers else _role_hint(n)) == "seed"),
            "active_leechers": sum(1 for n in online if (peers[n].role if n in peers else _role_hint(n)) == "leech"),
            "download_kbps": round(dl[b] / 1024.0 / bucket_sec, 2),
            "upload_kbps": round(ul[b] / 1024.0 / bucket_sec, 2),
            "tracker_announces": ann[b],
            "tracker_announces_failed": ann_fail[b],
            "tracker_up": up,
            "completed_leechers": len(completed & leech_seen),
            "completion_ratio_pct": round(100.0 * len(completed & leech_seen) / len(leech_seen), 2) if leech_seen else 0.0,
            "avg_chunk_replication": round(sum(reps) / num_pieces, 3) if num_pieces else 0.0,
            "min_chunk_replication": min(reps) if reps else 0,
        }
        for c, v in enumerate(reps):
            row[f"chunk_{c}_replicas"] = v
        rows.append(row)

    # ------------------------------------------------------------------ peer rows
    peer_rows = []
    for node in sorted(peers, key=lambda n: (peers[n].role != "seed", n)):
        p = peers[node]
        done = p.complete_ts is not None
        dl_time = round(p.complete_ts - p.start_ts, 3) if done and p.role == "leech" else None
        active = (p.end_ts or t_end) - p.start_ts
        peer_rows.append(
            {
                "peer_id": node,
                "role": p.role,
                "start_rel_sec": round(p.start_ts - t0, 3),
                "end_rel_sec": round((p.end_ts or t_end) - t0, 3),
                "completed": 1 if (done or p.role == "seed") else 0,
                "download_time_sec": dl_time if dl_time is not None else "",
                "pieces_downloaded": p.pieces_down,
                "bytes_downloaded": p.bytes_down,
                "avg_download_kbps": round(p.bytes_down / 1024.0 / dl_time, 2) if dl_time else (round(p.bytes_down / 1024.0 / active, 2) if active > 0 and p.bytes_down else 0.0),
                "pieces_uploaded": p.pieces_up,
                "bytes_uploaded": p.bytes_up,
                "uploaded_before_complete": p.uploaded_before_complete,
                "distinct_sources": len(p.sources),
                "invalid_chunks": p.invalid,
                "peer_lost": p.lost,
                "switch_source": p.switches,
                "announces": p.announces,
                "announce_failures": p.announce_failures,
                "exit_code": p.exit_code,
                "end_reason": p.end_reason,
            }
        )

    # ------------------------------------------------------------------ summary
    leeches = [p for p in peers.values() if p.role == "leech"]
    seeds = [p for p in peers.values() if p.role == "seed"]
    times = [p.complete_ts - p.start_ts for p in leeches if p.complete_ts is not None]
    total_up = sum(p.bytes_up for p in peers.values())
    leech_up = sum(p.bytes_up for p in leeches)
    recovery = []
    for rts in restart_ts:
        after = [float(r["ts"]) for r in records if r.get("event") == "announce_ok" and str(r.get("peer_id")) not in TRACKER_IDS and float(r["ts"]) >= rts]
        if after:
            recovery.append(after[0] - rts)
    reps_series = [r["avg_chunk_replication"] for r in rows if r["active_peers"] > 0]
    min_after_start = [r["min_chunk_replication"] for r in rows if r["active_peers"] > 0]
    duration = t_end - t0
    summary = {
        "num_pieces": num_pieces,
        "num_seeds": len(seeds),
        "total_leechers": len(leeches),
        "completed_leechers": len(times),
        "completion_ratio_pct": round(100.0 * len(times) / len(leeches), 2) if leeches else 0.0,
        "mean_download_time_sec": round(statistics.mean(times), 3) if times else "",
        "median_download_time_sec": round(statistics.median(times), 3) if times else "",
        "min_download_time_sec": round(min(times), 3) if times else "",
        "max_download_time_sec": round(max(times), 3) if times else "",
        "total_bytes_downloaded": sum(p.bytes_down for p in leeches),
        "total_bytes_uploaded": total_up,
        "leecher_upload_share_pct": round(100.0 * leech_up / total_up, 2) if total_up else 0.0,
        "peak_download_kbps": max((r["download_kbps"] for r in rows), default=0.0),
        "mean_download_kbps": round(sum(dl) / 1024.0 / duration, 2) if duration > 0 else 0.0,
        "avg_chunk_replication_mean": round(statistics.mean(reps_series), 3) if reps_series else 0.0,
        "avg_chunk_replication_final": reps_series[-1] if reps_series else 0.0,
        "min_chunk_replication_observed": min(min_after_start) if min_after_start else 0,
        "total_tracker_announces": sum(ann),
        "total_tracker_announces_failed": sum(ann_fail),
        "peak_tracker_announces_per_bucket": max(ann) if ann else 0,
        "mean_tracker_announces_per_sec": round(sum(ann) / duration, 3) if duration > 0 else 0.0,
        "tracker_downtime_sec": round(sum(((e if e is not None else t_end) - s) for s, e in tracker_windows), 3),
        "tracker_recovery_sec": round(max(recovery), 3) if recovery else "",
        "invalid_chunks": sum(p.invalid for p in peers.values()),
        "peer_lost_events": sum(p.lost for p in peers.values()),
        "switch_source_events": sum(p.switches for p in peers.values()),
        "duration_sec": round(duration, 3),
        "bucket_sec": bucket_sec,
    }
    return {"peers": peer_rows, "timeseries": rows, "summary": summary, "t0": t0, "tracker_windows": tracker_windows}


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        if not rows:
            f.write("")
            return
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def analyze_and_export(scenario_name: str, log_path: str | Path, out_dir: str | Path, bucket_sec: float = 1.0, quiet: bool = False) -> dict[str, Any]:
    """Parse one run and write the three CSV files. Returns the summary dict."""
    out_dir = Path(out_dir)
    res = analyze(parse_logs(log_path), bucket_sec)
    _write_csv(out_dir / f"{scenario_name}_peers.csv", res["peers"])
    _write_csv(out_dir / f"{scenario_name}_timeseries.csv", res["timeseries"])
    summary = {"scenario": scenario_name, **res["summary"]}
    _write_csv(out_dir / f"{scenario_name}_summary.csv", [summary])
    if not quiet:
        s = res["summary"]
        print(f"=== [C4 metrics] {scenario_name}: CSVs written to {out_dir} ===")
        print(f"    completion {s['completion_ratio_pct']}% ({s['completed_leechers']}/{s['total_leechers']}) | "
              f"mean download {s['mean_download_time_sec']}s | announces {s['total_tracker_announces']} | "
              f"avg replication {s['avg_chunk_replication_mean']}")
    return summary


def write_summary_table(summaries: list[dict[str, Any]], path: str | Path) -> None:
    _write_csv(Path(path), summaries)


def main() -> int:
    parser = argparse.ArgumentParser(description="DS02 metrics extractor (C4)")
    parser.add_argument("--scenario", default="baseline", help="scenario name (used for file names)")
    parser.add_argument("--log", default=None, help="events.jsonl (default logs/<scenario>/events.jsonl)")
    parser.add_argument("--out-dir", default=str(REPO_ROOT / "results"))
    parser.add_argument("--bucket-sec", type=float, default=1.0)
    args = parser.parse_args()
    log = Path(args.log) if args.log else REPO_ROOT / "logs" / args.scenario / "events.jsonl"
    summary = analyze_and_export(args.scenario, log, args.out_dir, args.bucket_sec)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
