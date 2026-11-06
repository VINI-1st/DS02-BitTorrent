"""
Benchmark script for the DS02 Tracker Server (Task A7).

Measures:
- Requests per second (RPS) under concurrent load
- Latency percentiles (avg, p50, p95, p99, max)
- Error rate under stress
"""

import argparse
import json
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import urlopen, Request
from urllib.parse import urlencode

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.tracker.server import TrackerServer


def run_single_request(base_url: str, info_hash: str, peer_id: str, port: int) -> tuple[bool, float]:
    params = {
        "info_hash": info_hash,
        "peer_id": peer_id,
        "port": str(port),
        "left": "100000",
    }
    url = f"{base_url}/announce?{urlencode(params)}"
    start = time.perf_counter()
    try:
        req = Request(url)
        with urlopen(req, timeout=5.0) as resp:
            data = resp.read()
            json.loads(data)
            duration = time.perf_counter() - start
            return True, duration
    except Exception:
        duration = time.perf_counter() - start
        return False, duration


def benchmark(host: str = "127.0.0.1", port: int = 8888, total_requests: int = 1000, concurrency: int = 20):
    server = TrackerServer(host=host, port=port, interval=15, ttl=45)
    server.start()
    time.sleep(0.5)

    base_url = f"http://{host}:{port}"
    info_hash = "a" * 40

    print(f"=== Starting Tracker Benchmark ===")
    print(f"Target: {base_url}")
    print(f"Total Requests: {total_requests}")
    print(f"Concurrency: {concurrency}")

    latencies = []
    successes = 0
    failures = 0

    start_total = time.perf_counter()

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(run_single_request, base_url, info_hash, f"bench-peer-{i:05d}", 9000 + (i % 1000))
            for i in range(total_requests)
        ]
        for f in as_completed(futures):
            ok, dur = f.result()
            latencies.append(dur * 1000)  # ms
            if ok:
                successes += 1
            else:
                failures += 1

    total_time = time.perf_counter() - start_total
    rps = total_requests / total_time

    server.stop()

    latencies.sort()
    avg_lat = statistics.mean(latencies)
    p50 = statistics.median(latencies)
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]
    max_lat = max(latencies)

    print("\n=== Benchmark Results ===")
    print(f"Total time:       {total_time:.2f} s")
    print(f"Throughput:       {rps:.1f} req/s")
    print(f"Success / Fail:   {successes} / {failures}")
    print(f"Average latency:  {avg_lat:.2f} ms")
    print(f"Median (p50):     {p50:.2f} ms")
    print(f"p95 latency:      {p95:.2f} ms")
    print(f"p99 latency:      {p99:.2f} ms")
    print(f"Max latency:      {max_lat:.2f} ms")

    return {
        "total_requests": total_requests,
        "concurrency": concurrency,
        "total_time_s": round(total_time, 2),
        "rps": round(rps, 1),
        "successes": successes,
        "failures": failures,
        "avg_ms": round(avg_lat, 2),
        "p50_ms": round(p50, 2),
        "p95_ms": round(p95, 2),
        "p99_ms": round(p99, 2),
        "max_ms": round(max_lat, 2),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tracker benchmark")
    parser.add_argument("--requests", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=20)
    args = parser.parse_args()
    benchmark(total_requests=args.requests, concurrency=args.concurrency)
