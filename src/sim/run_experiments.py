"""C6 – Run the 5 standard experiments, export CSVs, draw charts, write the analysis.

    py src/sim/run_experiments.py                       # stub peer + stub tracker
    py src/sim/run_experiments.py --only baseline multi_seed
    py src/sim/run_experiments.py --peer-script "-m peer" --tracker-script "-m tracker"

Outputs:
    logs/<scenario>/events.jsonl, logs/<scenario>/processes.json
    results/<scenario>_{peers,timeseries,summary}.csv, results/summary_metrics.csv
    results/charts/exp1..exp6 *.png
    results/analysis.md  (Vietnamese report with concrete numbers)
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.sim.common import REPO_ROOT, configure_utf8_stdio  # noqa: E402
from src.sim.launcher import DEFAULT_PEER, DEFAULT_TRACKER, LaunchOptions, ScenarioRunner  # noqa: E402
from src.sim.metrics import analyze_and_export, write_summary_table  # noqa: E402
from src.sim.report import SCENARIO_ORDER, plot_all_charts, write_analysis_report  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    configure_utf8_stdio()
    p = argparse.ArgumentParser(description="DS02 experiment suite (C6)")
    p.add_argument("--only", nargs="*", default=None, help="subset of scenario names")
    p.add_argument("--peer-script", default=DEFAULT_PEER)
    p.add_argument("--tracker-script", default=DEFAULT_TRACKER)
    p.add_argument("--tracker-args", default="", help='extra tracker args, e.g. "--interval 2 --ttl 6"')
    p.add_argument("--make-torrent-script", default=None)
    p.add_argument("--results", default=str(REPO_ROOT / "results"))
    p.add_argument("--verbose", action="store_true", help="echo every event")
    args = p.parse_args(argv)

    names = args.only or SCENARIO_ORDER
    results = Path(args.results)
    results.mkdir(parents=True, exist_ok=True)
    summaries = []
    t_all = time.time()
    for name in names:
        yaml_path = REPO_ROOT / "scenarios" / f"{name}.yaml"
        print(f"\n>>> [C6] running scenario '{name}' ...", flush=True)
        opts = LaunchOptions(
            peer=args.peer_script,
            tracker=args.tracker_script,
            tracker_extra_args=args.tracker_args,
            make_torrent=args.make_torrent_script,
            echo=args.verbose,
        )
        res = ScenarioRunner(yaml_path, opts).run()
        print(f"    finished in {res.duration_sec:.1f}s, {len(res.completed_nodes)} peer(s) completed", flush=True)
        summaries.append(analyze_and_export(name, res.log_path, results))

    # merge with previous rows for scenarios not re-run this time
    from src.sim.report import read_csv

    merged = {r["scenario"]: r for r in read_csv(results / "summary_metrics.csv")}
    for s in summaries:
        merged[s["scenario"]] = s
    ordered = [merged[n] for n in SCENARIO_ORDER if n in merged] + [v for k, v in merged.items() if k not in SCENARIO_ORDER]
    write_summary_table(ordered, results / "summary_metrics.csv")

    charts = plot_all_charts(results, results / "charts")
    report = write_analysis_report(
        results,
        {
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "peer": args.peer_script,
            "tracker": args.tracker_script,
            "stub": "stub_peer" in args.peer_script,
        },
    )
    print(f"\n=== [C6] {len(names)} scenario(s) in {time.time() - t_all:.1f}s ===")
    for c in charts:
        print(f"    chart : {c.relative_to(REPO_ROOT)}")
    print(f"    report: {report.relative_to(REPO_ROOT)}")
    print(f"    table : {(results / 'summary_metrics.csv').relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
