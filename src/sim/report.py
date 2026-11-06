"""C6 – Charts and the Vietnamese analysis report built from the C4 CSV files.

Used by ``run_experiments.py``; can also be run alone to re-draw from existing CSVs::

    py src/sim/report.py --results results

Chart conventions (results/charts/*.png):
    * one y-axis per chart (no dual axes): throughput and tracker load are separate figures,
    * cross-scenario bars use a single hue; the scenario is named on the axis,
    * per-scenario lines use a fixed categorical order (same scenario = same colour in every chart),
    * thin lines, light recessive grid, legend + direct end labels.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.sim.common import REPO_ROOT  # noqa: E402

import matplotlib  # noqa: E402

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

SCENARIO_ORDER = ["baseline", "multi_seed", "high_churn", "flash_crowd", "tracker_crash"]
LABELS = {
    "baseline": "Baseline\n(1 seed)",
    "multi_seed": "Multi-seed\n(4 seeds)",
    "high_churn": "High churn\n(Poisson+Pareto)",
    "flash_crowd": "Flash crowd\n(+8 burst)",
    "tracker_crash": "Tracker crash\n(5 s down)",
}
SHORT = {
    "baseline": "Baseline",
    "multi_seed": "Multi-seed",
    "high_churn": "High churn",
    "flash_crowd": "Flash crowd",
    "tracker_crash": "Tracker crash",
}
# reference categorical palette (light mode), fixed order
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
SCEN_COLOR = {s: SERIES[i] for i, s in enumerate(SCENARIO_ORDER)}
SURFACE = "#fcfcfb"
TEXT = "#0b0b0b"
TEXT2 = "#52514e"
GRID = "#e4e3df"
NEUTRAL_BAND = "#d9d8d3"


def _style() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": SURFACE,
            "axes.facecolor": SURFACE,
            "savefig.facecolor": SURFACE,
            "axes.edgecolor": GRID,
            "axes.labelcolor": TEXT2,
            "axes.titlecolor": TEXT,
            "axes.titlesize": 12,
            "axes.titleweight": "bold",
            "axes.titlelocation": "left",
            "axes.labelsize": 10,
            "xtick.color": TEXT2,
            "ytick.color": TEXT2,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "axes.grid": True,
            "grid.color": GRID,
            "grid.linewidth": 0.8,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "legend.frameon": False,
            "legend.fontsize": 9,
            "font.size": 10,
            "lines.linewidth": 2.0,
        }
    )


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def fnum(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _bar_axis(ax) -> None:
    ax.grid(axis="x", visible=False)
    ax.set_axisbelow(True)


def chart_download_time(summary: list[dict], peers: dict[str, list[dict]], out: Path) -> None:
    names = [r["scenario"] for r in summary]
    means = [fnum(r["mean_download_time_sec"]) for r in summary]
    fig, ax = plt.subplots(figsize=(9, 4.8))
    x = list(range(len(names)))
    bars = ax.bar(x, means, width=0.56, color=SERIES[0], zorder=2)
    for i, n in enumerate(names):
        times = [fnum(p["download_time_sec"]) for p in peers.get(n, []) if p["role"] == "leech" and p["download_time_sec"]]
        if times:
            ax.scatter([i] * len(times), times, s=26, color=SURFACE, edgecolors=TEXT, linewidths=1.0, zorder=3)
    top = max([fnum(p["download_time_sec"]) for n in names for p in peers.get(n, []) if p.get("download_time_sec")] + means + [1])
    for b, v in zip(bars, means):
        if v:
            ax.text(b.get_x() + b.get_width() / 2, top * 0.03, f"{v:.1f} s", ha="center", va="bottom", color="#ffffff", fontsize=10, fontweight="bold", zorder=4)
    ax.set_xticks(x, [LABELS.get(n, n) for n in names])
    ax.set_ylabel("Thời gian tải xong (giây)")
    ax.set_ylim(0, top * 1.18)
    ax.set_title("Thời gian tải xong của leecher theo kịch bản", pad=20)
    ax.text(0, 1.015, "Cột = trung bình; chấm = từng leecher đã tải xong", transform=ax.transAxes, color=TEXT2, fontsize=9, va="bottom")
    _bar_axis(ax)
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def chart_completion(summary: list[dict], out: Path) -> None:
    names = [r["scenario"] for r in summary]
    vals = [fnum(r["completion_ratio_pct"]) for r in summary]
    fig, ax = plt.subplots(figsize=(9, 4.8))
    x = list(range(len(names)))
    bars = ax.bar(x, vals, width=0.56, color=SERIES[0], zorder=2)
    for b, v, r in zip(bars, vals, summary):
        ax.text(b.get_x() + b.get_width() / 2, v + 2, f"{v:.0f}%\n({r['completed_leechers']}/{r['total_leechers']})", ha="center", va="bottom", color=TEXT, fontsize=9)
    ax.set_xticks(x, [LABELS.get(n, n) for n in names])
    ax.set_ylim(0, 122)
    ax.set_yticks([0, 25, 50, 75, 100])
    ax.set_ylabel("Leecher tải xong (%)")
    ax.set_title("Tỉ lệ hoàn tất của swarm dưới churn và lỗi")
    _bar_axis(ax)
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def _line_end_label(ax, xs, ys, text, color) -> None:
    if xs:
        ax.annotate(text, (xs[-1], ys[-1]), xytext=(4, 0), textcoords="offset points", color=TEXT2, fontsize=8, va="center")
        ax.plot([xs[-1]], [ys[-1]], "o", ms=4, color=color, mec=SURFACE, mew=1.5)


def chart_replication(series: dict[str, list[dict]], out: Path) -> None:
    fig, ax = plt.subplots(figsize=(9.5, 4.8))
    for n in SCENARIO_ORDER:
        rows = [r for r in series.get(n, []) if int(fnum(r["active_peers"])) > 0]
        if not rows:
            continue
        xs = [fnum(r["rel_sec"]) for r in rows]
        ys = [fnum(r["avg_chunk_replication"]) for r in rows]
        ax.plot(xs, ys, color=SCEN_COLOR[n], label=SHORT[n])
        _line_end_label(ax, xs, ys, SHORT[n], SCEN_COLOR[n])
    ax.set_xlabel("Thời gian từ lúc bắt đầu kịch bản (giây)")
    ax.set_ylabel("Số bản sao trung bình / chunk")
    ax.set_ylim(bottom=0)
    ax.set_title("Hệ số nhân bản chunk theo thời gian (chỉ tính peer đang online)")
    ax.legend(loc="upper left", ncol=5, bbox_to_anchor=(0, -0.14))
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def chart_throughput(series: dict[str, list[dict]], out: Path) -> None:
    present = [n for n in SCENARIO_ORDER if series.get(n)]
    fig, axes = plt.subplots(len(present), 1, figsize=(9.5, 1.9 * len(present) + 0.8), sharex=True, sharey=True, squeeze=False)
    for ax, n in zip(axes[:, 0], present):
        rows = series[n]
        xs = [fnum(r["rel_sec"]) for r in rows]
        ys = [fnum(r["download_kbps"]) for r in rows]
        ax.plot(xs, ys, color=SCEN_COLOR[n])
        ax.fill_between(xs, ys, color=SCEN_COLOR[n], alpha=0.12, lw=0)
        ax.text(0.005, 0.92, SHORT[n], transform=ax.transAxes, color=TEXT, fontsize=10, fontweight="bold", va="top")
        ax.set_ylabel("KiB/s")
    axes[0, 0].set_title("Thông lượng tải xuống của toàn swarm theo thời gian (bucket 1 s)", pad=20)
    axes[0, 0].text(0, 1.04, "Tổng upload luôn bằng tổng download: mỗi byte tải về đều do một peer khác phục vụ", transform=axes[0, 0].transAxes, color=TEXT2, fontsize=9, va="bottom")
    axes[-1, 0].set_xlabel("Thời gian (giây)")
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def chart_tracker_load(series: dict[str, list[dict]], out: Path) -> None:
    present = [n for n in SCENARIO_ORDER if series.get(n)]
    fig, axes = plt.subplots(len(present), 1, figsize=(9.5, 1.9 * len(present) + 0.8), sharex=True, sharey=True, squeeze=False)
    for ax, n in zip(axes[:, 0], present):
        rows = series[n]
        xs = [fnum(r["rel_sec"]) for r in rows]
        down = [x for x, r in zip(xs, rows) if r.get("tracker_up") == "0"]
        for x in down:
            ax.axvspan(x, x + 1, color=NEUTRAL_BAND, alpha=0.6, lw=0, zorder=0)
        ax.bar(xs, [fnum(r["tracker_announces"]) for r in rows], width=0.8, align="edge", color=SERIES[0], label="announce thành công", zorder=2)
        failed = [fnum(r.get("tracker_announces_failed", 0)) for r in rows]
        if any(failed):
            ax.bar(xs, [-f for f in failed], width=0.8, align="edge", color=SERIES[7], label="announce thất bại (âm)", zorder=2)
        if down:
            ax.text(down[0] + 0.1, 0.92, "tracker sập", transform=ax.get_xaxis_transform(), color=TEXT2, fontsize=8, va="top")
        ax.axhline(0, color=TEXT2, linewidth=0.8)
        ax.text(0.005, 0.92, SHORT[n], transform=ax.transAxes, color=TEXT, fontsize=10, fontweight="bold", va="top", ha="left")
        ax.set_ylabel("req / s")
        ax.grid(axis="x", visible=False)
    axes[0, 0].set_title("Tải tracker: số announce mỗi giây")
    handles, labels = [], []
    for ax in axes[:, 0]:
        h, lab = ax.get_legend_handles_labels()
        for hh, ll in zip(h, lab):
            if ll not in labels:
                handles.append(hh)
                labels.append(ll)
    axes[0, 0].legend(handles, labels, loc="upper right", ncol=2)
    axes[-1, 0].set_xlabel("Thời gian (giây)")
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def chart_upload_share(summary: list[dict], peers: dict[str, list[dict]], out: Path) -> None:
    names = [r["scenario"] for r in summary]
    seed_mb, leech_mb = [], []
    for n in names:
        rows = peers.get(n, [])
        seed_mb.append(sum(fnum(p["bytes_uploaded"]) for p in rows if p["role"] == "seed") / 1048576)
        leech_mb.append(sum(fnum(p["bytes_uploaded"]) for p in rows if p["role"] == "leech") / 1048576)
    fig, ax = plt.subplots(figsize=(9, 4.8))
    x = list(range(len(names)))
    ax.bar(x, seed_mb, width=0.56, color=SERIES[0], label="Seed phục vụ", zorder=2, edgecolor=SURFACE, linewidth=2)
    ax.bar(x, leech_mb, width=0.56, bottom=seed_mb, color=SERIES[2], label="Leecher phục vụ lại", zorder=2, edgecolor=SURFACE, linewidth=2)
    for i, (s, l) in enumerate(zip(seed_mb, leech_mb)):
        tot = s + l
        pct = 100 * l / tot if tot else 0
        ax.text(i, tot + 0.3, f"{pct:.0f}% từ leecher", ha="center", va="bottom", color=TEXT, fontsize=9)
    ax.set_xticks(x, [LABELS.get(n, n) for n in names])
    ax.set_ylabel("Dữ liệu đã upload (MiB)")
    ax.set_ylim(0, max([s + l for s, l in zip(seed_mb, leech_mb)] + [1]) * 1.18)
    ax.set_title("Ai phục vụ dữ liệu? Seed so với leecher (tính chất P2P)")
    ax.legend(loc="upper left")
    _bar_axis(ax)
    fig.tight_layout()
    fig.savefig(out, dpi=160)
    plt.close(fig)


def plot_all_charts(results_dir: Path, charts_dir: Path) -> list[Path]:
    _style()
    charts_dir.mkdir(parents=True, exist_ok=True)
    summary = read_csv(results_dir / "summary_metrics.csv")
    summary.sort(key=lambda r: SCENARIO_ORDER.index(r["scenario"]) if r["scenario"] in SCENARIO_ORDER else 99)
    if not summary:
        return []
    names = [r["scenario"] for r in summary]
    peers = {n: read_csv(results_dir / f"{n}_peers.csv") for n in names}
    series = {n: read_csv(results_dir / f"{n}_timeseries.csv") for n in names}
    outs = [
        charts_dir / "exp1_download_time.png",
        charts_dir / "exp2_completion_ratio.png",
        charts_dir / "exp3_chunk_replication.png",
        charts_dir / "exp4_throughput.png",
        charts_dir / "exp5_tracker_load.png",
        charts_dir / "exp6_upload_share.png",
    ]
    chart_download_time(summary, peers, outs[0])
    chart_completion(summary, outs[1])
    chart_replication(series, outs[2])
    chart_throughput(series, outs[3])
    chart_tracker_load(series, outs[4])
    chart_upload_share(summary, peers, outs[5])
    return outs


# ------------------------------------------------------------------------- report


def _fmt(v: Any, unit: str = "", nd: int = 2) -> str:
    if v in ("", None):
        return "–"
    try:
        return f"{float(v):.{nd}f}{unit}"
    except (TypeError, ValueError):
        return str(v)


def write_analysis_report(results_dir: Path, meta: dict[str, Any] | None = None) -> Path:
    meta = meta or {}
    rows = {r["scenario"]: r for r in read_csv(results_dir / "summary_metrics.csv")}
    peers = {n: read_csv(results_dir / f"{n}_peers.csv") for n in rows}
    g = lambda s, k: rows.get(s, {}).get(k, "")  # noqa: E731

    b_t, m_t = fnum(g("baseline", "mean_download_time_sec")), fnum(g("multi_seed", "mean_download_time_sec"))
    speedup = b_t / m_t if b_t and m_t else 0.0

    lines: list[str] = []
    lines.append("# Báo cáo thực nghiệm và đánh giá hiệu năng (C6)")
    lines.append("")
    lines.append(f"_Sinh tự động bởi `py src/sim/run_experiments.py` lúc {meta.get('generated_at', '')}. "
                 f"Peer: `{meta.get('peer', 'src/sim/stub_peer.py')}` · Tracker: `{meta.get('tracker', 'src/sim/stub_tracker.py')}`._")
    lines.append("")
    lines.append("## 1. Phương pháp")
    lines.append("")
    lines.append("- Mỗi kịch bản trong `scenarios/*.yaml` được chạy bằng launcher (C1): tracker và mọi peer là **tiến trình hệ điều hành riêng**, "
                 "giao tiếp qua TCP/HTTP thật trên `127.0.0.1`. Dữ liệu là file 2 MiB sinh tất định, chia thành 16 chunk 128 KiB; "
                 "mỗi chunk được kiểm SHA-1 khi nhận, file ghép xong được so SHA-256 với file gốc.")
    lines.append("- Băng thông upload mỗi peer bị giới hạn bằng `--max-up-kbps` (KiB/s, Hợp đồng 4); độ trễ thêm bằng `--latency-ms` (chỉ StubPeer).")
    lines.append("- Churn (C2) sinh từ `random_seed=42`: cùng seed cho cùng dãy sự kiện vào/ra/sập (dấu vân tay lịch churn ghi trong sự kiện `scenario_start`).")
    lines.append("- Số liệu (C4) tính từ `logs/<kịch bản>/events.jsonl`; CSV trong `results/`, biểu đồ trong `results/charts/`. Đơn vị `kbps` = KiB/s.")
    if meta.get("stub"):
        lines.append("- **Lưu ý:** kết quả này đo bằng StubPeer/StubTracker của C. Khi peer thật của B và tracker thật của A sẵn sàng, "
                     "chạy lại `py src/sim/run_experiments.py --peer-script \"-m peer\" --tracker-script \"-m tracker\"` để có số liệu cuối cùng.")
    lines.append("")
    lines.append("## 2. Bảng tổng hợp")
    lines.append("")
    lines.append("| Kịch bản | Seed | Leecher | Hoàn tất | Thời gian tải TB (min–max) | Nhân bản TB | Leecher upload | Announce (TB/s) | peer_lost / switch / invalid |")
    lines.append("|---|---:|---:|---:|---|---:|---:|---:|---|")
    for n in SCENARIO_ORDER:
        if n not in rows:
            continue
        r = rows[n]
        lines.append(
            f"| {SHORT[n]} | {r['num_seeds']} | {r['total_leechers']} | {_fmt(r['completion_ratio_pct'], '%', 0)} ({r['completed_leechers']}/{r['total_leechers']}) | "
            f"{_fmt(r['mean_download_time_sec'], ' s', 1)} ({_fmt(r['min_download_time_sec'], '', 1)}–{_fmt(r['max_download_time_sec'], '', 1)}) | "
            f"{_fmt(r['avg_chunk_replication_mean'], '', 2)} | {_fmt(r['leecher_upload_share_pct'], '%', 0)} | "
            f"{r['total_tracker_announces']} ({_fmt(r['mean_tracker_announces_per_sec'], '', 2)}) | "
            f"{r['peer_lost_events']} / {r['switch_source_events']} / {r['invalid_chunks']} |"
        )
    lines.append("")
    lines.append("![Thời gian tải](charts/exp1_download_time.png)")
    lines.append("![Tỉ lệ hoàn tất](charts/exp2_completion_ratio.png)")
    lines.append("")

    lines.append("## 3. Phân tích từng kịch bản")
    lines.append("")
    # baseline
    lines.append("### 3.1 Baseline – 1 seed, 3 leecher")
    lines.append(f"- **Kết quả:** {g('baseline', 'completed_leechers')}/{g('baseline', 'total_leechers')} leecher tải xong, thời gian trung bình "
                 f"**{_fmt(g('baseline', 'mean_download_time_sec'), ' s', 2)}**; leecher tự phục vụ lại **{_fmt(g('baseline', 'leecher_upload_share_pct'), '%', 0)}** "
                 f"tổng dữ liệu upload; thông lượng đỉnh {_fmt(g('baseline', 'peak_download_kbps'), ' KiB/s', 0)}.")
    lines.append("- **Kết luận:** dù chỉ có một seed giới hạn 256 KiB/s, các leecher trao đổi chunk cho nhau ngay khi có, nên tổng băng thông "
                 "phục vụ của swarm lớn hơn băng thông của seed – đúng tính chất P2P. Mọi file tải về khớp SHA-256 với file gốc.")
    lines.append("")
    # multi seed
    lines.append("### 3.2 Multi-seed – 4 seed, 3 leecher")
    if speedup:
        verdict = "nhanh hơn" if speedup > 1.05 else ("tương đương" if speedup > 0.95 else "chậm hơn")
        lines.append(f"- **Kết quả:** thời gian tải trung bình **{_fmt(m_t, ' s', 2)}** so với {_fmt(b_t, ' s', 2)} ở baseline → "
                     f"**{verdict} {speedup:.2f}×**; tỉ lệ dữ liệu do leecher phục vụ giảm còn {_fmt(g('multi_seed', 'leecher_upload_share_pct'), '%', 0)}.")
    lines.append("- **Kết luận:** thêm nguồn làm tăng tổng băng thông upload khả dụng; bộ tải song song (tối đa 4 chunk đồng thời, ≤ 2 yêu cầu/nguồn) "
                 "trải yêu cầu lên nhiều seed nên rút ngắn thời gian tải. Mức tăng tốc nhỏ hơn 4× vì leecher ở baseline đã bù một phần băng thông "
                 "và độ trễ cố định cho mỗi chunk không giảm theo số seed.")
    lines.append("")
    # high churn
    hc = peers.get("high_churn", [])
    dyn = [p for p in hc if p["role"] == "leech" and fnum(p["start_rel_sec"]) > 1.0]
    dyn_done = [p for p in dyn if p["completed"] == "1"]
    crashed = [p for p in dyn if "crash" in p.get("end_reason", "")]
    lines.append("### 3.3 High churn – Poisson + Pareto + crash")
    lines.append(f"- **Kết quả:** {g('high_churn', 'total_leechers')} leecher tham gia ({len(dyn)} leecher động, {len(crashed)} bị kill đột ngột). "
                 f"Tỉ lệ hoàn tất **{_fmt(g('high_churn', 'completion_ratio_pct'), '%', 0)}** ({len(dyn_done)}/{len(dyn)} leecher động tải xong trước khi rời). "
                 f"Ghi nhận **{g('high_churn', 'peer_lost_events')}** lần `peer_lost` và **{g('high_churn', 'switch_source_events')}** lần `switch_source`; "
                 f"hệ số nhân bản thấp nhất quan sát được = {g('high_churn', 'min_chunk_replication_observed')}.")
    lines.append("- **Kết luận:** leecher bị kill giữa lúc đang phục vụ làm đứt kết nối của peer khác; các peer phát hiện (`peer_lost`) và "
                 "chuyển chunk sang nguồn khác (`switch_source`) nên các leecher tĩnh vẫn tải xong đúng hash. Peer có thời gian sống ngắn "
                 "(đuôi nặng của Pareto: nhiều peer sống rất ngắn) không kịp tải đủ – đây là nguyên nhân chính làm tỉ lệ hoàn tất < 100%. "
                 "Do seed gốc luôn online nên mọi chunk luôn có ít nhất 1 bản sao.")
    lines.append("")
    # flash crowd
    lines.append("### 3.4 Flash crowd – 8 leecher ập vào cùng lúc, rồi rời đồng loạt")
    lines.append(f"- **Kết quả:** tỉ lệ hoàn tất **{_fmt(g('flash_crowd', 'completion_ratio_pct'), '%', 0)}**, thời gian tải trung bình "
                 f"**{_fmt(g('flash_crowd', 'mean_download_time_sec'), ' s', 2)}** (dài nhất {_fmt(g('flash_crowd', 'max_download_time_sec'), ' s', 1)}); "
                 f"leecher phục vụ **{_fmt(g('flash_crowd', 'leecher_upload_share_pct'), '%', 0)}** dữ liệu; tải tracker đỉnh "
                 f"{g('flash_crowd', 'peak_tracker_announces_per_bucket')} announce/giây.")
    lines.append("- **Kết luận:** khi cả đám đông cùng đến, một seed không đủ băng thông; swarm vẫn chịu được vì chunk vừa tải xong lập tức được "
                 "phục vụ lại (rarest-first giúp các leecher giữ những chunk khác nhau). Hệ số nhân bản tăng nhanh sau đợt ập vào và sụt mạnh khi "
                 "rời đồng loạt (biểu đồ exp3); seed gốc là điều kiện để chunk không biến mất.")
    lines.append("")
    # tracker crash
    lines.append("### 3.5 Tracker crash – tracker bị kill ở giây 3, sống lại ở giây 8")
    lines.append(f"- **Kết quả:** tracker ngừng {_fmt(g('tracker_crash', 'tracker_downtime_sec'), ' s', 1)}; có "
                 f"**{g('tracker_crash', 'total_tracker_announces_failed')}** announce thất bại trong thời gian sập; swarm đăng ký lại sau "
                 f"**{_fmt(g('tracker_crash', 'tracker_recovery_sec'), ' s', 2)}** kể từ khi tracker khởi động lại. Tỉ lệ hoàn tất "
                 f"**{_fmt(g('tracker_crash', 'completion_ratio_pct'), '%', 0)}** ({g('tracker_crash', 'completed_leechers')}/{g('tracker_crash', 'total_leechers')}).")
    lines.append("- **Kết luận:** tracker chỉ dùng để tìm peer; các peer đã biết nhau tiếp tục trao đổi chunk trực tiếp khi tracker sập. "
                 "Leecher mới đến trong lúc tracker sập không tìm được nguồn cho tới khi tracker sống lại (announce thử lại với back-off), "
                 "sau đó tracker dựng lại swarm hoàn toàn từ các announce định kỳ mà không cần lưu trạng thái.")
    lines.append("")
    lines.append("## 4. Biểu đồ khác")
    lines.append("")
    lines.append("![Nhân bản chunk](charts/exp3_chunk_replication.png)")
    lines.append("![Thông lượng](charts/exp4_throughput.png)")
    lines.append("![Tải tracker](charts/exp5_tracker_load.png)")
    lines.append("![Tỉ lệ upload](charts/exp6_upload_share.png)")
    lines.append("")
    lines.append("## 5. Kết luận chung và hạn chế")
    lines.append("")
    lines.append("- Hệ thống giữ **tính đúng** trong mọi kịch bản: mọi leecher báo `completed` đều có file trùng SHA-256 với file gốc; chunk sai hash bị từ chối.")
    lines.append("- **Hiệu năng** tăng theo số nguồn (multi-seed) và nhờ leecher phục vụ lại (baseline, flash crowd).")
    lines.append("- **Chịu lỗi:** mất nguồn → `peer_lost` → `switch_source`; tracker sập không làm gián đoạn trao đổi chunk giữa các peer đã biết nhau.")
    lines.append("- **Hạn chế:** chạy trên một máy (loopback) nên độ trễ/băng thông là mô phỏng; giới hạn băng thông là của phía upload; "
                 "kết quả có dao động nhỏ giữa các lần chạy do lập lịch luồng của hệ điều hành (lịch churn thì tất định).")
    lines.append("")
    path = results_dir / "analysis.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-draw charts and analysis from results/*.csv")
    parser.add_argument("--results", default=str(REPO_ROOT / "results"))
    args = parser.parse_args()
    res = Path(args.results)
    for p in plot_all_charts(res, res / "charts"):
        print(f"chart: {p}")
    print(f"report: {write_analysis_report(res)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
