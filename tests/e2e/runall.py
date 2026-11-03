import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.sim.launcher import run_scenario
from src.sim.metrics import analyze_and_export, parse_Logs


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def verify_scenario_e2e(scenario_yaml: str, peer_script: str, min_completed: int = 1) -> tuple[bool, str]:
    scen_path = Path(scenario_yaml)
    scen_name = scen_path.stem

    # 1. Chạy kịch bản với cờ keep_dirs=True để giữ lại file tải về cho bước kiểm tra mã băm
    run_scenario(str(scen_path), peer_script, keep_dirs=True)

    log_path = Path("logs") / scen_name / "events.jsonl"
    work_dir = Path("sim_workspace") / scen_name

    try:
        records = parse_Logs(log_path)
    except Exception as e:
        return False, f"Failed to read log file: {e}"

    if not records:
        return False, "No log events emitted during scenario execution."

    # 2. Kiểm tra sự kiện started và completed trong log
    started_peers = [r["peer_id"] for r in records if r.get("event") == "started"]
    completed_peers = [r["peer_id"] for r in records if r.get("event") == "completed"]

    if len(completed_peers) < min_completed:
        return (
            False,
            f"Expected at least {min_completed} completed leecher(s), got {len(completed_peers)}. Started: {started_peers}",
        )

    # 3. Tạo file chuẩn (reference file) và so sánh mã băm SHA-256 với mọi file tải xong
    torrent_meta_path = work_dir / "sample.torrent.json"
    meta = json.loads(torrent_meta_path.read_text(encoding="utf-8"))
    num_chunks = (meta["file_size_bytes"] + meta["piece_length"] - 1) // meta["piece_length"]
    expected_bytes = b"0" * (num_chunks * meta["piece_length"])
    expected_hash = hashlib.sha256(expected_bytes).hexdigest()

    verified_files = 0
    for pid in completed_peers:
        port = pid.split("-")[-1]
        dl_file = work_dir / f"leech_{port}" / "downloaded_file.bin"
        if not dl_file.exists():
            return False, f"Completed peer {pid} is missing downloaded file at {dl_file}"
        actual_hash = sha256_file(dl_file)
        if actual_hash != expected_hash:
            return (
                False,
                f"Hash mismatch for {pid}: expected {expected_hash[:12]}..., got {actual_hash[:12]}...",
            )
        verified_files += 1

    # 4. Xuất số liệu CSV (C4) và dọn dẹp thư mục tạm
    analyze_and_export(scen_name, log_path, Path("results"))
    if work_dir.exists():
        shutil.rmtree(work_dir)

    return (
        True,
        f"Verified {verified_files} downloaded file(s) match SHA-256 ({expected_hash[:12]}...)",
    )


def main():
    parser = argparse.ArgumentParser(description="DS02 End-to-End Regression Test Suite (C5)")
    parser.add_argument("--peer-script", default="src/sim/stub_peer.py")
    args = parser.parse_args()

    # Xóa file tổng hợp cũ trước khi chạy toàn bộ suite
    summary_csv = Path("results") / "summary_metrics.csv"
    if summary_csv.exists():
        summary_csv.unlink()

    test_matrix = [
        ("scenarios/baseline.yaml", 3),
        ("scenarios/high_churn.yaml", 1),
        ("scenarios/flash_crowd.yaml", 1),
    ]

    print("==========================================================")
    print("       DS02 END-TO-END REGRESSION TEST SUITE (C5)         ")
    print("==========================================================")

    all_passed = True
    for yaml_path, min_comp in test_matrix:
        ok, msg = verify_scenario_e2e(yaml_path, args.peer_script, min_comp)
        status = "PASS" if ok else "FAIL"
        print(f"\n[{status}] Scenario: {yaml_path} -> {msg}\n")
        if not ok:
            all_passed = False

    print("==========================================================")
    if all_passed:
        print("RESULT: ALL E2E SCENARIOS PASSED SUCCESSFULLY!")
        sys.exit(0)
    else:
        print("RESULT: SOME E2E SCENARIOS FAILED. CHECK LOGS ABOVE.")
        sys.exit(1)


if __name__ == "__main__":
    main()