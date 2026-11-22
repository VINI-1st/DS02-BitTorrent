# DS02 – Hệ thống chia sẻ file P2P mô phỏng BitTorrent (Python)

Đồ án môn **Hệ thống phân tán** – đề tài DS02. Một file được chia thành các chunk có mã SHA-1; **tracker** giúp các peer tìm nhau;
các **peer** vừa tải vừa phục vụ chunk cho nhau (tải song song từ nhiều nguồn); bộ **mô phỏng** bật hàng chục tiến trình, gây
churn (peer vào/ra/sập), làm sập tracker, đo đạc và vẽ biểu đồ.

> A P2P file-sharing system in the style of BitTorrent, written in Python, plus a churn simulator, an automated test suite,
> an experiment pipeline and a live terminal demo. Quick start: `pip install -r requirements.txt` then `py demo/run_demo.py`.

---

## 1. Kiến trúc

```mermaid
flowchart TD
    T["Tracker (A)<br/>announce / scrape, TTL, danh sách peer"]
    P1["Peer seed (B)<br/>chunk, upload"]
    P2["Peer leecher (B)<br/>tải song song, rarest-first"]
    P3["Peer leecher (B)<br/>vừa tải vừa phục vụ"]
    S["Simulator (C)<br/>launcher · churn · netem · metrics · demo"]

    T -- "HTTP announce (Hợp đồng 2)" --> P1
    T -- "HTTP announce" --> P2
    T -- "HTTP announce" --> P3
    P1 <-- "wire protocol TCP (Hợp đồng 3)" --> P2
    P2 <-- "wire protocol TCP" --> P3
    P1 <-- "wire protocol TCP" --> P3
    S -- "khởi chạy / kill (Hợp đồng 4 CLI)" --> P1
    S -- "khởi chạy / kill" --> P2
    S -- "khởi chạy / kill" --> P3
    S -- "kill / bật lại" --> T
    P1 -. "log JSONL (Hợp đồng 5)" .-> S
    P2 -. "log JSONL" .-> S
    T -. "log JSONL" .-> S
```

Năm hợp đồng giao tiếp (chi tiết trong `docs/`):

| # | Hợp đồng | Người viết | Tài liệu |
| --- | --- | --- | --- |
| 1 | Metainfo `*.torrent.json` (`info.name/length/piece_length/pieces`, `info_hash`, `announce`) | B | `docs/contracts/metainfo-and-peer-cli.md` |
| 2 | API tracker `announce(info_hash, peer_id, ip, port, event, left)` → `{interval, peers[]}`, `scrape` | A | `docs/protocol.md` (A7) |
| 3 | Wire protocol: `handshake, bitfield, have, interested, choke/unchoke, request, piece, keepalive` (4 byte độ dài + 1 byte loại) | A | `docs/protocol.md` (A7) |
| 4 | CLI peer `--torrent --port --dir --role seed/leech --max-up-kbps`, mã thoát | B | `docs/contracts/metainfo-and-peer-cli.md` |
| 5 | Log sự kiện JSON Lines + định dạng kịch bản YAML | C | `docs/log-and-scenario-spec.md` |

## 2. Phân công

| Thành viên | Module | Đầu việc | Thư mục |
| --- | --- | --- | --- |
| **A** – Tracker & tầng mạng | tracker, wire protocol, kết nối, client tracker | A1–A7 | `src/tracker/`, `src/protocol/`, `src/network/`, `src/trackerclient/` |
| **B** – Peer core | metainfo, lưu trữ chunk, chọn chunk, tải song song, upload, tiến trình `peer` | B1–B7 | `src/torrent/`, `src/storage/`, `src/peer/` |
| **C** – Mô phỏng & đánh giá | launcher, churn, netem, metrics, kiểm thử E2E, thí nghiệm, demo | C1–C7 | `src/sim/`, `scenarios/`, `tests/`, `results/`, `demo/` |

Chi tiết: `docs/work-division.md`.

## 3. Cài đặt

Yêu cầu Python ≥ 3.10 (đã thử trên Windows với `py` 3.14 và Linux với 3.13).

```bash
py -m pip install -r requirements.txt      # Windows   (Linux/macOS: python3 -m pip install -r requirements.txt)
```

Mọi lệnh dưới đây chạy từ thư mục gốc của repo. Trên Linux/macOS thay `py` bằng `python3`.

## 4. Phần của C – chạy mô phỏng, kiểm thử, thí nghiệm, demo

### 4.1 Chạy một kịch bản (C1 + C2 + C3)

```bash
py src/sim/launcher.py --scenario scenarios/baseline.yaml            # in mọi sự kiện ra màn hình
py src/sim/launcher.py --scenario scenarios/high_churn.yaml --quiet  # chỉ in tóm tắt
```

Launcher sinh file dữ liệu tất định + metainfo (Hợp đồng 1, có SHA-1 thật), bật tracker, bật seed/leecher, chạy lịch churn,
fault injection (tracker crash, kill peer, peer gửi chunk hỏng), gom log về `logs/<kịch bản>/events.jsonl` và ghi mã thoát của mọi tiến trình vào
`logs/<kịch bản>/processes.json`. Thư mục dữ liệu của các peer nằm trong thư mục tạm của hệ điều hành (`%TEMP%\ds02-sim`) để tránh khóa file
của OneDrive; thêm `--keep-dirs` để giữ lại.

| Kịch bản | Nội dung |
| --- | --- |
| `baseline.yaml` | 1 seed, 3 leecher, mạng ổn định |
| `multi_seed.yaml` | 4 seed, 3 leecher – tải song song |
| `high_churn.yaml` | Poisson (λ=0.7/s) + thời gian sống Pareto + 50% crash |
| `flash_crowd.yaml` | 8 leecher ập vào trong 0.5 s ở giây 2; 75% rời đồng loạt ở giây 8 khi swarm còn đang tải |
| `tracker_crash.yaml` | tracker bị kill ở giây 3, bật lại ở giây 8; 2 leecher đến lúc tracker đang sập |
| `demo_live.yaml` | kịch bản trình diễn 7 tình huống bắt buộc |

Xem trước lịch churn (tất định theo `random_seed`): `py src/sim/churn.py --scenario scenarios/high_churn.yaml`.

### 4.2 Xuất số liệu (C4)

```bash
py src/sim/metrics.py --scenario baseline      # đọc logs/baseline/events.jsonl → results/baseline_*.csv
```

CSV chứa đủ 5 nhóm số liệu: thời gian tải từng peer (`download_time_sec`), thông lượng theo thời gian (`download_kbps`, `upload_kbps`, KiB/s),
hệ số nhân bản chunk (`avg_chunk_replication`, `chunk_<i>_replicas`), tỉ lệ hoàn tất (`completion_ratio_pct`) và tải tracker
(`tracker_announces`, `tracker_announces_failed` mỗi giây đo phía peer; `tracker_http_requests` đo phía server khi tracker ghi log
request – tracker của A ghi `http_request`; tổng trong `_summary.csv`). `tracker_recovery_sec` = thời gian từ lúc tracker bật lại
tới announce thành công **đầu tiên**. Thời gian tải trung bình chỉ tính leecher đã tải xong.

### 4.3 Kiểm thử toàn hệ thống (C5) – một lệnh

```bash
py tests/e2e/run_all.py           # 20 hạng mục (8 bộ unit test của A, B, C + 12 kiểm thử E2E với peer B + tracker A), ~3 phút
py tests/e2e/run_all.py --quick   # bỏ 6 hạng mục dài, ~50 s – KHÔNG dùng khi chấm vì mất cả 2 test Concurrency
py -m pytest -q tests/test_sim_modules.py tests/test_metainfo.py tests/test_storage.py tests/test_piece_picker.py tests/test_peer.py tests/test_tracker.py tests/test_protocol.py tests/test_trackerclient.py
```

Cuối bảng kết quả có dòng đếm theo nhóm so với mức tối thiểu của đề (Functional 8 · Concurrency 2 · Fault & Recovery 2 ·
Security 2 · Performance 1). `tests/test_network.py` (A5) chưa nằm trong bộ chạy chung vì `test_send_have` lỗi và
`test_inbound_limit` bị treo – đã báo A trong `docs/issues-from-C.md`.

Bao phủ mục 9 của đề: **Functional** (hợp đồng CLI/mã thoát, so SHA-256 + SHA-1 từng chunk với file gốc), **Concurrency** (high churn, flash crowd),
**Fault & Recovery** (kill nguồn → `peer_lost` → `switch_source`, graceful vs crash, tracker crash + respawn),
**Security/invalid data** (chunk sai hash bị từ chối, peer xấu bị cấm), **Performance** (giới hạn băng thông đúng mức cấu hình, multi-seed nhanh hơn baseline).

### 4.4 Thí nghiệm & biểu đồ (C6)

```bash
py src/sim/run_experiments.py                           # 5 kịch bản → CSV + 6 biểu đồ + báo cáo (~2 phút)
py src/sim/run_experiments.py --only baseline multi_seed
py src/sim/report.py                                    # chỉ vẽ lại biểu đồ/báo cáo từ CSV có sẵn
```

Dòng đầu của `analysis.md` ghi peer, tracker và máy đã chạy. Số liệu hiện có đo bằng **peer của B + tracker của A**;
trước khi nộp hãy chạy lại trên máy Windows của nhóm: `py src/sim/run_experiments.py`.

Kết quả: `results/summary_metrics.csv`, `results/charts/exp1…exp6*.png`, và **`results/analysis.md`** (báo cáo tiếng Việt có số liệu cụ thể và kết luận
cho từng kịch bản – dùng trực tiếp cho chương "Thí nghiệm và đánh giá").

### 4.5 Demo trực tiếp (C7) – một lệnh

```bash
py demo/run_demo.py
```

Dashboard trên terminal hiển thị số peer đang hoạt động, vai trò, trạng thái kết nối, bitfield `[####....] 4/16 (25%)`, số chunk đã upload,
sự kiện gần nhất và **danh sách 7 tình huống bắt buộc (mục 6.4)** – mỗi mục chỉ được đánh dấu khi sự kiện thật xảy ra:
tạo metadata & chia sẻ, tải song song từ ≥ 2 peer, leecher upload trước khi tải xong, dừng một nguồn giữa chừng, tự chuyển nguồn,
phát hiện & từ chối chunk sai hash, ghép file & xác minh hash. Demo kết thúc sau ~30 s với `RESULT: 7/7` (mã thoát 0).
Tùy chọn: `--no-clear` (in theo khối, dùng khi quay màn hình/CI), `--no-color`, `--logs-root %TEMP%\ds02-logs`
(ghi log ra ngoài thư mục OneDrive để tránh bị khóa file). Checklist trước khi demo: `deploy/preflight.ps1`.

## 5. Tích hợp A + B + C (mặc định)

Từ 2026-11-22 mọi công cụ của C **mặc định chạy peer của B và tracker của A**:

| Công cụ | Peer mặc định | Tracker mặc định |
| --- | --- | --- |
| `src/sim/launcher.py`, `tests/e2e/run_all.py`, `src/sim/run_experiments.py`, `demo/run_demo.py` | `-m peer` (B, `src/peer/`) | `-m src.tracker` (A, `src/tracker/`) |

Kết quả tích hợp (2026-11-22, `main` + bản sửa của C): `run_all.py` **20/20 PASS**, demo **7/7**, 5 thí nghiệm trong `results/`
đều đo bằng peer của B (`started.meta.implementation = "peer-b"`).

Mọi công cụ nhận **entrypoint** là đường dẫn script hoặc module (`"-m peer"`). `src/` và thư mục gốc được thêm vào `PYTHONPATH` của
tiến trình con, nên `-m peer`, `-m src.tracker`, `-m torrent.make_torrent` chạy được ngay.

### 5.1 Peer của B (Hợp đồng 4)

```
py -m peer --torrent <path> --port <int> --dir <path> --role seed|leech --peer-id <seed|leech-PORT> --max-up-kbps <int> [--exit-on-complete]
```

Seed đọc `DIR/<info.name>` (launcher chép file gốc vào), leech ghi vào đó; log Hợp đồng 5 ra stdout; bắt `SIGINT`, `SIGTERM`, `SIGBREAK`
và file `.sim_stop`; mã thoát `0/1/2/3/130`. Peer-id được pad tới 20 byte cho handshake 68 byte của A (`src/peer/wire.py::wire_peer_id`).
Launcher truyền thêm `--latency-ms` và `--corrupt-pieces` cho peer của B và StubPeer (`Entrypoint.supports_sim_flags`; ép bằng `--sim-flags on|off`).

### 5.2 Tracker của A (Hợp đồng 2)

Tracker chạy bằng `py -m src.tracker --host H --port N --interval S --ttl S`. Mặc định của A là `interval=30, ttl=90`, quá dài cho kịch bản
vài chục giây, nên launcher **tự truyền** `--interval <tracker.interval_sec>` và `--ttl <3 × interval>`. `--tracker-args` vẫn ghi đè được
(đặt sau cùng, argparse lấy giá trị cuối). `--no-tracker` khi tracker đã chạy sẵn ở `tracker.host:port`.

Tracker của A ghi `tracker_started` / `tracker_stopped`; launcher đổi thành `started` / `stopped` (giữ tên gốc trong `meta.original_event`)
và đếm `http_request` tới `/announce` thành `tracker_http_requests`. Kịch bản `tracker_crash` **kill thật** tiến trình tracker rồi bật lại
– phép thử A3 (swarm tự dựng lại từ announce).

### 5.3 Chạy bằng bản giả của C (để so sánh hoặc khi A/B đang sửa)

```bash
py tests/e2e/run_all.py --peer-script src/sim/stub_peer.py --tracker-script src/sim/stub_tracker.py
py src/sim/run_experiments.py --peer-script src/sim/stub_peer.py --tracker-script src/sim/stub_tracker.py --results results/stub
py demo/run_demo.py --peer-script src/sim/stub_peer.py --tracker-script src/sim/stub_tracker.py
```

## 6. Cấu trúc thư mục

```
demo/run_demo.py              C7 – demo một lệnh + dashboard
docs/                         hợp đồng giao tiếp, phân công
docs/report-role-c.md         C – chương Churn / Thí nghiệm & đánh giá / Kết luận & hướng phát triển
docs/review-A-by-C.md         C – review chéo code của A
docs/issues-from-C.md         C – lỗi/lệch hợp đồng gửi A và B
deploy/                       preflight.ps1 (kiểm tra trước demo), run_all.ps1 (chạy kiểm thử + thí nghiệm + demo)
scenarios/*.yaml              C1/C6 – kịch bản
src/sim/launcher.py           C1 – orchestrator
src/sim/churn.py              C2 – Poisson / Exponential / Pareto / flash crowd / mass departure
src/sim/netem.py              C3 – băng thông, độ trễ, tracker crash, kill / corrupt peer
src/sim/metrics.py            C4 – log JSONL → CSV
src/sim/run_experiments.py    C6 – chạy 5 thí nghiệm; report.py vẽ biểu đồ + analysis.md
src/sim/stub_peer.py          StubPeer – peer BitTorrent tối giản nhưng thật (TCP, SHA-1, rarest-first, choke, throttle)
src/sim/stub_tracker.py       StubTracker – tracker HTTP tối giản (announce, scrape, TTL)
src/sim/common.py, procutil.py  schema log, dữ liệu tất định, metainfo, quản lý tiến trình đa nền tảng
src/torrent/, src/storage/, src/piece_picker/, src/peer/   B1–B7 (Role B)
tests/test_sim_modules.py     unit test của C;  tests/test_metainfo.py, test_storage.py của B
tests/e2e/run_all.py          C5 – bộ kiểm thử một lệnh
results/                      CSV, biểu đồ, analysis.md
logs/                         log của từng lần chạy (không commit)
```

## 7. Ghi chú Windows

- Dùng `py` (Python launcher). Mọi file/stdout đều mở UTF-8; tiến trình con chạy với `PYTHONUTF8=1`.
- Dừng đàng hoàng dùng `CTRL_BREAK_EVENT` + file `.sim_stop`; nếu tiến trình không thoát sau 4 s thì bị kill – không còn tiến trình mồ côi.
- Nếu cổng 8000 bận, launcher tự chọn cổng trống cho tracker; nếu cổng peer (9001…) bận, launcher báo lỗi rõ ràng.
- Windows Firewall có thể hỏi quyền cho Python lần đầu – chỉ cần cho phép mạng riêng (mọi kết nối là `127.0.0.1`).
