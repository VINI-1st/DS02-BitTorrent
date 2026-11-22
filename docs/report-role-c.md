# Báo cáo – phần của C (Churn, thí nghiệm, đánh giá; Kết luận và hướng phát triển)

Theo `docs/work-division.md` mục 6: chương chuyên sâu của C là "Churn, thí nghiệm, đánh giá"; phần chung của C là "Kết luận, hướng phát triển".
Số liệu lấy từ `results/summary_metrics.csv` và `results/analysis.md` (sinh bởi `py src/sim/run_experiments.py`, đo với **peer của B**
`-m peer` và **tracker của A** `-m src.tracker`). Dòng đầu của `analysis.md` ghi peer, tracker và máy đã chạy; **chạy lại trên máy Windows
của nhóm trước khi nộp** rồi cập nhật các con số trong chương này. Báo cáo chung của nhóm ở `report/main.tex`; chương này dùng để ghép vào đó.

---

## Chương X. Mô phỏng churn, thí nghiệm và đánh giá

### X.1 Mục tiêu

Hệ P2P chỉ có ý nghĩa khi vẫn đúng và vẫn nhanh lúc thành viên thay đổi liên tục (churn) và khi node trung tâm (tracker) hỏng.
Chương này trả lời bốn câu hỏi: (1) thêm nguồn có làm tải nhanh hơn không; (2) leecher có thực sự phục vụ nhau không;
(3) swarm chịu được peer vào/ra/sập đến mức nào; (4) tracker sập ảnh hưởng gì tới việc trao đổi dữ liệu.

### X.2 Phương pháp: quan sát hệ thống từ bên ngoài

Bộ mô phỏng (C) không sửa mã của tracker (A) và peer (B). Nó chỉ dùng hai giao diện đã chốt:

- **Điều khiển** qua CLI của peer (Hợp đồng 4, `python -m peer` của B) và tracker (`python -m src.tracker` của A): `src/sim/launcher.py` bật mỗi node thành một tiến trình hệ điều hành riêng
  (`subprocess.Popen`), mọi trao đổi đi qua TCP/HTTP thật trên `127.0.0.1`.
- **Quan sát** qua log JSON Lines (Hợp đồng 5, `docs/log-and-scenario-spec.md`): mỗi tiến trình in một dòng JSON cho mỗi sự kiện
  (`started`, `piece_done`, `piece_uploaded`, `peer_lost`, `switch_source`, `completed`, `stopped`…). Launcher dùng một luồng đọc
  cho mỗi tiến trình và một khóa chung để gộp mọi dòng vào `logs/<kịch bản>/events.jsonl`.

Kịch bản là file YAML (`scenarios/*.yaml`) mô tả số seed/leecher, kích thước file, `piece_length`, băng thông, churn và lỗi cần gây ra.
Mọi kịch bản dùng file 2 MiB sinh tất định (SHAKE-256), chia 16 chunk × 128 KiB; mỗi peer upload tối đa 128–256 KiB/s (`--max-up-kbps`).

### X.3 Mô hình churn (C2 – `src/sim/churn.py`)

| Thành phần | Mô hình | Công thức / tham số |
| --- | --- | --- |
| Thời điểm đến | Quá trình Poisson cường độ λ | khoảng cách giữa hai lần đến ~ Exp(λ), trung bình 1/λ |
| Thời gian sống | Mũ hoặc Pareto | Exp: trung bình `mean`, không nhớ. Pareto(α, x_m) với x_m = mean·(α−1)/α để kỳ vọng đúng `mean` |
| Cách rời | Đàng hoàng hoặc sập | sập với xác suất `crash_prob`; đàng hoàng = SIGTERM / `CTRL_BREAK_EVENT` + file `.sim_stop`, peer kịp announce `stopped`; sập = SIGKILL / `TerminateProcess`, không lời tạm biệt |
| Flash crowd | `burst_size` peer đến đều trong `burst_window_sec` | kèm rời đồng loạt `depart_fraction` ở `mass_depart_time_sec` |

Pareto với α = 1.8 (kịch bản high_churn, trung bình 8 s) có x_m = 3.56 s, trung vị ≈ 5.2 s và **phương sai vô hạn** (α ≤ 2):
phần lớn peer sống ngắn, một số rất ít sống rất lâu – đúng với các đo đạc mạng P2P thực tế, và khắc nghiệt hơn phân phối mũ cùng trung bình.

**Lặp lại được:** mọi phép rút ngẫu nhiên dùng bốn bộ sinh `random.Random` riêng, seed bằng chuỗi `"<seed>:arrival"`, `":lifetime"`,
`":leave"`, `":jitter"`. Cùng `random_seed` cho cùng lịch; dấu vân tay SHA-256 của lịch được ghi vào sự kiện `scenario_start` và được kiểm
bởi test "Churn: same seed => same schedule". Cần phân biệt: **lịch** là tất định, còn **kết quả tải** vẫn dao động giữa các lần chạy vì
lập lịch luồng/tiến trình của hệ điều hành (ví dụ high_churn: 3/10 và 4/10 leecher động tải xong ở các lần chạy cùng seed).

### X.4 Giả lập mạng và lỗi (C3 – `src/sim/netem.py`)

- Băng thông: cờ `--max-up-kbps` (Hợp đồng 4), peer tự giới hạn bằng token bucket. Kiểm chứng: 768 KiB với giới hạn 192 KiB/s phải mất ≥ 4.0 s
  (test "Bandwidth cap enforced").
- Độ trễ: `--latency-ms` thêm ở tầng ứng dụng (Windows không có `tc netem`); peer của B và StubPeer đều hỗ trợ.
- Tracker sập: tiến trình tracker bị kill thật ở `crash_at_sec`, bật lại sau `downtime_sec` với trạng thái rỗng.
- Peer lỗi: `faults.kill_peers` dừng một nguồn giữa chừng; `faults.corrupt_peers` cho một peer gửi dữ liệu sai hash.

### X.5 Số liệu đo (C4 – `src/sim/metrics.py`)

| Số liệu | Định nghĩa |
| --- | --- |
| Thời gian tải | `completed.ts − started.ts` của từng leecher; trung bình chỉ tính leecher đã xong |
| Thông lượng | tổng byte của `piece_done` (tải về) / `piece_uploaded` (tải lên) trong mỗi giây, KiB/s |
| Hệ số nhân bản | mỗi giây, số peer đang online giữ chunk i (`chunk_<i>_replicas`), trung bình và nhỏ nhất qua các chunk |
| Tỉ lệ hoàn tất | số leecher đã `completed` / số leecher đã tham gia |
| Tải tracker | phía peer: `announce_ok`, `announce_failed` mỗi giây; phía server: `tracker_http_requests` (khi tracker ghi log request) |

### X.6 Kết quả

Lần chạy: 2026-11-22 17:00, Windows 11, Python 3.14.2, peer của B + tracker của A (dòng đầu `results/analysis.md`).

| Kịch bản | Thay đổi so với baseline | Hoàn tất | Thời gian tải TB | Nhân bản TB | Leecher upload | Request /announce (server) | peer_lost / switch |
| --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 1 seed, 3 leecher | 100% (3/3) | 11.8 s | 2.38 | 56% | 32 | 0 / 0 |
| multi_seed | 4 seed | 100% (3/3) | 5.2 s | 4.81 | 27% | 35 | 0 / 0 |
| high_churn | + Poisson λ = 0.7/s, Pareto, 50% sập | 42% (5/12) | 12.2 s* | 3.46 | 75% | 91 | 27 / 13 |
| flash_crowd | + 8 peer đến trong 0.5 s, 75% rời ở 8 s | 33% (3/9) | 12.0 s* | 2.46 | 68% | 53 | 32 / 6 |
| tracker_crash | 2 seed, tracker chết 3 → 8 s | 100% (5/5) | 16.1 s | 4.00 | 60% | 62 (+ 14 lỗi) | 0 / 0 |

\* chỉ tính leecher đã tải xong (survivor bias) – không so trực tiếp với baseline.

Biểu đồ: `results/charts/exp1_download_time.png` … `exp6_upload_share.png`.

### X.7 Đánh giá

1. **P2P nhân băng thông.** Ở baseline, một seed 256 KiB/s phải gửi 3 × 2 MiB nếu tự phục vụ, tức ≥ 24 s; thực tế 11.8 s vì 56% dữ liệu
   do leecher phục vụ nhau ngay khi có chunk.
2. **Tải song song từ nhiều nguồn.** 4 seed nhanh hơn 1 seed 2.25×, chưa tới 4× vì số yêu cầu song song của mỗi leecher có giới hạn,
   có độ trễ cố định mỗi chunk, và baseline đã được leecher bù băng thông.
3. **Chịu churn.** Khi peer sập, peer khác phát hiện (`peer_lost`) và chuyển chunk sang nguồn khác (`switch_source`); mọi leecher tĩnh vẫn tải xong
   đúng SHA-256. Tỉ lệ hoàn tất thấp (42%) là do Pareto cho phần lớn peer sống 3.6–5.2 s, ngắn hơn ~6–13 s cần để tải xong – không phải do
   mất dữ liệu: hệ số nhân bản nhỏ nhất luôn ≥ 1 vì seed gốc không rời.
4. **Flash crowd và rời đồng loạt.** Khi 8 peer cùng đến ở giây 2, hệ số nhân bản tăng từ 1.0 lên 3.56 trong khoảng giây 1–7 nhờ leecher phục vụ nhau; khi 6/8 rời ở
   giây 8 (chưa ai xong), hệ số nhân bản giảm ngay từ 3.56 xuống 2.31 nhưng không chunk nào biến mất, và 3 leecher còn lại vẫn tải xong.
5. **Tracker không nằm trên đường dữ liệu.** Trong 5.4 s tracker chết vẫn có 2176 KiB được trao đổi giữa các peer đã biết nhau;
   14 announce thất bại rồi announce thành công đầu tiên đến 0.23 s sau khi tracker bật lại. Hai leecher đến lúc tracker chết vẫn tải xong
   sau khi tracker sống lại – tracker là soft state, dựng lại hoàn toàn từ announce định kỳ.
6. **Tính đúng.** Ở mọi kịch bản, mọi leecher báo `completed` đều có file trùng SHA-256 và trùng SHA-1 từng chunk với file gốc
   (`tests/e2e/run_all.py::Suite.verify_files`); chunk sai hash bị từ chối và nguồn gửi sai bị cấm sau 2 lần.

### X.8 Kiểm thử (C5)

`py tests/e2e/run_all.py` chạy 20 hạng mục (8 bộ unit test của A, B, C và 12 kiểm thử E2E với tiến trình thật của A và B) và in số lượng theo nhóm:
Functional 8, Concurrency 2, Fault & Recovery 5, Security 3, Performance 2 – đạt mức tối thiểu của đề (8 / 2 / 2 / 2 / 1). Kết quả trên Windows 11 (2026-11-22): 20/20 PASS; trên Linux cũng 20/20.

### X.9 Hạn chế của phương pháp

- Chạy trên một máy (loopback): băng thông và độ trễ là giới hạn ở tầng ứng dụng, không mô phỏng mất gói hay tắc nghẽn mạng.
- Mỗi kịch bản chạy một lần; chưa có trung bình nhiều lần chạy và khoảng tin cậy.
- Poisson bị cắt ở `max_dynamic_peers` peer, nên trong high_churn chỉ có 10 peer động (kỳ vọng ~23 nếu không cắt).

---

## Kết luận

Hệ thống đáp ứng các khái niệm chính của đề: các peer là tiến trình độc lập trao đổi chunk có kiểm SHA-1 qua TCP, tracker chỉ giúp tìm peer,
leecher vừa tải vừa phục vụ, tải song song từ nhiều nguồn làm thời gian tải giảm 2.25×, và hệ thống vẫn cho kết quả đúng khi peer vào/ra/sập
hoặc khi tracker bị kill. Bộ mô phỏng cho phép lặp lại cùng một lịch churn bằng `random_seed`, đo năm nhóm số liệu từ log và kiểm thử tự động
bằng một lệnh, chạy trực tiếp trên peer của B và tracker của A.

## Hướng phát triển

1. So sánh peer của B với StubPeer của C trên cùng kịch bản (`--peer-script src/sim/stub_peer.py`) để tách ảnh hưởng của chiến lược tải.
2. Chạy mỗi kịch bản 5–10 lần với các `random_seed` khác nhau, báo cáo trung bình và khoảng tin cậy 95%.
3. Thử nhiều máy thật (cờ `--bind 0.0.0.0`, `--ip` của peer) để có độ trễ và mất gói thật thay vì giả lập.
4. Đo tải tracker phía server ở quy mô lớn (hàng trăm peer) bằng `scripts/benchmark_tracker.py` của A và cột `tracker_http_requests`.
5. Thêm chiến lược chống churn ở peer: giữ lại một phần thời gian seed sau khi tải xong, ưu tiên chunk hiếm hơn khi số bản sao nhỏ nhất < 2.
6. Nhiều tracker hoặc DHT để bỏ điểm hỏng đơn của tracker.
