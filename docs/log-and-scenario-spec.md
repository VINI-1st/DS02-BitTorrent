# Hợp đồng 5 – Log sự kiện (JSON Lines) & định dạng file kịch bản (YAML)

**Chủ sở hữu:** C · **Người dùng:** A (tracker), B (peer) · Thay đổi cần cả ba đồng ý (xem `docs/work-division.md`, mục 8).

Tài liệu này là nguồn duy nhất để A và B biết **phải ghi log thế nào** để bộ mô phỏng của C (launcher, metrics, kiểm thử, demo)
đọc được mà không phải sửa mã. Bộ kiểm tra tự động: `src/sim/common.py::validate_event` (được dùng trong `tests/e2e/run_all.py`).

---

## 1. Log sự kiện – Hợp đồng 5

### 1.1 Quy tắc chung

- Mỗi tiến trình (`tracker`, `peer`) ghi **một đối tượng JSON trên một dòng** (JSON Lines), mã hóa **UTF-8**, ra **stdout**
  (mặc định) hoặc ra file `.jsonl` khi có `--log PATH`.
- **Flush sau mỗi dòng** (`print(..., flush=True)`) để launcher nhận sự kiện theo thời gian thực.
- **Không** in gì khác lên stdout. Thông báo cho người đọc, traceback, debug → **stderr**
  (launcher lưu stderr vào `logs/<kịch bản>/stderr/<node>.log`; dòng stdout không phải JSON bị đưa vào `noise.log`).
- Không dùng NaN/Infinity; mọi khóa là chuỗi.

### 1.2 Các trường

| Trường | Kiểu | Bắt buộc | Mô tả |
| :--- | :--- | :---: | :--- |
| `ts` | `float` | Có | Unix epoch, giây (`time.time()`), độ phân giải ≥ 1 ms. |
| `peer_id` | `string` | Có | ID của node: `"tracker"`, `"seed-9001"`, `"leech-9002"`… Peer dùng giá trị của `--peer-id` nếu được truyền. |
| `event` | `string` | Có | Một trong các mã ở mục 1.3. |
| `chunk_index` | `int \| null` | Có | Chỉ số chunk (0-based) với sự kiện gắn với chunk, còn lại `null`. |
| `meta` | `object` | Nên có | Thông tin phụ. Có thể thêm khóa tùy ý; các khóa ở bảng 1.3 được C đọc. |

### 1.3 Mã sự kiện bắt buộc (A & B phát ra)

| `event` | Ai ghi | Khi nào | `chunk_index` | `meta` mà C đọc |
| --- | --- | --- | --- | --- |
| `started` | tracker, peer | Tiến trình đã sẵn sàng (đã mở cổng) | `null` | `role` (`seed`/`leech`/`tracker`), `port`, `num_pieces` |
| `announce_ok` | peer | Một lần announce tới tracker thành công | `null` | `announce_event` (`started`/`completed`/`stopped`/`periodic`), `peers` (số peer nhận về) |
| `piece_done` | peer (leech) | Nhận đủ và **xác minh SHA-1 đúng** một chunk | chỉ số | `bytes`, `from_peer` |
| `piece_uploaded` | peer | Đã gửi xong trọn một chunk cho peer khác | chỉ số | `bytes`, `to_peer` |
| `invalid_chunk` | peer (leech) | Chunk nhận về sai SHA-1 và bị loại | chỉ số | `from_peer`, `reason` (`sha1_mismatch`) |
| `peer_lost` | peer | Mất kết nối / không kết nối được tới một peer nguồn đã từng kết nối | chỉ số hoặc `null` | `peer`, `reason` |
| `switch_source` | peer (leech) | Chuyển việc tải một chunk sang nguồn khác sau lỗi | chỉ số | `old_peer`, `new_peer`, `reason` (`peer_lost`/`invalid_chunk`) |
| `completed` | peer (leech) | Có đủ chunk, ghép file và **file khớp hash** | `null` | `sha256` (của file ghép), `bytes` |
| `stopped` | tracker, peer | Thoát **đàng hoàng** (SIGINT/SIGTERM/SIGBREAK, hoặc tải xong với `--exit-on-complete`) | `null` | `reason` |

Một tiến trình bị **kill** (crash) sẽ không ghi được `stopped` – đó chính là cách C phân biệt *rời đàng hoàng* và *sập*.

### 1.4 Sự kiện mở rộng (không bắt buộc với A, B)

| `event` | Ai ghi | Ý nghĩa |
| --- | --- | --- |
| `announce_failed` | peer (khuyến khích) | Announce thất bại (tracker sập/timeout). Dùng để đo tải tracker khi sập. `meta.error` |
| `scenario_start`, `scenario_end` | `simulator` | Bắt đầu/kết thúc kịch bản; `meta.churn_fingerprint` = SHA-256 của lịch churn |
| `torrent_created` | `simulator` | Metainfo đã tạo: `info_hash`, `num_pieces`, `sha256` của file gốc |
| `spawned`, `process_exit` | `simulator` | Launcher bật / thấy tiến trình kết thúc: `target`, `role`, `pid`, `returncode`, `end_reason` |
| `churn_join`, `churn_leave`, `churn_crash` | `simulator` | Hành động của engine churn (C2): `target`, `cause` |
| `fault_kill_peer` | `simulator` | Dừng một peer theo `faults.kill_peers`: `target`, `mode` |
| `tracker_crashed`, `tracker_restarted` | `tracker` (do launcher ghi) | Fault injection C3: tracker bị kill / được bật lại |

Nếu peer thật dùng `peer_id` khác tên node của launcher (ví dụ ID 20 ký tự), launcher tự thêm `meta.node = "<seed|leech>-<port>"`
vào mọi dòng của tiến trình đó, nên A/B **không phải** làm gì thêm.

### 1.5 Ví dụ

```json
{"ts": 1793862432.5742, "peer_id": "leech-9002", "event": "piece_done", "chunk_index": 3, "meta": {"bytes": 131072, "from_peer": "seed-9001"}}
{"ts": 1793862432.6011, "peer_id": "seed-9001", "event": "piece_uploaded", "chunk_index": 3, "meta": {"bytes": 131072, "to_peer": "leech-9002"}}
{"ts": 1793862433.1020, "peer_id": "leech-9003", "event": "invalid_chunk", "chunk_index": 5, "meta": {"from_peer": "seed-9003", "reason": "sha1_mismatch"}}
{"ts": 1793862434.0005, "peer_id": "leech-9003", "event": "peer_lost", "chunk_index": 2, "meta": {"peer": "seed-9002", "reason": "ConnectionResetError"}}
{"ts": 1793862434.0102, "peer_id": "leech-9003", "event": "switch_source", "chunk_index": 2, "meta": {"old_peer": "seed-9002", "new_peer": "leech-9002", "reason": "peer_lost"}}
{"ts": 1793862440.2741, "peer_id": "leech-9002", "event": "completed", "chunk_index": null, "meta": {"sha256": "5614d2d7...", "bytes": 2097152}}
```

### 1.6 Đoạn mã mẫu cho A và B

```python
import json, sys, time

def log_event(peer_id, event, chunk_index=None, **meta):
    print(json.dumps({"ts": time.time(), "peer_id": peer_id, "event": event,
                      "chunk_index": chunk_index, "meta": meta}, ensure_ascii=False), flush=True)

log_event("leech-9002", "piece_done", 3, bytes=131072, from_peer="seed-9001")
```

Tự kiểm tra một file log: `py -c "from src.sim.common import read_jsonl, validate_event; print([validate_event(r) for r in read_jsonl('logs/baseline/events.jsonl') if validate_event(r)])"`.

---

## 2. Những gì launcher yêu cầu ở tiến trình của A và B

| Hạng mục | Peer (B, Hợp đồng 4) | Tracker (A, Hợp đồng 2) |
| --- | --- | --- |
| Lệnh khởi động | `<peer> --torrent P --port N --dir D --role seed\|leech --peer-id ID --max-up-kbps K [--exit-on-complete]` | `<tracker> --host H --port N` |
| Seed | đọc `D/<info.name>` (launcher đã chép file gốc vào) | – |
| Dừng đàng hoàng | bắt **SIGINT, SIGTERM và SIGBREAK** (Windows), announce `stopped`, log `stopped`, thoát | tương tự |
| Mã thoát | 0 / 1 / 2 / 3 / 130 theo Hợp đồng 4 | 0 khi dừng bình thường |
| Ghi log | Hợp đồng 5 ra stdout | Hợp đồng 5 ra stdout (`started`, `stopped`) |

> **Windows:** launcher tạo mỗi tiến trình trong process group riêng và gửi `CTRL_BREAK_EVENT` để dừng đàng hoàng.
> Python nhận nó là `signal.SIGBREAK`; hãy đăng ký handler: `signal.signal(getattr(signal, "SIGBREAK", signal.SIGTERM), handler)`.
> Launcher còn tạo file `D/.sim_stop` trước khi gửi tín hiệu – peer có thể (không bắt buộc) kiểm tra file này.
> Sau 4 giây nếu tiến trình chưa thoát, launcher kill.

---

## 3. File kịch bản (YAML)

Mỗi file `scenarios/<tên>.yaml` mô tả một thí nghiệm. Chỉ `name` là bắt buộc; mọi khóa khác có giá trị mặc định (cột "Mặc định").
Kiểm tra cú pháp: `src/sim/launcher.py::validate_scenario` (chạy trong unit test).

| Khóa | Kiểu | Mặc định | Ý nghĩa |
| --- | --- | --- | --- |
| `name` | string | tên file | Tên kịch bản → `logs/<name>/`, `results/<name>_*.csv` |
| `description` | string | – | Mô tả cho người đọc |
| `tracker.host`, `tracker.port` | string, int | `127.0.0.1`, `8000` | Địa chỉ tracker (cổng bận → tự chọn cổng trống) |
| `tracker.interval_sec` | int | 2 | Chu kỳ announce (chỉ StubTracker) |
| `torrent.file_name` | string | `sample_data.bin` | Tên file chia sẻ (`info.name`) |
| `torrent.file_size_bytes` | int | 1048576 | Kích thước file, sinh tất định |
| `torrent.piece_length` | int | 262144 | Kích thước chunk |
| `torrent.data_seed` | string | `ds02` | Seed của bộ sinh dữ liệu tất định |
| `swarm.num_seeds` | int ≥ 1 | 1 | Số seed tĩnh (cổng `base_port …`) |
| `swarm.num_leechers` | int ≥ 0 | 3 | Số leecher tĩnh (cổng tiếp theo) |
| `swarm.base_port` | int | 9001 | Cổng đầu tiên. Node tên `seed-<port>` / `leech-<port>` |
| `swarm.max_up_kbps` | int | 0 | Giới hạn upload mỗi peer, **KiB/s** (0 = không giới hạn) |
| `swarm.leech_exit_on_complete` | bool | false | Truyền `--exit-on-complete` cho leecher |
| `swarm.leech_start_delay_sec` | float | 0 | Khoảng cách giữa các lần bật leecher tĩnh |
| `swarm.late_leechers` | list | [] | `[{at_sec: 4.0, count: 2}]` – leecher đến muộn (cổng `base_port+300…`) |
| `churn.enabled` | bool | false | Bật engine churn C2 |
| `churn.mode` | `poisson` \| `flash_crowd` | `poisson` | Mô hình đến |
| `churn.arrival_rate_per_sec` | float | 0.4 | λ của quá trình Poisson |
| `churn.mean_lifetime_sec` | float | 4.0 | Thời gian sống trung bình |
| `churn.lifetime_dist` | `exponential` \| `pareto` | `exponential` | Phân phối thời gian sống |
| `churn.pareto_alpha` | float > 1 | 2.0 | Tham số đuôi Pareto (x_m chọn để kỳ vọng = mean) |
| `churn.crash_prob` | 0..1 | 0.4 | Xác suất rời bằng **crash** (kill) thay vì **graceful** |
| `churn.max_dynamic_peers` | int | 10 | Số leecher động tối đa |
| `churn.port_offset` | int | 100 | Leecher động dùng cổng `base_port + port_offset + i` |
| `churn.burst_time_sec`, `burst_size`, `burst_window_sec` | float, int, float | 2.0, 5, 0.5 | Flash crowd |
| `churn.mass_depart_time_sec`, `depart_fraction` | float, 0..1 | –, 1.0 | Rời đồng loạt |
| `churn.mass_departure` | map | – | Chế độ poisson: `{at_sec, fraction, crash_prob}` |
| `netem.default_max_up_kbps` | int | `swarm.max_up_kbps` | Băng thông mặc định |
| `netem.latency_ms` | int | 0 | Độ trễ thêm (chỉ StubPeer: `--latency-ms`) |
| `netem.role_profiles` | map | – | `{seed: {max_up_kbps: 128}, leech: {...}}` |
| `netem.peer_profiles` | map | – | `{seed-9002: {max_up_kbps: 32, latency_ms: 150}}` |
| `netem.tracker_fault` | map | – | `{enabled, crash_at_sec, downtime_sec}` hoặc `{enabled, crashes: [{at_sec, downtime_sec}, …]}` |
| `faults.kill_peers` | list | [] | `[{target: seed-9002, at_sec: 4.0, mode: crash\|graceful}]` |
| `faults.corrupt_peers` | map | {} | `{seed-9003: "all" \| "0,3"}` – peer gửi chunk sai (chỉ StubPeer) |
| `simulation.duration_sec` | float | 20 | Thời lượng tối đa |
| `simulation.random_seed` | int | 42 | Seed của churn – cùng seed ⇒ cùng lịch |
| `simulation.stop_when_complete` | bool | true | Dừng sớm khi mọi leecher đã xong/thoát và không còn sự kiện chờ |
| `simulation.linger_after_complete_sec` | float | 0.5 | Thời gian chờ thêm trước khi dừng |

Ví dụ đầy đủ: `scenarios/high_churn.yaml` (Poisson + Pareto + crash), `scenarios/tracker_crash.yaml` (tracker fault + late leechers),
`scenarios/demo_live.yaml` (kill peer + chunk hỏng).

Xem lịch churn của một kịch bản mà không chạy: `py src/sim/churn.py --scenario scenarios/high_churn.yaml`.

---

## 4. Đầu ra của một lần chạy

| File | Nội dung |
| --- | --- |
| `logs/<name>/events.jsonl` | Toàn bộ sự kiện của mọi tiến trình + simulator, theo thứ tự nhận |
| `logs/<name>/processes.json` | Mỗi tiến trình: node, role, port, pid, `returncode`, `end_reason` (`self_exit`, `scenario_end`, `churn_crash`, `churn_leave`, `fault_crash`, `fault_tracker_crash`…) |
| `logs/<name>/stderr/<node>.log` | stderr của từng tiến trình |
| `logs/<name>/noise.log` | Dòng stdout không phải JSON (nên rỗng) |
| `results/<name>_peers.csv`, `_timeseries.csv`, `_summary.csv` | Số liệu C4 (`py src/sim/metrics.py --scenario <name>`) |
