# Review chéo: C review code của A

Theo `docs/work-division.md` mục 6 ("Review code chéo: C review code của A"). Phạm vi: `src/tracker/`, `src/protocol/`, `src/network/`,
`src/trackerclient/`, `scripts/benchmark_tracker.py`. Ngày 2026-11-22. Lỗi cần sửa được chép sang `docs/issues-from-C.md` (A-1 … A-5).

## Kết luận

Tracker của A đúng Hợp đồng 2 và chạy được dưới launcher của C: kịch bản `tracker_crash` với `--tracker-script "-m src.tracker"` đạt 5/5 leecher,
tracker bị kill thật, bật lại và swarm tự dựng lại. 89 unit test của tracker, protocol, tracker client pass. Tầng `network/` có một lỗi treo khi
dừng trên Python ≥ 3.12.1 (A-2). Peer của B dùng `src/protocol` (codec, handshake 68 byte) của A nhưng **không** dùng `TcpTransport`
(A5) mà tự mở socket trong `src/peer/wire.py` – cần thống nhất cách giải thích điểm này khi bảo vệ.

## Điểm tốt

- `src/tracker/swarm.py::SwarmManager`: mọi thao tác trên swarm nằm dưới một `threading.Lock`; `ThreadingHTTPServer` xử lý mỗi request một thread – đúng yêu cầu A3 về đồng thời. Có test `test_concurrent_100_peers`, `test_concurrent_http_announces`.
- Trạng thái swarm là soft state trong bộ nhớ, peer tự đăng ký lại bằng announce định kỳ; `cleanup_expired` xóa peer quá TTL – đúng A2.
- `announce` loại chính người gọi khỏi danh sách và lấy mẫu ngẫu nhiên tối đa 50 peer – đúng A1.
- `src/network/tcp_transport.py`: mọi `connect` và đọc handshake đều có `asyncio.wait_for(..., connection_timeout)`; từ chối `info_hash` sai; giới hạn kích thước khung.
- `src/trackerclient/client.py`: backoff mũ có trần (`initial_backoff = 2 s`, `max_backoff = 60 s`) và jitter 10% trên `interval` – tránh mọi peer announce cùng lúc.

## Vấn đề

| # | File | Vấn đề | Mức |
| --- | --- | --- | --- |
| 1 | `src/network/tcp_transport.py::TcpTransport.stop` | `server.wait_closed()` trước khi đóng kết nối → treo trên Python ≥ 3.12.1 (A-2) | High |
| 2 | `tests/test_network.py::test_send_have` | Dùng `Have(index=...)` trong khi dataclass có `piece_index` (A-1) | High |
| 3 | `src/network/tcp_transport.py::_read_message` | Giới hạn khung `2**17` byte (128 KiB): message `piece` chứa trọn chunk 128 KiB (131 072 + 9 byte) sẽ bị từ chối. Peer của B hiện tự quản lý socket (`src/peer/wire.py`) và chỉ dùng codec của A, nên chưa bị ảnh hưởng; nên ghi giới hạn này vào `docs/protocol.md` | Low |
| 4 | `src/tracker/__main__.py` | Không bắt `SIGBREAK` → không dừng đàng hoàng trên Windows (A-3) | Medium |
| 5 | `src/tracker/swarm.py::announce` | `_downloaded[info_hash] += 1` ở **mỗi** announce `completed`, kể cả khi một peer gửi lại; `scrape.downloaded` có thể đếm trùng. Nên chỉ cộng khi trạng thái chuyển từ `left > 0` sang `completed` | Low |
| 6 | `src/tracker/swarm.py` | `info_hash` không được chuẩn hóa chữ thường; peer gửi hex in hoa sẽ rơi vào swarm khác. Hợp đồng 1 quy định hex thường nên chưa gây lỗi | Low |
| 7 | `src/tracker/__main__.py`, `server.py` | `print(...)` ra stdout; trường log ở top-level thay vì `meta` (A-4, A-5) | Low |
| 8 | `src/tracker/server.py::TrackerServer` | Vòng dọn TTL chạy mỗi `interval` giây, nên peer chết bị xóa sau TTL + tối đa 1 interval (tới 8 s với `--interval 2 --ttl 6`). Nên ghi rõ trong báo cáo A2 | Info |

## Câu hỏi cho A (để thống nhất trước buổi bảo vệ)

- `TrackerClient` mặc định `interval = 30` khi tracker không trả `interval`; khi B dùng client này dưới launcher, tracker luôn trả `interval = 2` nên ổn. Có cần ghi rõ trong `docs/protocol.md` không?
- Benchmark A7 (`scripts/benchmark_tracker.py`) đã có số req/s và độ trễ chưa? C cần số này cho chương đánh giá (tải tracker phía server).
