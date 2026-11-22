# Issue từ C gửi A và B

Theo `docs/work-division.md` mục 5: C không sửa mã của A và B; lỗi phát hiện khi tích hợp được ghi ở đây để mở issue cho đúng chủ module.
Mỗi mục có bằng chứng tái hiện và đề xuất sửa tối thiểu. Kiểm tra ngày 2026-11-22 trên `main` mới nhất (`6eabd53`).

## Gửi B (Peer core)

**Không có lỗi chặn.** Peer của B (`src/peer/`, commit `fc05969`, 09/11) đã được tích hợp vào bộ mô phỏng của C ngày 2026-11-22:

- `py tests/e2e/run_all.py` (mặc định giờ là `-m peer` + `-m src.tracker`): **20/20 PASS**, gồm cả `tests/test_peer.py` của B.
- `py demo/run_demo.py`: **7/7** mục bắt buộc với peer của B.
- 5 thí nghiệm trong `results/` đo bằng peer của B (`started.meta.implementation = "peer-b"`).
- Peer của B đã đáp ứng các điểm C cần: handshake 68 byte của A với peer-id pad tới 20 byte (`src/peer/wire.py::wire_peer_id`),
  bắt `SIGINT/SIGTERM/SIGBREAK` + file `.sim_stop`, mã thoát 0/1/2/3/130, cờ thử nghiệm `--corrupt-pieces` và `--latency-ms`,
  log đủ `bytes`, `from_peer`, `to_peer`, `sha256` theo Hợp đồng 5.

Đề xuất nhỏ (không bắt buộc): ghi các cờ thử nghiệm `--corrupt-pieces`, `--latency-ms`, `--ip`, `--bind` vào
`docs/contracts/metainfo-and-peer-cli.md` để Hợp đồng 4 khớp với `src/peer/peer.py::parse_args`.

## Gửi A (Tracker & tầng mạng)

### A-1 [HIGH] `tests/test_network.py::TestTcpTransportMessaging::test_send_have` fail

- **Tái hiện:** `py -m pytest -q tests/test_network.py::TestTcpTransportMessaging::test_send_have` → `TypeError: Have.__init__() got an unexpected keyword argument 'index'`.
- **Nguyên nhân:** `src/protocol/messages.py::Have` có trường `piece_index`, test gọi `Have(index=42)` và kiểm `msg.index`.
- **Sửa tối thiểu:** trong test dùng `Have(piece_index=42)` và `msg.piece_index`.

### A-2 [HIGH] `tests/test_network.py::TestTcpTransportMaxConnections::test_inbound_limit` bị treo

- **Tái hiện:** `py -m pytest -q tests/test_network.py::TestTcpTransportMaxConnections::test_inbound_limit` không kết thúc (đã thử timeout 20 s, Python 3.13).
- **Nguyên nhân:** `src/network/tcp_transport.py::TcpTransport.stop` gọi `self._server.close(); await self._server.wait_closed()` **trước** khi đóng các kết nối đang mở. Từ Python 3.12.1, `asyncio.Server.wait_closed()` chờ tới khi mọi kết nối của server đóng hết, nên `stop()` của `transport_a` (đang giữ kết nối từ B) chờ mãi. Máy của nhóm dùng Python 3.14 nên cũng bị.
- **Sửa tối thiểu:** trong `stop()`, chuyển khối "Disconnect every peer" lên trước khối "Close the TCP server" (hoặc gọi `self._server.close_clients()` khi Python ≥ 3.13 trước `wait_closed()`). Peer của B hiện dùng socket riêng (`src/peer/wire.py`) nên không bị ảnh hưởng, nhưng ai dùng `TcpTransport` sẽ treo khi tắt.
- **Đã kiểm chứng trên bản sao tạm (không sửa repo):** đổi thứ tự như trên thì `tests/test_network.py` chạy xong trong 2.9 s, 23/24 pass; test còn fail là A-1.
- **Ảnh hưởng:** C phải tạm bỏ `tests.test_network` khỏi `tests/e2e/run_all.py`; khi sửa xong, thêm lại một dòng `("Unit: Role A TCP transport (A5)", "Fault & Recovery", lambda: self.unit("tests.test_network", "network"))`.

### A-3 [MEDIUM] Tracker không bắt `SIGBREAK` trên Windows

- `src/tracker/__main__.py` chỉ đăng ký `SIGINT`, `SIGTERM`. Launcher dừng đàng hoàng trên Windows bằng `CTRL_BREAK_EVENT` → tracker bị kết thúc ngay, không log `tracker_stopped`, mã thoát khác 0.
- **Sửa tối thiểu:** thêm `signal.signal(getattr(signal, "SIGBREAK", signal.SIGTERM), _handle_signal)`.

### A-4 [LOW] Dòng không phải JSON trên stdout

- `print(f"Tracker running on ...")` trong `src/tracker/__main__.py` đi ra stdout, bị launcher đưa vào `noise.log`. Hợp đồng 5 mục 1.1: thông báo cho người đọc → stderr. Sửa: `print(..., file=sys.stderr)`.

### A-5 [LOW] Trường phụ của log tracker nằm ở top-level

- Log của tracker có `host`, `port`, `client`, `request`, `removed` ngang hàng với `event` thay vì trong `meta`, và không có `peer_id`, `chunk_index`. Launcher tự bù nên vẫn qua `validate_event`; nên chuyển vào `meta` để đúng Hợp đồng 5 mục 1.2.

## Đã xử lý phía C (không cần A/B làm gì)

- Launcher tự truyền `--interval <tracker.interval_sec>` và `--ttl <3 × interval>` cho tracker của A (`src/sim/launcher.py::_tracker_argv`); trước đây tracker của A chạy với mặc định 30 s / 90 s nếu quên `--tracker-args`.
- `src/sim/metrics.py` đếm `http_request` tới `/announce` của tracker A thành `tracker_http_requests` (tải tracker phía server).
