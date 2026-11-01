# Hợp đồng số 5: Định dạng Log sự kiện (JSON Lines) & File Kịch bản (YAML)

## 1. Định dạng Log sự kiện (Event Log Format)
Mọi tiến trình (`tracker`, `peer`) ghi log sự kiện ra `stdout` hoặc file `.jsonl` theo chuẩn **JSON Lines** (mỗi dòng là 1 đối tượng JSON hợp lệ) gồm các trường:

| Trường | Kiểu | Bắt buộc | Mô tả |
| :--- | :--- | :---: | :--- |
| `ts` | `float` | Có | Unix epoch timestamp tính bằng giây (ví dụ `time.time()`). |
| `peer_id` | `string` | Có | Định danh duy nhất của node (`tracker`, `seed-01`, `leech-01`...). |
| `event` | `string` | Có | Tên sự kiện chuẩn hóa (xem bảng dưới). |
| `chunk_index` | `int \| null` | Có | Chỉ số chunk liên quan (để `null` nếu sự kiện không gắn với chunk). |
| `meta` | `object` | Không | Thông tin phụ (`bytes`, `from_peer`, `to_peer`, `status`, `reason`). |

### Danh mục mã sự kiện (`event`) chuẩn:
- `started`: Tiến trình khởi động thành công.
- `announce_ok`: Peer gửi announce lên Tracker thành công.
- `piece_done`: Tải xong và xác minh đúng mã băm SHA-1 của 1 chunk.
- `piece_uploaded`: Phục vụ (upload) thành công 1 chunk cho peer khác.
- `invalid_chunk`: Phát hiện và từ chối 1 chunk bị sai mã băm.
- `peer_lost`: Phát hiện mất kết nối tới một peer nguồn.
- `switch_source`: Chuyển yêu cầu tải chunk sang một peer nguồn khác.
- `completed`: Peer đã tải xong toàn bộ các chunk và ghép file khớp hash.
- `stopped`: Tiến trình kết thúc đàng hoàng (graceful exit).

**Ví dụ một dòng log hợp lệ:**
```json
{"ts": 1762001234.567, "peer_id": "leech-01", "event": "piece_done", "chunk_index": 3, "meta": {"bytes": 262144, "from_peer": "seed-01"}}