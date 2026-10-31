# Hợp đồng 1 & 4 – Metainfo và CLI của peer (chủ sở hữu: B)

Tài liệu này chốt hai hợp đồng do B viết (mục 2 trong `docs/work-division.md`).
A và C đọc để dùng; muốn đổi thì mở issue / pull request và cần cả ba đồng ý.

## Hợp đồng 1 – Metainfo (`*.torrent.json`)

File metainfo là JSON, mã hóa UTF-8:

```json
{
  "announce": "http://127.0.0.1:8000/announce",
  "created_by": "ds02-make-torrent",
  "creation_date": 1793404800,
  "info": {
    "name": "movie.bin",
    "length": 1048576,
    "piece_length": 262144,
    "pieces": [
      "3f786850e387550fdab836ed7e6dc881de23001b",
      "89e6c98d92887913cadf06b2adb97f26cde4849b",
      "2b66fd261ee5c6cfc8de7fa466bab600bcfe4f69",
      "e9d71f5ee7c92d6dc9e92ffdad17b8bd49418f98"
    ]
  },
  "info_hash": "a1b2c3..."
}
```

| Trường | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `announce` | string | URL tracker (A) |
| `created_by` | string | Công cụ tạo file, chỉ để tham khảo |
| `creation_date` | int | Unix time (giây), chỉ để tham khảo |
| `info.name` | string | Tên file gợi ý khi lưu |
| `info.length` | int | Kích thước file, byte |
| `info.piece_length` | int | Kích thước chunk, byte (mặc định 262144 = 256 KiB). Chunk cuối có thể ngắn hơn |
| `info.pieces` | string[] | SHA-1 (hex thường, 40 ký tự) của từng chunk, theo thứ tự chỉ số |
| `info_hash` | string | SHA-1 (hex thường) của `info` ở dạng chuẩn hóa – xem dưới |

**Dạng chuẩn hóa của `info`:** `json.dumps(info, sort_keys=True, separators=(",", ":"), ensure_ascii=False)`
rồi mã hóa UTF-8. `info_hash` là SHA-1 của chuỗi byte đó.
Chỉ `info` được băm, nên đổi `announce` / `created_by` không đổi `info_hash`.

**Quy ước dùng `info_hash`:**

- Gửi tracker (API của A): chuỗi hex 40 ký tự.
- Trong `handshake` của wire protocol (A): 20 byte thô, tức `bytes.fromhex(info_hash)`.

**Kiểm tra khi đọc:** số phần tử `pieces` phải bằng `ceil(length / piece_length)`
(file rỗng có 0 chunk), mọi hash là hex 40 ký tự, và `info_hash` trong file phải
khớp với giá trị tính lại. Sai thì từ chối file.

## Hợp đồng 4 – CLI của peer

```
python -m peer --torrent PATH --port PORT --dir DIR --role {seed,leech}
               [--max-up-kbps N] [--peer-id ID] [--log PATH] [--exit-on-complete]
```

| Tham số | Bắt buộc | Ý nghĩa |
| --- | --- | --- |
| `--torrent` | có | Đường dẫn file metainfo |
| `--port` | có | Cổng TCP lắng nghe kết nối từ peer khác |
| `--dir` | có | Thư mục chứa dữ liệu. Seed đọc `DIR/<info.name>`; leech ghi vào đó |
| `--role` | có | `seed`: đã có đủ file, chỉ phục vụ. `leech`: tải về, có chunk nào phục vụ chunk đó |
| `--max-up-kbps` | không | Giới hạn băng thông tải lên (kilobyte/giây). `0` hoặc bỏ trống = không giới hạn |
| `--peer-id` | không | ID 20 ký tự. Bỏ trống thì sinh ngẫu nhiên với tiền tố `-DS0001-` |
| `--log` | không | File log sự kiện JSON (định dạng do C chốt ở hợp đồng 5). Bỏ trống = stdout |
| `--exit-on-complete` | không | Chỉ với `leech`: thoát mã 0 ngay khi tải xong thay vì tiếp tục seed |

**Hành vi theo vai trò:**

- `seed`: kiểm hash toàn bộ file lúc khởi động, announce `started` với `left=0`,
  phục vụ đến khi nhận SIGINT / SIGTERM, lúc đó announce `stopped` rồi thoát.
- `leech`: tải đến khi đủ mọi chunk và file khớp hash, announce `completed`, rồi
  **tiếp tục seed** cho tới khi bị dừng. Dùng `--exit-on-complete` để thoát ngay sau khi tải xong
  (tiện cho test của C).

**Mã thoát:**

| Mã | Ý nghĩa |
| --- | --- |
| 0 | Thoát bình thường (bị dừng, hoặc tải xong khi có `--exit-on-complete`) |
| 1 | Lỗi tham số hoặc metainfo không hợp lệ |
| 2 | File của seed không khớp hash trong metainfo |
| 3 | Không thể tải xong (hết nguồn, tracker không phản hồi quá lâu) |
| 130 | Bị ngắt bằng SIGINT trước khi hoàn tất (chỉ với `leech`) |

## Công cụ tạo torrent

```
python -m torrent.make_torrent FILE --announce URL [--piece-length N] [-o OUT]
```

Mặc định ghi ra `FILE.torrent.json` và in `info_hash` ra stdout.
