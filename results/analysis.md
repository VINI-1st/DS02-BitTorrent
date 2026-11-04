# Báo cáo Phân tích Thực nghiệm & Đánh giá Hiệu năng (Đầu việc C6)

## 1. Kịch bản Baseline (Cơ sở)
- **Kết quả:** Thời gian tải hoàn tất trung bình đạt **4.484 giây** với tỉ lệ hoàn tất 100%.
- **Kết luận:** Trong điều kiện mạng ổn định với 1 Seeder và 3 Leechers, hệ thống phân phối đầy đủ các mảnh dữ liệu và xác minh khớp mã băm toàn cục.

## 2. Kịch bản Multi-Seed (Tăng số lượng Seeder)
- **Kết quả:** Thời gian tải trung bình giảm xuống còn **4.341 giây** (nhanh hơn so với 4.484 giây ở kịch bản Baseline).
- **Kết luận:** Cơ chế tải song song từ nhiều nguồn giúp phân tải băng thông và rút ngắn đáng kể thời gian tải tệp tin.

## 3. Kịch bản High Churn (Biến động mạng cao theo Poisson & Pareto)
- **Kết quả:** Tỉ lệ hoàn tất đạt **28.57%** bất chấp các peer liên tục gia nhập và rời mạng đột ngột.
- **Kết luận:** Các peer vào mạng đủ lâu vẫn thu thập đủ các mảnh nhờ cơ chế chuyển đổi nguồn tải tự động khi một nguồn bị ngắt kết nối.

## 4. Kịch bản Flash Crowd (Đám đông ập vào đồng loạt)
- **Kết quả:** Thời gian tải trung bình đạt **4.534 giây** khi 5 Leechers mới cùng tham gia tại một thời điểm.
- **Kết luận:** Nhờ việc mỗi Leecher vừa tải xong mảnh đầu tiên đã lập tức chia sẻ lại cho các peer khác, hệ số nhân bản tăng nhanh và giảm áp lực cho Seeder gốc.

## 5. Kịch bản Tracker Crash (Tracker sập và phục hồi giữa chừng)
- **Kết quả:** Tỉ lệ hoàn tất đạt **100.0%** sau khi Tracker bị ngắt và khởi động lại.
- **Kết luận:** Việc truyền tải dữ liệu trực tiếp P2P giữa các peer không bị gián đoạn khi Tracker tạm ngừng hoạt động, và swarm tự động đồng bộ lại sau khi Tracker sống lại.
