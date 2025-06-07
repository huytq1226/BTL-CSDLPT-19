# Bộ Kiểm Thử Hiệu Suất cho Phân Vùng Dữ Liệu

Dự án này cung cấp các công cụ để kiểm thử hiệu suất của các thuật toán phân vùng dữ liệu (Range Partition và Round Robin Partition) với các bộ dữ liệu khác nhau.

## Cài đặt

1. Cài đặt các thư viện cần thiết:

```bash
pip install -r requirements.txt
```

2. Đảm bảo PostgreSQL đã được cài đặt và đang chạy trên máy của bạn.

## Tạo Dữ Liệu Kiểm Thử

Dự án này cung cấp ba bộ dữ liệu với kích thước khác nhau:

1. `test_data.dat`: Bộ dữ liệu nhỏ có sẵn với 20 bản ghi (mặc định của bài tập).
2. `large_data.dat`: Bộ dữ liệu trung bình với 100 bản ghi (đã được tạo sẵn).
3. `very_large_data.dat`: Bộ dữ liệu lớn với 10,000 bản ghi (cần tạo bằng lệnh).

Để tạo bộ dữ liệu lớn:

```bash
python very_large_data.py
```

## Chạy Kiểm Thử

### 1. Kiểm thử cơ bản (với bộ dữ liệu mặc định):

```bash
python Assignment1Tester.py
```

### 2. Kiểm thử với bộ dữ liệu lớn hơn:

```bash
python LargeDataTester.py
```

### 3. Kiểm thử hiệu suất chi tiết với bộ dữ liệu rất lớn:

```bash
python VeryLargeDataTester.py
```

## Kết Quả Kiểm Thử

Các kết quả kiểm thử được ghi lại trong các tệp nhật ký:

- `detailed_performance.log`: Kết quả của LargeDataTester.py
- `performance_detailed.log`: Kết quả chi tiết của VeryLargeDataTester.py

## Phân Tích Kết Quả

VeryLargeDataTester.py sẽ thực hiện các phân tích hiệu suất chi tiết sau:

1. Đo thời gian và bộ nhớ sử dụng cho mỗi thao tác
2. So sánh hiệu suất phân vùng với các số lượng phân vùng khác nhau (5, 10, 20, 50)
3. Phân tích phân bố dữ liệu trong các phân vùng (độ lệch chuẩn, mức độ mất cân bằng)
4. So sánh hiệu suất chèn dữ liệu giữa Range Partition và Round Robin Partition

## Tùy Chỉnh

Bạn có thể tùy chỉnh các thông số kiểm thử trong các tệp:

- `very_large_data.py`: Số lượng người dùng, phim và đánh giá trong bộ dữ liệu lớn
- `VeryLargeDataTester.py`: Số lượng phân vùng để kiểm thử (PARTITION_COUNTS)
