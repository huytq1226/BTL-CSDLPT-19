#!/usr/bin/env python3
# Tạo tệp dữ liệu lớn để kiểm thử hiệu năng

import random
import time
import os

# Cấu hình
OUTPUT_FILE = "very_large_data.dat"
NUM_USERS = 50         # Số lượng người dùng
NUM_MOVIES = 100       # Số lượng phim
NUM_RATINGS = 10000    # Tổng số lượng đánh giá cần tạo

def generate_large_dataset():
    print(f"Bắt đầu tạo tệp dữ liệu lớn với {NUM_RATINGS} đánh giá...")
    start_time = time.time()
    
    # Tạo danh sách đánh giá có thể (chuỗi decimals từ 0.0 đến 5.0 với bước 0.5)
    possible_ratings = [i/2 for i in range(11)]  # [0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
    
    # Tạo timestamp cơ sở
    base_timestamp = int(time.time()) - 365 * 24 * 60 * 60  # 1 năm trước
    
    # Mở tệp để ghi
    with open(OUTPUT_FILE, "w") as f:
        for i in range(NUM_RATINGS):
            user_id = random.randint(1, NUM_USERS)
            movie_id = random.randint(1, NUM_MOVIES)
            rating = random.choice(possible_ratings)
            timestamp = random.randint(base_timestamp, base_timestamp + 365 * 24 * 60 * 60)
            
            # Ghi dòng theo định dạng của tệp input
            f.write(f"{user_id}::{movie_id}::{rating}::{timestamp}\n")
            
            # Hiển thị tiến độ
            if (i + 1) % 1000 == 0:
                print(f"Đã tạo {i + 1}/{NUM_RATINGS} đánh giá ({(i + 1) / NUM_RATINGS * 100:.1f}%)")
    
    # Hiển thị thống kê
    elapsed_time = time.time() - start_time
    file_size = os.path.getsize(OUTPUT_FILE) / 1024  # kích thước theo KB
    
    print(f"\nHoàn thành tạo tệp dữ liệu trong {elapsed_time:.2f} giây")
    print(f"Tệp đầu ra: {OUTPUT_FILE}")
    print(f"Kích thước tệp: {file_size:.2f} KB")
    print(f"Số lượng đánh giá: {NUM_RATINGS}")

if __name__ == "__main__":
    generate_large_dataset() 