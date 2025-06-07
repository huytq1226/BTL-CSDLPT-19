import psycopg2
from Interface import getopenconnection, DATABASE_NAME, loadratings, rangepartition, rangeinsert

def setup_test_data():
    """Chuẩn bị dữ liệu kiểm tra mới với các giá trị biên cụ thể"""
    print("===== CHUẨN BỊ DỮ LIỆU KIỂM TRA =====")
    conn = getopenconnection(dbname=DATABASE_NAME)
    cur = conn.cursor()
    
    # Xóa tất cả các bảng hiện có
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    tables = cur.fetchall()
    for table in tables:
        cur.execute(f"DROP TABLE IF EXISTS {table[0]} CASCADE")
    conn.commit()
    
    # Tạo bảng ratings mới
    cur.execute("""
        CREATE TABLE ratings (
            userid INTEGER, 
            movieid INTEGER, 
            rating FLOAT
        )
    """)
    
    # Chèn dữ liệu kiểm tra với các giá trị biên quan trọng
    # Sử dụng các giá trị biên và giá trị giữa các khoảng để kiểm tra kỹ
    test_data = [
        # Giá trị biên chính
        (1, 1, 0.0),   # Biên dưới
        (2, 2, 2.5),   # Biên giữa cho N=2
        (3, 3, 5.0),   # Biên trên
        
        # Giá trị biên với N=3
        (4, 4, 1.67),  # ≈ 5/3
        (5, 5, 3.34),  # ≈ 2*5/3
        
        # Giá trị biên với N=4
        (6, 6, 1.25),  # = 5/4
        (7, 7, 2.5),   # = 2*5/4
        (8, 8, 3.75),  # = 3*5/4
        
        # Giá trị xấp xỉ biên
        (9, 9, 1.66),  # Gần biên 1.67 (N=3)
        (10, 10, 1.68), # Gần biên 1.67 (N=3)
        (11, 11, 2.49), # Gần biên 2.5 (N=2)
        (12, 12, 2.51), # Gần biên 2.5 (N=2)
    ]
    
    # Chèn dữ liệu
    for userid, movieid, rating in test_data:
        cur.execute(f"INSERT INTO ratings VALUES ({userid}, {movieid}, {rating})")
    
    conn.commit()
    print(f"Đã tạo bảng ratings với {len(test_data)} bản ghi kiểm tra")
    
    return conn

def check_table_schema():
    """Kiểm tra lược đồ bảng ratings và các bảng phân vùng"""
    print("\n===== KIỂM TRA LƯỢC ĐỒ BẢNG =====")
    conn = getopenconnection(dbname=DATABASE_NAME)
    cur = conn.cursor()
    
    # Kiểm tra lược đồ bảng ratings
    cur.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'ratings'
        ORDER BY ordinal_position
    """)
    
    print("Lược đồ bảng ratings:")
    schema_ok = True
    for col_name, data_type in cur.fetchall():
        print(f"  {col_name}: {data_type}")
        
        # Kiểm tra kiểu dữ liệu
        if col_name.lower() == 'userid' and data_type != 'integer':
            print(f"  ✗ Lỗi: Cột {col_name} phải có kiểu INTEGER, nhưng là {data_type}")
            schema_ok = False
        elif col_name.lower() == 'movieid' and data_type != 'integer':
            print(f"  ✗ Lỗi: Cột {col_name} phải có kiểu INTEGER, nhưng là {data_type}")
            schema_ok = False
        elif col_name.lower() == 'rating' and data_type not in ['real', 'double precision']:
            print(f"  ✗ Lỗi: Cột {col_name} phải có kiểu FLOAT, nhưng là {data_type}")
            schema_ok = False
    
    if schema_ok:
        print("  ✓ Lược đồ bảng ratings đúng theo yêu cầu")
    
    cur.close()
    conn.close()

def test_range_partition_with_n(n):
    """Kiểm tra cách chia khoảng cho Range Partition với n phân vùng"""
    print(f"\n===== KIỂM TRA RANGE PARTITION VỚI N={n} =====")
    conn = getopenconnection(dbname=DATABASE_NAME)
    
    # Tạo các phân vùng range với n phân vùng
    print(f"Tạo {n} phân vùng range...")
    rangepartition("ratings", n, conn)
    
    # Tính toán độ rộng khoảng lý thuyết
    width = 5.0 / n
    print(f"Độ rộng khoảng lý thuyết: {width}")
    
    # Kiểm tra từng phân vùng
    cur = conn.cursor()
    print("\nKiểm tra phân bố dữ liệu trong các phân vùng:")
    
    for i in range(n):
        min_range = i * width
        max_range = (i + 1) * width
        
        # Phân vùng i nên chứa các giá trị trong khoảng (min_range, max_range]
        # Với phân vùng đầu tiên, khoảng là [0, width]
        if i == 0:
            print(f"  range_part{i} nên chứa các giá trị trong khoảng [0, {max_range}]")
        else:
            print(f"  range_part{i} nên chứa các giá trị trong khoảng ({min_range}, {max_range}]")
        
        # Kiểm tra dữ liệu trong phân vùng
        cur.execute(f"SELECT userid, movieid, rating FROM range_part{i} ORDER BY rating")
        rows = cur.fetchall()
        
        if len(rows) > 0:
            print(f"  Dữ liệu trong range_part{i}:")
            for row in rows:
                userid, movieid, rating = row
                in_range = False
                
                # Kiểm tra xem rating có nằm trong khoảng dự kiến không
                if i == 0 and 0 <= rating <= max_range:
                    in_range = True
                elif i > 0 and min_range < rating <= max_range:
                    in_range = True
                
                status = "✓" if in_range else "✗"
                print(f"    {status} userid={userid}, movieid={movieid}, rating={rating}")
        else:
            print(f"  Không có dữ liệu trong range_part{i}")
    
    # Kiểm tra xử lý các giá trị biên
    print("\nKiểm tra xử lý các giá trị biên:")
    boundary_values = []
    
    # Tạo danh sách các giá trị biên dựa vào n
    for i in range(1, n):
        boundary_values.append(i * width)
    
    for boundary in boundary_values:
        print(f"  Giá trị biên {boundary}:")
        
        # Kiểm tra xem có bản ghi nào có rating = boundary không
        cur.execute(f"SELECT COUNT(*) FROM ratings WHERE rating = {boundary}")
        count = cur.fetchone()[0]
        
        if count > 0:
            # Xác định phân vùng chứa giá trị biên này
            for i in range(n):
                cur.execute(f"SELECT COUNT(*) FROM range_part{i} WHERE rating = {boundary}")
                part_count = cur.fetchone()[0]
                
                if part_count > 0:
                    # Giá trị biên nên nằm trong phân vùng phía dưới (theo quy ước)
                    expected_part = int(boundary / width) - 1
                    if i == expected_part:
                        print(f"    ✓ Giá trị {boundary} nằm trong range_part{i} (đúng theo quy ước)")
                    else:
                        print(f"    ✗ Giá trị {boundary} nằm trong range_part{i}, nhưng nên nằm trong range_part{expected_part}")
    
    # Kiểm tra tổng số bản ghi
    cur.execute("SELECT COUNT(*) FROM ratings")
    total_in_ratings = cur.fetchone()[0]
    
    total_in_parts = 0
    for i in range(n):
        cur.execute(f"SELECT COUNT(*) FROM range_part{i}")
        total_in_parts += cur.fetchone()[0]
    
    print(f"\nTổng số bản ghi trong ratings: {total_in_ratings}")
    print(f"Tổng số bản ghi trong tất cả các phân vùng: {total_in_parts}")
    
    if total_in_ratings == total_in_parts:
        print("✓ Tổng số bản ghi khớp nhau")
    else:
        print("✗ Tổng số bản ghi không khớp nhau")
    
    cur.close()
    conn.close()

def test_boundary_insert():
    """Kiểm tra việc chèn các giá trị biên bằng rangeinsert"""
    print("\n===== KIỂM TRA CHÈN GIÁ TRỊ BIÊN =====")
    conn = getopenconnection(dbname=DATABASE_NAME)
    cur = conn.cursor()
    
    # Xác định số lượng phân vùng hiện tại
    cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE 'range_part%'")
    n = cur.fetchone()[0]
    width = 5.0 / n
    
    print(f"Số lượng phân vùng range hiện tại: {n}")
    print(f"Độ rộng khoảng: {width}")
    
    # Chèn các giá trị biên và xấp xỉ biên
    test_data = []
    
    # Tạo giá trị biên cho từng khoảng
    for i in range(1, n):
        boundary = i * width
        test_data.extend([
            (100 + i*3, 200 + i*3, boundary),         # Đúng giá trị biên
            (100 + i*3 + 1, 200 + i*3 + 1, boundary - 0.01),  # Gần dưới biên
            (100 + i*3 + 2, 200 + i*3 + 2, boundary + 0.01)   # Gần trên biên
        ])
    
    # Thêm các trường hợp đặc biệt
    test_data.extend([
        (150, 250, 0.0),    # Biên dưới
        (151, 251, 5.0)     # Biên trên
    ])
    
    print("\nChèn các giá trị biên bằng rangeinsert:")
    for userid, movieid, rating in test_data:
        rangeinsert("ratings", userid, movieid, rating, conn)
        print(f"  Đã chèn (userid={userid}, movieid={movieid}, rating={rating})")
    
    # Kiểm tra kết quả chèn
    print("\nKiểm tra kết quả chèn:")
    for userid, movieid, rating in test_data:
        # Tìm phân vùng chứa bản ghi vừa chèn
        for i in range(n):
            cur.execute(f"""
                SELECT COUNT(*) FROM range_part{i} 
                WHERE userid = {userid} AND movieid = {movieid} AND rating = {rating}
            """)
            count = cur.fetchone()[0]
            
            if count > 0:
                # Xác định phân vùng lý thuyết dựa vào giá trị rating
                expected_part = 0
                
                # Trường hợp đặc biệt cho giá trị 5.0
                if rating == 5.0:
                    expected_part = n - 1  # Phân vùng cuối cùng theo lý thuyết
                elif rating > 0:
                    expected_part = min(n-1, int(rating / width))
                    # Xử lý trường hợp đặc biệt cho các giá trị biên
                    if rating % width == 0 and rating > 0:
                        expected_part = expected_part - 1
                
                if i == expected_part:
                    print(f"  ✓ Bản ghi (rating={rating}) được chèn vào range_part{i} (đúng)")
                else:
                    print(f"  ✗ Bản ghi (rating={rating}) được chèn vào range_part{i}, nhưng nên nằm trong range_part{expected_part}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    print("Bắt đầu kiểm tra lược đồ bảng và cách chia khoảng cho Range Partition...\n")
    
    # Chuẩn bị dữ liệu kiểm tra
    conn = setup_test_data()
    conn.close()
    
    # Kiểm tra lược đồ bảng
    check_table_schema()
    
    # Kiểm tra cách chia khoảng với các giá trị N khác nhau
    test_range_partition_with_n(2)  # Kiểm tra với 2 phân vùng
    test_range_partition_with_n(3)  # Kiểm tra với 3 phân vùng
    test_range_partition_with_n(4)  # Kiểm tra với 4 phân vùng
    
    # Kiểm tra việc chèn các giá trị biên
    test_boundary_insert()
    
    print("\nĐã hoàn thành kiểm tra!") 