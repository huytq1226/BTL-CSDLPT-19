import psycopg2
from Interface import getopenconnection, DATABASE_NAME, loadratings, rangepartition, roundrobinpartition, rangeinsert, roundrobininsert

def setup_database():
    """Chuẩn bị dữ liệu cho việc kiểm tra"""
    print("===== CHUẨN BỊ DỮ LIỆU =====")
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
    
    # Tải dữ liệu vào bảng ratings
    print("Đang tải dữ liệu vào bảng ratings...")
    loadratings("ratings", "test_data.dat", conn)
    
    # Tạo phân vùng range với 5 phân vùng
    print("Đang tạo phân vùng range...")
    rangepartition("ratings", 5, conn)
    
    # Tạo phân vùng round robin với 5 phân vùng
    print("Đang tạo phân vùng round robin...")
    roundrobinpartition("ratings", 5, conn)
    
    cur.close()
    conn.commit()
    print("Đã chuẩn bị xong dữ liệu!\n")
    return conn

def check_partitioning(conn):
    """Kiểm tra tính chính xác của việc phân vùng dữ liệu"""
    cur = conn.cursor()
    
    # Kiểm tra số lượng bản ghi trong bảng gốc
    print("===== KIỂM TRA PHÂN VÙNG RANGE =====")
    cur.execute("SELECT COUNT(*) FROM ratings")
    total_records = cur.fetchone()[0]
    print(f"Tổng số bản ghi trong bảng 'ratings': {total_records}")
    
    # Kiểm tra các bảng phân vùng range
    total_in_range_parts = 0
    cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE 'range_part%'")
    num_parts = cur.fetchone()[0]
    print(f"Số lượng bảng phân vùng range: {num_parts}")
    
    # Kiểm tra từng bảng phân vùng range
    for i in range(num_parts):
        table_name = f"range_part{i}"
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        part_count = cur.fetchone()[0]
        total_in_range_parts += part_count
        print(f"Số bản ghi trong {table_name}: {part_count}")
        
        # Kiểm tra phạm vi rating trong mỗi bảng phân vùng
        cur.execute(f"SELECT MIN(rating), MAX(rating) FROM {table_name}")
        min_rating, max_rating = cur.fetchone()
        print(f"  Phạm vi rating: {min_rating} đến {max_rating}")
    
    # So sánh tổng số bản ghi
    print(f"Tổng số bản ghi trong tất cả các bảng phân vùng range: {total_in_range_parts}")
    if total_records == total_in_range_parts:
        print("✓ KIỂM TRA THÀNH CÔNG: Tổng số bản ghi trong các bảng phân vùng range bằng với bảng gốc")
    else:
        print("✗ KIỂM TRA THẤT BẠI: Tổng số bản ghi trong các bảng phân vùng range KHÔNG bằng với bảng gốc")
    
    # Kiểm tra các bảng phân vùng round robin
    print("\n===== KIỂM TRA PHÂN VÙNG ROUND ROBIN =====")
    total_in_rrobin_parts = 0
    cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE 'rrobin_part%'")
    num_parts = cur.fetchone()[0]
    print(f"Số lượng bảng phân vùng round robin: {num_parts}")
    
    # Kiểm tra từng bảng phân vùng round robin
    for i in range(num_parts):
        table_name = f"rrobin_part{i}"
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        part_count = cur.fetchone()[0]
        total_in_rrobin_parts += part_count
        print(f"Số bản ghi trong {table_name}: {part_count}")
    
    # So sánh tổng số bản ghi
    print(f"Tổng số bản ghi trong tất cả các bảng phân vùng round robin: {total_in_rrobin_parts}")
    if total_records == total_in_rrobin_parts:
        print("✓ KIỂM TRA THÀNH CÔNG: Tổng số bản ghi trong các bảng phân vùng round robin bằng với bảng gốc")
    else:
        print("✗ KIỂM TRA THẤT BẠI: Tổng số bản ghi trong các bảng phân vùng round robin KHÔNG bằng với bảng gốc")
    
    # Kiểm tra biên
    print("\n===== KIỂM TRA GIÁ TRỊ BIÊN =====")
    # Kiểm tra giá trị biên (ví dụ: 1.0, 2.0, 3.0, 4.0, 5.0)
    boundary_values = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
    for val in boundary_values:
        cur.execute(f"SELECT COUNT(*) FROM ratings WHERE rating = {val}")
        count = cur.fetchone()[0]
        if count > 0:
            print(f"Giá trị biên {val}:")
            # Tìm trong các bảng phân vùng range
            for i in range(num_parts):
                table_name = f"range_part{i}"
                cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE rating = {val}")
                part_count = cur.fetchone()[0]
                if part_count > 0:
                    print(f"  ✓ Nằm trong bảng {table_name}: {part_count} bản ghi")
    
    cur.close()
    return num_parts

def check_insert_functionality(conn, num_range_parts, num_rrobin_parts):
    """Kiểm tra chức năng chèn hoạt động nhiều lần"""
    print("\n===== KIỂM TRA CHỨC NĂNG CHÈN =====")
    
    # 1. Đếm số bản ghi trước khi chèn
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM ratings")
    count_before = cur.fetchone()[0]
    print(f"Số bản ghi trong bảng ratings trước khi chèn: {count_before}")
    
    # Đếm số bản ghi trong các bảng phân vùng
    range_counts_before = []
    for i in range(num_range_parts):
        table_name = f"range_part{i}"
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        range_counts_before.append(cur.fetchone()[0])
    
    rrobin_counts_before = []
    for i in range(num_rrobin_parts):
        table_name = f"rrobin_part{i}"
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        rrobin_counts_before.append(cur.fetchone()[0])
    
    # 2. Thực hiện một số thao tác chèn
    print("\nThực hiện thao tác chèn...")
    # Chèn 3 bản ghi sử dụng rangeinsert với các giá trị rating khác nhau
    test_data = [
        (101, 201, 1.5),  # Nên nằm ở phân vùng đầu tiên hoặc thứ hai
        (102, 202, 3.5),  # Nên nằm ở phân vùng giữa
        (103, 203, 5.0)   # Nên nằm ở phân vùng cuối cùng
    ]
    
    for userid, movieid, rating in test_data:
        rangeinsert("ratings", userid, movieid, rating, conn)
        print(f"  Đã chèn bản ghi (userid={userid}, movieid={movieid}, rating={rating}) bằng rangeinsert")
    
    # Chèn 3 bản ghi sử dụng roundrobininsert
    test_data = [
        (104, 204, 2.5),
        (105, 205, 3.5),
        (106, 206, 4.5)
    ]
    
    for userid, movieid, rating in test_data:
        roundrobininsert("ratings", userid, movieid, rating, conn)
        print(f"  Đã chèn bản ghi (userid={userid}, movieid={movieid}, rating={rating}) bằng roundrobininsert")
    
    # 3. Đếm số bản ghi sau khi chèn
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM ratings")
    count_after = cur.fetchone()[0]
    print(f"\nSố bản ghi trong bảng ratings sau khi chèn: {count_after}")
    print(f"Số bản ghi đã thêm: {count_after - count_before}")
    
    # Kiểm tra các bảng phân vùng
    print("\nKiểm tra các bảng phân vùng range sau khi chèn:")
    for i in range(num_range_parts):
        table_name = f"range_part{i}"
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_after = cur.fetchone()[0]
        diff = count_after - range_counts_before[i]
        print(f"  {table_name}: {count_after} bản ghi (thêm {diff})")
    
    print("\nKiểm tra các bảng phân vùng round robin sau khi chèn:")
    for i in range(num_rrobin_parts):
        table_name = f"rrobin_part{i}"
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count_after = cur.fetchone()[0]
        diff = count_after - rrobin_counts_before[i]
        print(f"  {table_name}: {count_after} bản ghi (thêm {diff})")
    
    # 4. Kiểm tra dữ liệu đã chèn
    print("\nKiểm tra dữ liệu đã chèn:")
    
    # Kiểm tra rangeinsert
    for userid, movieid, rating in [(101, 201, 1.5), (102, 202, 3.5), (103, 203, 5.0)]:
        cur.execute(f"SELECT COUNT(*) FROM ratings WHERE userid = {userid} AND movieid = {movieid} AND rating = {rating}")
        found_in_ratings = cur.fetchone()[0] > 0
        
        found_parts = []
        for i in range(num_range_parts):
            table_name = f"range_part{i}"
            cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE userid = {userid} AND movieid = {movieid} AND rating = {rating}")
            if cur.fetchone()[0] > 0:
                found_parts.append(table_name)
        
        if found_in_ratings and len(found_parts) == 1:
            print(f"  ✓ Bản ghi (userid={userid}, movieid={movieid}, rating={rating}) được chèn vào bảng ratings và {found_parts[0]}")
        else:
            print(f"  ✗ Vấn đề với bản ghi (userid={userid}, movieid={movieid}, rating={rating})")
    
    # Kiểm tra roundrobininsert
    for userid, movieid, rating in [(104, 204, 2.5), (105, 205, 3.5), (106, 206, 4.5)]:
        cur.execute(f"SELECT COUNT(*) FROM ratings WHERE userid = {userid} AND movieid = {movieid} AND rating = {rating}")
        found_in_ratings = cur.fetchone()[0] > 0
        
        found_parts = []
        for i in range(num_rrobin_parts):
            table_name = f"rrobin_part{i}"
            cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE userid = {userid} AND movieid = {movieid} AND rating = {rating}")
            if cur.fetchone()[0] > 0:
                found_parts.append(table_name)
        
        if found_in_ratings and len(found_parts) == 1:
            print(f"  ✓ Bản ghi (userid={userid}, movieid={movieid}, rating={rating}) được chèn vào bảng ratings và {found_parts[0]}")
        else:
            print(f"  ✗ Vấn đề với bản ghi (userid={userid}, movieid={movieid}, rating={rating})")
    
    cur.close()

if __name__ == "__main__":
    print("Bắt đầu kiểm tra phân vùng và chức năng chèn...\n")
    
    # Chuẩn bị dữ liệu
    conn = setup_database()
    
    # Kiểm tra phân vùng
    num_range_parts = check_partitioning(conn)
    
    # Lấy số lượng phân vùng round robin
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE 'rrobin_part%'")
    num_rrobin_parts = cur.fetchone()[0]
    cur.close()
    
    # Kiểm tra chức năng chèn
    check_insert_functionality(conn, num_range_parts, num_rrobin_parts)
    
    conn.close()
    print("\nĐã hoàn thành kiểm tra!") 