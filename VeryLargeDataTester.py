#
# Tester for very large data - Performance Testing
#
import psycopg2
import traceback
import testHelper
import Interface as MyAssignment
import time
import logging
import os
import sys
import psutil
import gc

# Cấu hình logging chi tiết hơn
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("performance_detailed.log"),
                        logging.StreamHandler()
                    ])

logger = logging.getLogger("VeryLargeDataTester")

DATABASE_NAME = 'dds_assgn1_verylarge'

# Thông số cấu hình
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

# Sử dụng tệp dữ liệu rất lớn
INPUT_FILE_PATH = 'very_large_data.dat'

# Số phân vùng để thử nghiệm - thử với nhiều giá trị khác nhau
PARTITION_COUNTS = [5, 10, 20, 50]

def get_memory_usage():
    """Lấy thông tin sử dụng bộ nhớ hiện tại của tiến trình."""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    return memory_info.rss / 1024 / 1024  # Convert to MB

def test_with_timing_and_memory(func_name, func, *args, **kwargs):
    """Thực thi một hàm và đo thời gian thực thi và bộ nhớ sử dụng."""
    # Thu gom rác trước khi đo
    gc.collect()
    
    # Đo bộ nhớ ban đầu
    memory_before = get_memory_usage()
    logger.info(f"Bắt đầu thực thi {func_name}... (Bộ nhớ ban đầu: {memory_before:.2f} MB)")
    
    start_time = time.time()
    
    try:
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        
        # Đo bộ nhớ sau khi thực thi
        memory_after = get_memory_usage()
        memory_diff = memory_after - memory_before
        
        logger.info(f"Hoàn thành {func_name} trong {elapsed_time:.4f} giây")
        logger.info(f"Bộ nhớ sau khi thực thi: {memory_after:.2f} MB (Thay đổi: {memory_diff:+.2f} MB)")
        
        return True, result, elapsed_time, memory_diff
    except Exception as e:
        elapsed_time = time.time() - start_time
        memory_after = get_memory_usage()
        memory_diff = memory_after - memory_before
        
        logger.error(f"Lỗi trong {func_name} sau {elapsed_time:.4f} giây")
        logger.error(f"Bộ nhớ sau khi thực thi: {memory_after:.2f} MB (Thay đổi: {memory_diff:+.2f} MB)")
        logger.error(f"Lỗi: {str(e)}")
        traceback.print_exc()
        
        return False, e, elapsed_time, memory_diff

def count_records_in_table(conn, table_name):
    """Đếm số lượng bản ghi trong bảng."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        return cur.fetchone()[0]

def analyze_partitions(conn, prefix, number_of_partitions):
    """Phân tích phân bố dữ liệu trong các phân vùng."""
    partition_counts = []
    
    with conn.cursor() as cur:
        for i in range(number_of_partitions):
            table_name = f"{prefix}{i}"
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()[0]
            partition_counts.append(count)
            logger.info(f"Phân vùng {table_name}: {count} bản ghi")
    
    total = sum(partition_counts)
    avg = total / number_of_partitions
    max_count = max(partition_counts)
    min_count = min(partition_counts)
    
    logger.info(f"Tổng số bản ghi trong tất cả phân vùng: {total}")
    logger.info(f"Trung bình số bản ghi mỗi phân vùng: {avg:.2f}")
    logger.info(f"Phân vùng lớn nhất: {max_count} bản ghi")
    logger.info(f"Phân vùng nhỏ nhất: {min_count} bản ghi")
    
    # Tính độ lệch chuẩn
    std_dev = (sum((x - avg) ** 2 for x in partition_counts) / number_of_partitions) ** 0.5
    logger.info(f"Độ lệch chuẩn: {std_dev:.2f}")
    
    # Tính phần trăm chênh lệch giữa phân vùng lớn nhất và nhỏ nhất
    if min_count > 0:
        imbalance = (max_count - min_count) / min_count * 100
        logger.info(f"Mức độ mất cân bằng: {imbalance:.2f}%")
    
    return partition_counts

def test_multiple_inserts(conn, num_inserts, insert_func, table_name, start_id=1000):
    """Kiểm tra hiệu suất với nhiều thao tác chèn liên tiếp."""
    start_time = time.time()
    memory_before = get_memory_usage()
    
    for i in range(num_inserts):
        user_id = start_id + i
        movie_id = start_id + 1000 + i
        rating = (i % 50) / 10.0  # Ratings from 0.0 to 4.9 với bước 0.1
        
        insert_func(table_name, user_id, movie_id, rating, conn)
        
        if (i + 1) % 10 == 0:
            logger.info(f"Đã chèn {i + 1}/{num_inserts} bản ghi")
    
    elapsed_time = time.time() - start_time
    memory_after = get_memory_usage()
    memory_diff = memory_after - memory_before
    
    logger.info(f"{num_inserts} lần chèn: {elapsed_time:.4f} giây ({elapsed_time/num_inserts:.6f} giây/thao tác)")
    logger.info(f"Thay đổi bộ nhớ: {memory_diff:+.2f} MB")
    
    return elapsed_time, memory_diff

def test_partition_performance(partition_type, partition_counts):
    """Kiểm tra hiệu suất phân vùng với nhiều cấu hình khác nhau."""
    logger.info(f"=== Kiểm tra hiệu suất phân vùng {partition_type} với các cấu hình khác nhau ===")
    
    results = []
    
    # Xác định hàm phân vùng và tiền tố bảng
    if partition_type == "range":
        partition_func = MyAssignment.rangepartition
        prefix = RANGE_TABLE_PREFIX
    else:  # "roundrobin"
        partition_func = MyAssignment.roundrobinpartition
        prefix = RROBIN_TABLE_PREFIX
    
    try:
        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            
            # Đếm số bản ghi trong bảng ratings
            total_records = count_records_in_table(conn, RATINGS_TABLE)
            logger.info(f"Tổng số bản ghi trong bảng {RATINGS_TABLE}: {total_records}")
            
            for num_partitions in partition_counts:
                logger.info(f"\nThử nghiệm với {num_partitions} phân vùng:")
                
                # Xóa tất cả bảng trước khi thử nghiệm
                testHelper.deleteAllPublicTables(conn)
                
                # Tải lại dữ liệu
                logger.info(f"Tải lại dữ liệu vào bảng {RATINGS_TABLE}...")
                MyAssignment.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)
                
                # Thực hiện phân vùng và đo thời gian
                success, _, elapsed_time, memory_diff = test_with_timing_and_memory(
                    f"{partition_type}partition with {num_partitions} partitions",
                    partition_func,
                    RATINGS_TABLE,
                    num_partitions,
                    conn
                )
                
                if success:
                    # Phân tích kết quả phân vùng
                    logger.info(f"Phân tích phân vùng với {num_partitions} phân vùng:")
                    partition_counts = analyze_partitions(conn, prefix, num_partitions)
                    
                    # Lưu kết quả để so sánh
                    results.append({
                        "partition_type": partition_type,
                        "num_partitions": num_partitions,
                        "elapsed_time": elapsed_time,
                        "memory_diff": memory_diff,
                        "max_count": max(partition_counts),
                        "min_count": min(partition_counts),
                        "std_dev": (sum((x - (total_records / num_partitions)) ** 2 for x in partition_counts) / num_partitions) ** 0.5
                    })
                else:
                    logger.error(f"Phân vùng với {num_partitions} thất bại")
    
    except Exception as e:
        logger.error(f"Lỗi trong quá trình kiểm tra hiệu suất: {str(e)}")
        traceback.print_exc()
    
    # Hiển thị bảng so sánh kết quả
    if results:
        logger.info("\n=== Bảng so sánh hiệu suất ===")
        logger.info(f"{'Loại':10} {'Số lượng':10} {'Thời gian (s)':15} {'Bộ nhớ (MB)':15} {'Độ lệch chuẩn':15}")
        logger.info("-" * 65)
        
        for r in results:
            logger.info(f"{r['partition_type']:10} {r['num_partitions']:10} {r['elapsed_time']:15.4f} {r['memory_diff']:15.2f} {r['std_dev']:15.2f}")
    
    return results

if __name__ == '__main__':
    # Kiểm tra xem tệp dữ liệu rất lớn có tồn tại không
    if not os.path.exists(INPUT_FILE_PATH):
        logger.error(f"Tệp dữ liệu {INPUT_FILE_PATH} không tồn tại. Hãy chạy very_large_data.py trước.")
        logger.info("Sử dụng lệnh: python very_large_data.py")
        sys.exit(1)
    
    # Đếm số dòng trong tệp
    with open(INPUT_FILE_PATH, 'r') as f:
        ACTUAL_ROWS_IN_INPUT_FILE = sum(1 for _ in f)
    
    logger.info(f"=== Bắt đầu kiểm thử hiệu suất với tệp dữ liệu lớn: {INPUT_FILE_PATH} ===")
    logger.info(f"Số dòng trong tệp: {ACTUAL_ROWS_IN_INPUT_FILE}")
    logger.info(f"Kích thước tệp: {os.path.getsize(INPUT_FILE_PATH) / 1024:.2f} KB")
    
    try:
        # Tạo cơ sở dữ liệu mới
        test_with_timing_and_memory("createdb", testHelper.createdb, DATABASE_NAME)
        
        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            
            # Xóa tất cả bảng hiện có
            testHelper.deleteAllPublicTables(conn)
            
            # Kiểm tra loadratings
            success, _, elapsed_time, memory_diff = test_with_timing_and_memory(
                "loadratings", 
                MyAssignment.loadratings, 
                RATINGS_TABLE, 
                INPUT_FILE_PATH, 
                conn
            )
            
            if success:
                row_count = count_records_in_table(conn, RATINGS_TABLE)
                logger.info(f"Bảng {RATINGS_TABLE} có {row_count} bản ghi")
                
                if row_count == ACTUAL_ROWS_IN_INPUT_FILE:
                    logger.info("loadratings: PASS - Đã tải đúng số lượng bản ghi")
                else:
                    logger.warning(f"loadratings: WARN - Số bản ghi ({row_count}) khác với mong đợi ({ACTUAL_ROWS_IN_INPUT_FILE})")
                
                # Kiểm tra hiệu suất Range Partition với nhiều cấu hình
                range_results = test_partition_performance("range", PARTITION_COUNTS)
                
                # Kiểm tra hiệu suất Round Robin Partition với nhiều cấu hình
                rrobin_results = test_partition_performance("roundrobin", PARTITION_COUNTS)
                
                # Kiểm tra hiệu suất chèn dữ liệu
                logger.info("\n=== Kiểm tra hiệu suất chèn dữ liệu ===")
                
                # Tải lại dữ liệu
                testHelper.deleteAllPublicTables(conn)
                MyAssignment.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)
                
                # Phân vùng với cấu hình tốt nhất (dựa trên kết quả trước đó)
                optimal_partition_count = 10  # Giả sử đây là cấu hình tốt nhất
                
                try:
                    # Range Partition
                    logger.info(f"Thực hiện rangepartition với {optimal_partition_count} phân vùng...")
                    MyAssignment.rangepartition(RATINGS_TABLE, optimal_partition_count, conn)
                    
                    # Kiểm tra xem phân vùng đã được tạo thành công chưa
                    range_partition_count = 0
                    try:
                        with conn.cursor() as cur:
                            cur.execute(f"SELECT COUNT(*) FROM {RANGE_TABLE_PREFIX}0")
                            range_partition_count = cur.fetchone()[0]
                    except:
                        pass
                    
                    if range_partition_count > 0:
                        logger.info("\nHiệu suất Range Insert:")
                        range_insert_time, range_insert_memory = test_multiple_inserts(
                            conn, 100, MyAssignment.rangeinsert, RATINGS_TABLE, 2000
                        )
                    else:
                        logger.error("Không thể tạo phân vùng range. Bỏ qua kiểm tra range insert.")
                except Exception as e:
                    logger.error(f"Lỗi khi thực hiện range partition và insert: {str(e)}")
                    range_insert_time = None
                    range_insert_memory = None
                
                # Tải lại dữ liệu
                testHelper.deleteAllPublicTables(conn)
                MyAssignment.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)
                
                try:
                    # Round Robin Partition
                    logger.info(f"Thực hiện roundrobinpartition với {optimal_partition_count} phân vùng...")
                    MyAssignment.roundrobinpartition(RATINGS_TABLE, optimal_partition_count, conn)
                    
                    # Kiểm tra xem phân vùng đã được tạo thành công chưa
                    rrobin_partition_count = 0
                    try:
                        with conn.cursor() as cur:
                            cur.execute(f"SELECT COUNT(*) FROM {RROBIN_TABLE_PREFIX}0")
                            rrobin_partition_count = cur.fetchone()[0]
                    except:
                        pass
                    
                    if rrobin_partition_count > 0:
                        logger.info("\nHiệu suất Round Robin Insert:")
                        rrobin_insert_time, rrobin_insert_memory = test_multiple_inserts(
                            conn, 100, MyAssignment.roundrobininsert, RATINGS_TABLE, 3000
                        )
                    else:
                        logger.error("Không thể tạo phân vùng round robin. Bỏ qua kiểm tra round robin insert.")
                except Exception as e:
                    logger.error(f"Lỗi khi thực hiện round robin partition và insert: {str(e)}")
                    rrobin_insert_time = None
                    rrobin_insert_memory = None
                
                # So sánh hiệu suất chèn
                if range_insert_time is not None and rrobin_insert_time is not None:
                    logger.info("\n=== So sánh hiệu suất chèn ===")
                    logger.info(f"Range Insert: {range_insert_time:.4f} giây, {range_insert_memory:.2f} MB")
                    logger.info(f"Round Robin Insert: {rrobin_insert_time:.4f} giây, {rrobin_insert_memory:.2f} MB")
                    
                    if range_insert_time < rrobin_insert_time:
                        logger.info(f"Range Insert nhanh hơn {(rrobin_insert_time/range_insert_time - 1)*100:.2f}%")
                    else:
                        logger.info(f"Round Robin Insert nhanh hơn {(range_insert_time/rrobin_insert_time - 1)*100:.2f}%")
                else:
                    logger.warning("Không thể so sánh hiệu suất do một hoặc cả hai phương pháp insert thất bại.")
            
            choice = input('Nhấn Enter để xóa tất cả bảng hoặc nhập "n" để giữ lại: ')
            if choice.lower() != 'n':
                testHelper.deleteAllPublicTables(conn)
                logger.info("Đã xóa tất cả bảng")
            else:
                logger.info("Giữ lại tất cả bảng để kiểm tra thủ công")
    
    except Exception as e:
        logger.error(f"Lỗi không mong muốn: {str(e)}")
        traceback.print_exc()
    
    logger.info("=== Kết thúc kiểm thử hiệu suất ===") 