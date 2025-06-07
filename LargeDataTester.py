#
# Tester for large data
#
import psycopg2
import traceback
import testHelper
import Interface as MyAssignment
import time
import logging

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("detailed_performance.log"),
                        logging.StreamHandler()
                    ])

logger = logging.getLogger("LargeDataTester")

DATABASE_NAME = 'dds_assgn1_large'

# Thông số cấu hình
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

# Sử dụng tệp dữ liệu lớn hơn
INPUT_FILE_PATH = 'large_data.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 100  # Số dòng trong tệp dữ liệu lớn

# Số phân vùng để thử nghiệm
PARTITION_COUNT = 10  # Thử nghiệm với nhiều phân vùng hơn

def test_with_timing(func_name, func, *args, **kwargs):
    """Thực thi một hàm và đo thời gian thực thi."""
    logger.info(f"Bắt đầu thực thi {func_name}...")
    start_time = time.time()
    
    try:
        result = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        logger.info(f"Hoàn thành {func_name} trong {elapsed_time:.4f} giây")
        return True, result, elapsed_time
    except Exception as e:
        elapsed_time = time.time() - start_time
        logger.error(f"Lỗi trong {func_name} sau {elapsed_time:.4f} giây: {str(e)}")
        traceback.print_exc()
        return False, e, elapsed_time

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
    logger.info(f"Độ lệch chuẩn: {(sum((x - avg) ** 2 for x in partition_counts) / number_of_partitions) ** 0.5:.2f}")
    
    return partition_counts

if __name__ == '__main__':
    logger.info(f"=== Bắt đầu kiểm thử với tệp dữ liệu lớn: {INPUT_FILE_PATH} ===")
    logger.info(f"Số dòng dự kiến: {ACTUAL_ROWS_IN_INPUT_FILE}")
    logger.info(f"Số phân vùng: {PARTITION_COUNT}")
    
    try:
        # Tạo cơ sở dữ liệu mới
        test_with_timing("createdb", testHelper.createdb, DATABASE_NAME)
        
        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            
            # Xóa tất cả bảng hiện có
            testHelper.deleteAllPublicTables(conn)
            
            # Kiểm tra loadratings
            success, result, elapsed_time = test_with_timing(
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
            else:
                logger.error("loadratings: FAIL")
            
            # Kiểm tra rangepartition
            success, result, elapsed_time = test_with_timing(
                "rangepartition", 
                MyAssignment.rangepartition, 
                RATINGS_TABLE, 
                PARTITION_COUNT, 
                conn
            )
            
            if success:
                logger.info("Phân tích phân vùng dựa trên khoảng giá trị:")
                analyze_partitions(conn, RANGE_TABLE_PREFIX, PARTITION_COUNT)
                logger.info("rangepartition: PASS")
            else:
                logger.error("rangepartition: FAIL")
            
            # Kiểm tra rangeinsert
            success, result, elapsed_time = test_with_timing(
                "rangeinsert", 
                MyAssignment.rangeinsert, 
                RATINGS_TABLE, 
                101, 
                201, 
                3.5, 
                conn
            )
            
            if success:
                logger.info("rangeinsert: PASS")
            else:
                logger.error("rangeinsert: FAIL")
            
            # Xóa tất cả bảng để chuẩn bị cho kiểm thử roundrobin
            testHelper.deleteAllPublicTables(conn)
            
            # Tải lại dữ liệu
            success, result, elapsed_time = test_with_timing(
                "loadratings (for roundrobin test)", 
                MyAssignment.loadratings, 
                RATINGS_TABLE, 
                INPUT_FILE_PATH, 
                conn
            )
            
            # Kiểm tra roundrobinpartition
            success, result, elapsed_time = test_with_timing(
                "roundrobinpartition", 
                MyAssignment.roundrobinpartition, 
                RATINGS_TABLE, 
                PARTITION_COUNT, 
                conn
            )
            
            if success:
                logger.info("Phân tích phân vùng round robin:")
                analyze_partitions(conn, RROBIN_TABLE_PREFIX, PARTITION_COUNT)
                logger.info("roundrobinpartition: PASS")
            else:
                logger.error("roundrobinpartition: FAIL")
            
            # Kiểm tra roundrobininsert
            success, result, elapsed_time = test_with_timing(
                "roundrobininsert", 
                MyAssignment.roundrobininsert, 
                RATINGS_TABLE, 
                101, 
                201, 
                3.5, 
                conn
            )
            
            if success:
                logger.info("roundrobininsert: PASS")
            else:
                logger.error("roundrobininsert: FAIL")
            
            # Thực hiện nhiều thao tác chèn để kiểm tra hiệu suất
            logger.info("Kiểm tra hiệu suất với nhiều thao tác chèn liên tiếp:")
            
            # Range insert performance test
            try:
                # Kiểm tra xem đã có phân vùng range chưa
                if count_records_in_table(conn, f"{RANGE_TABLE_PREFIX}0") > 0:
                    start_time = time.time()
                    for i in range(50):
                        MyAssignment.rangeinsert(RATINGS_TABLE, 200+i, 300+i, (i % 50) / 10.0, conn)
                    elapsed_time = time.time() - start_time
                    logger.info(f"50 range inserts: {elapsed_time:.4f} giây ({elapsed_time/50:.4f} giây/thao tác)")
                else:
                    logger.warning("Không tìm thấy phân vùng range. Thực hiện rangepartition trước khi test rangeinsert.")
                    # Thực hiện rangepartition trước
                    logger.info("Thực hiện rangepartition với 5 phân vùng...")
                    MyAssignment.rangepartition(RATINGS_TABLE, 5, conn)
                    
                    start_time = time.time()
                    for i in range(50):
                        MyAssignment.rangeinsert(RATINGS_TABLE, 200+i, 300+i, (i % 50) / 10.0, conn)
                    elapsed_time = time.time() - start_time
                    logger.info(f"50 range inserts: {elapsed_time:.4f} giây ({elapsed_time/50:.4f} giây/thao tác)")
            except Exception as e:
                logger.error(f"Lỗi khi thực hiện range inserts: {str(e)}")
            
            # Round robin insert performance test
            try:
                # Kiểm tra xem đã có phân vùng round robin chưa
                if count_records_in_table(conn, f"{RROBIN_TABLE_PREFIX}0") > 0:
                    start_time = time.time()
                    for i in range(50):
                        MyAssignment.roundrobininsert(RATINGS_TABLE, 300+i, 400+i, (i % 50) / 10.0, conn)
                    elapsed_time = time.time() - start_time
                    logger.info(f"50 round robin inserts: {elapsed_time:.4f} giây ({elapsed_time/50:.4f} giây/thao tác)")
                else:
                    logger.warning("Không tìm thấy phân vùng round robin. Thực hiện roundrobinpartition trước khi test roundrobininsert.")
                    # Thực hiện roundrobinpartition trước
                    logger.info("Thực hiện roundrobinpartition với 5 phân vùng...")
                    MyAssignment.roundrobinpartition(RATINGS_TABLE, 5, conn)
                    
                    start_time = time.time()
                    for i in range(50):
                        MyAssignment.roundrobininsert(RATINGS_TABLE, 300+i, 400+i, (i % 50) / 10.0, conn)
                    elapsed_time = time.time() - start_time
                    logger.info(f"50 round robin inserts: {elapsed_time:.4f} giây ({elapsed_time/50:.4f} giây/thao tác)")
            except Exception as e:
                logger.error(f"Lỗi khi thực hiện round robin inserts: {str(e)}")
            
            choice = input('Nhấn Enter để xóa tất cả bảng hoặc nhập "n" để giữ lại: ')
            if choice.lower() != 'n':
                testHelper.deleteAllPublicTables(conn)
                logger.info("Đã xóa tất cả bảng")
            else:
                logger.info("Giữ lại tất cả bảng để kiểm tra thủ công")
    
    except Exception as e:
        logger.error(f"Lỗi không mong muốn: {str(e)}")
        traceback.print_exc()
    
    logger.info("=== Kết thúc kiểm thử ===") 