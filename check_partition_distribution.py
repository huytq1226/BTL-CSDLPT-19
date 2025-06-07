import psycopg2
import logging
import sys
import argparse
from Interface import getopenconnection, DATABASE_NAME

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("partition_distribution.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

def check_partition_distribution(table_prefix, num_partitions, openconnection):
    """
    Kiểm tra và hiển thị phân phối dữ liệu trong các phân vùng
    
    Args:
        table_prefix: Tiền tố của bảng phân vùng ('range_part' hoặc 'rrobin_part')
        num_partitions: Số lượng phân vùng cần kiểm tra
        openconnection: Kết nối đến database
    """
    conn = openconnection
    cur = conn.cursor()
    
    logging.info(f"Kiểm tra phân phối dữ liệu cho {num_partitions} phân vùng với tiền tố '{table_prefix}'")
    
    # Thu thập thông tin về các bảng
    partition_data = []
    total_records = 0
    
    for i in range(num_partitions):
        table_name = f"{table_prefix}{i}"
        
        try:
            # Kiểm tra xem bảng có tồn tại không
            cur.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                )
            """)
            
            if cur.fetchone()[0]:
                # Đếm số bản ghi
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cur.fetchone()[0]
                total_records += count
                
                # Nếu là range partition, lấy thêm thông tin về phạm vi
                if table_prefix == 'range_part':
                    cur.execute(f"SELECT MIN(rating), MAX(rating) FROM {table_name}")
                    min_rating, max_rating = cur.fetchone()
                    partition_data.append((i, count, min_rating, max_rating))
                else:
                    partition_data.append((i, count, None, None))
            else:
                logging.warning(f"Bảng {table_name} không tồn tại")
                partition_data.append((i, 0, None, None))
        except Exception as e:
            logging.error(f"Lỗi khi kiểm tra {table_name}: {str(e)}")
            partition_data.append((i, 0, None, None))
    
    # Hiển thị kết quả
    if not partition_data:
        logging.warning(f"Không tìm thấy phân vùng nào với tiền tố '{table_prefix}'")
        return
    
    # Sắp xếp theo số lượng bản ghi (giảm dần)
    partition_data.sort(key=lambda x: x[1], reverse=True)
    
    # In kết quả
    logging.info(f"Tổng cộng: {total_records} bản ghi trong {len(partition_data)} phân vùng")
    logging.info(f"{'Index':<10} {'Số bản ghi':<15} {'% Tổng':<10} {'Min Rating':<15} {'Max Rating':<15}")
    logging.info("-" * 65)
    
    for data in partition_data:
        idx, count, min_rating, max_rating = data
        percentage = (count / total_records * 100) if total_records > 0 else 0
        
        if min_rating is not None and max_rating is not None:
            logging.info(f"{idx:<10} {count:<15} {percentage:<10.2f} {min_rating:<15} {max_rating:<15}")
        else:
            logging.info(f"{idx:<10} {count:<15} {percentage:<10.2f}")
    
    # Tính toán thống kê
    if partition_data:
        counts = [data[1] for data in partition_data]
        min_count = min(counts)
        max_count = max(counts)
        avg_count = sum(counts) / len(counts)
        
        logging.info("-" * 65)
        logging.info(f"Phân phối: Min={min_count}, Max={max_count}, Avg={avg_count:.2f}")
        
        if avg_count > 0:
            deviation = (max_count - min_count) / avg_count * 100
            logging.info(f"Chênh lệch: {max_count - min_count} bản ghi ({deviation:.2f}%)")
        
        # Số lượng phân vùng có dữ liệu
        non_empty = sum(1 for count in counts if count > 0)
        logging.info(f"Số phân vùng có dữ liệu: {non_empty}/{len(counts)} ({non_empty/len(counts)*100:.2f}%)")
    
    cur.close()

def main():
    parser = argparse.ArgumentParser(description='Kiểm tra phân phối dữ liệu trong các phân vùng')
    parser.add_argument('--prefix', type=str, choices=['range', 'rrobin', 'both'], default='both',
                        help='Loại phân vùng cần kiểm tra (range, rrobin, hoặc both)')
    parser.add_argument('--partitions', type=int, default=5,
                        help='Số lượng phân vùng cần kiểm tra')
    
    args = parser.parse_args()
    
    # Kết nối đến database
    conn = getopenconnection(dbname=DATABASE_NAME)
    
    try:
        if args.prefix in ['range', 'both']:
            check_partition_distribution('range_part', args.partitions, conn)
        
        if args.prefix in ['rrobin', 'both']:
            check_partition_distribution('rrobin_part', args.partitions, conn)
    finally:
        conn.close()

if __name__ == "__main__":
    main() 