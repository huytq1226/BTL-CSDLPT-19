#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
from psycopg2.extensions import AsIs
import logging
from io import StringIO
logging.basicConfig(level=logging.INFO)

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection): 
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    Ultra-optimized version using stream processing and batch loading with StringIO.
    """
    create_db(DATABASE_NAME)
    conn = openconnection
    cur = conn.cursor()
    
    try:
        # Tạo bảng đích trực tiếp với cấu trúc cuối cùng
        cur.execute(f"""
            DROP TABLE IF EXISTS {ratingstablename};
            CREATE TABLE {ratingstablename} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
        """)
        
        # Sử dụng COPY command để tải dữ liệu vào bảng - cách nhanh nhất
        batch_size = 500_000  # Batch lớn hơn để giảm số lần gọi DB
        buffer = StringIO()
        count = 0
        
        # Xử lý từng dòng và định dạng lại để COPY
        with open(ratingsfilepath, 'r') as f:
            for line in f:
                # Tách và định dạng lại dữ liệu
                parts = line.strip().split('::')
                if len(parts) >= 3:
                    buffer.write(f"{parts[0]}\t{parts[1]}\t{parts[2]}\n")
                    count += 1
                    
                    # Đẩy dữ liệu theo batch
                    if count % batch_size == 0:
                        buffer.seek(0)
                        cur.copy_expert(
                            f"COPY {ratingstablename} (userid, movieid, rating) FROM STDIN WITH DELIMITER E'\t'",
                            buffer
                        )
                        buffer.truncate(0)
                        buffer.seek(0)
        
        # Xử lý phần còn lại
        if buffer.tell() > 0:
            buffer.seek(0)
            cur.copy_expert(
                f"COPY {ratingstablename} (userid, movieid, rating) FROM STDIN WITH DELIMITER E'\t'",
                buffer
            )
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error in loadratings: {e}")
        raise
    finally:
        cur.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    """
    conn = openconnection
    cur = conn.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    
    # Kiểm tra đầu vào
    if not isinstance(numberofpartitions, int) or numberofpartitions <= 0:
        raise ValueError("numberofpartitions phải là số nguyên dương")
    
    # Tính toán khoảng phân vùng
    delta = 5.0 / numberofpartitions
    
    try:
        # Xóa và tạo lại các bảng partition
        for i in range(numberofpartitions):
            table_name = f"{RANGE_TABLE_PREFIX}{i}"
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            cur.execute(f"CREATE TABLE {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT)")
        
        conn.commit()
        
        # Phân vùng đầu tiên (0) - bao gồm giá trị 0
        table_name = f"{RANGE_TABLE_PREFIX}0"
        cur.execute(f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            SELECT userid, movieid, rating FROM {ratingstablename}
            WHERE rating >= 0 AND rating <= {delta}
        """)
        
        # Các phân vùng 1 đến n-2
        for i in range(1, numberofpartitions-1):
            table_name = f"{RANGE_TABLE_PREFIX}{i}"
            min_range = i * delta
            max_range = (i + 1) * delta
            
            cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating FROM {ratingstablename}
                WHERE rating > {min_range} AND rating <= {max_range}
            """)
        
        # Phân vùng cuối cùng - bao gồm giá trị 5.0
        if numberofpartitions > 1:
            table_name = f"{RANGE_TABLE_PREFIX}{numberofpartitions-1}"
            min_range = (numberofpartitions - 1) * delta
            
            cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating FROM {ratingstablename}
                WHERE rating > {min_range} AND rating <= 5.0
            """)
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error in rangepartition: {e}")
        raise
    finally:
        cur.close()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    Ultra-fast implementation with optimized partition creation and insertion.
    """
    conn = openconnection
    cur = conn.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    try:
        # Tạo tất cả bảng partition trong một câu lệnh
        create_tables_sql = []
        for i in range(numberofpartitions):
            create_tables_sql.append(f"""
                DROP TABLE IF EXISTS {RROBIN_TABLE_PREFIX}{i};
                CREATE TABLE {RROBIN_TABLE_PREFIX}{i} (userid INTEGER, movieid INTEGER, rating FLOAT);
            """)
        
        cur.execute(";".join(create_tables_sql))
        conn.commit()
        
        # Lấy tổng số dòng để tính partition
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
        total_rows = cur.fetchone()[0]
        
        # Nếu không có dòng, không cần phân vùng
        if total_rows == 0:
            save_rr_index(0)
            return
        
        # Sử dụng INSERT với MOD để phân vùng dữ liệu
        for i in range(numberofpartitions):
            cur.execute(f"""
                INSERT INTO {RROBIN_TABLE_PREFIX}{i} (userid, movieid, rating)
                SELECT userid, movieid, rating 
                FROM (
                    SELECT userid, movieid, rating, 
                           ROW_NUMBER() OVER() AS rn 
                    FROM {ratingstablename}
                ) t
                WHERE MOD(rn - 1, {numberofpartitions}) = {i}
            """)
        
        # Khởi tạo file rr_index.txt
        save_rr_index(total_rows % numberofpartitions)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error in roundrobinpartition: {e}")
        raise
    finally:
        cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin
    approach. Optimized version with improved query performance while maintaining test compatibility.
    
    Optimizations:
    1. Sử dụng f-strings thay vì nối chuỗi để cải thiện hiệu suất và dễ đọc
    2. Sử dụng prepared statements để tránh SQL injection và cải thiện hiệu suất
    3. Xử lý lỗi với try-except để đảm bảo tính ổn định
    4. Đảm bảo tương thích với bộ kiểm thử bằng cách giữ nguyên logic ban đầu
    """
    # Sử dụng một transaction duy nhất cho tất cả các thao tác
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    
    try:
        # Chèn vào bảng chính - giữ nguyên trình tự thao tác để đảm bảo tương thích với bộ kiểm thử
        insert_query = f"INSERT INTO {ratingstablename}(userid, movieid, rating) VALUES (%s, %s, %s)"
        cur.execute(insert_query, (userid, itemid, rating))
        
        # Lấy số lượng hàng - phải thực hiện riêng để đảm bảo tương thích với bộ kiểm thử
        count_query = f"SELECT COUNT(*) FROM {ratingstablename}"
        cur.execute(count_query)
        total_rows = cur.fetchone()[0]
        
        # Xác định số lượng phân vùng và tính toán chỉ số
        numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
        index = (total_rows-1) % numberofpartitions
        table_name = f"{RROBIN_TABLE_PREFIX}{index}"
        
        # Chèn vào phân vùng tương ứng
        partition_insert_query = f"INSERT INTO {table_name}(userid, movieid, rating) VALUES (%s, %s, %s)"
        cur.execute(partition_insert_query, (userid, itemid, rating))
        
        # Commit transaction
        con.commit()
    except Exception as e:
        # Rollback trong trường hợp có lỗi
        con.rollback()
        raise e
    finally:
        # Đảm bảo cursor luôn được đóng
        cur.close()

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    Optimized version with better performance and correct logic.
    """
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    
    try:
        # Insert vào bảng chính trước
        cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)", 
                   (userid, itemid, rating))
        
        # Tính toán partition index
        numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
        delta = 5.0 / numberofpartitions
        
        # Logic tính index phải đồng nhất với cách rangepartition phân vùng dữ liệu
        if rating == 0.0:
            # Giá trị biên dưới (0.0) luôn thuộc phân vùng đầu tiên
            index = 0
        elif rating == 5.0:
            # Giá trị biên trên (5.0) thuộc về phân vùng cuối cùng
            # Điều này đồng nhất với cách rangepartition đưa giá trị 5.0 vào phân vùng cuối
            index = numberofpartitions - 1
        else:
            # Các giá trị khác
            index = int(rating / delta)
            # Giá trị nằm đúng tại biên giữa các phân vùng (trừ 0.0 và 5.0)
            # thuộc về phân vùng phía dưới
            if rating > 0 and rating % delta == 0:
                index = index - 1
        
        # Đảm bảo index trong phạm vi hợp lệ
        index = max(0, min(index, numberofpartitions - 1))
        
        table_name = f"{RANGE_TABLE_PREFIX}{index}"
        
        # Insert vào partition tương ứng với prepared statement
        cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)", 
                   (userid, itemid, rating))
        
        con.commit()
    except Exception as e:
        con.rollback()
        raise e
    finally:
        cur.close()

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()

    return count

def get_rr_index():
    """Get the current index for round robin insert"""
    try:
        with open("rr_index.txt", 'r') as f:
            return int(f.read().strip())
    except:
        return 0


def save_rr_index(index):
    """Save the current index for round robin insert"""
    with open("rr_index.txt", 'w') as f:
        f.write(str(index))