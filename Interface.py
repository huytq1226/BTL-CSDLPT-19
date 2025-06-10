#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
from psycopg2.extensions import AsIs
from psycopg2.sql import SQL, Identifier, Literal
import logging
import os
from io import StringIO
import time  # Import time module for logging execution time

logging.basicConfig(level=logging.INFO)

DATABASE_NAME = 'dds_assgn1'

# Helper function to log execution time
def log_execution_time(func_name, start_time):
    end_time = time.time()
    execution_time = end_time - start_time
    logging.info(f"Function {func_name} completed in {execution_time:.4f} seconds")

def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    start_time = time.time()
    connection = psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")
    log_execution_time("getopenconnection", start_time)
    return connection


def loadratings(ratingstablename, ratingsfilepath, openconnection): 
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    """
    start_time = time.time()
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
        log_execution_time("loadratings", start_time)


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    """
    start_time = time.time()
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
        log_execution_time("rangepartition", start_time)

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    Uses direct SQL partition queries for better performance.
    """
    start_time = time.time()
    # Reset rr_index for new partitioning
    save_rr_index(0)
    
    # Get connection and cursor
    conn = openconnection
    cur = conn.cursor()
    
    try:
        # Create partition tables
        for i in range(numberofpartitions):
            table_name = f"rrobin_part{i}"
            # Drop if exists and recreate
            cur.execute(f"DROP TABLE IF EXISTS {table_name};")
            cur.execute(f"""
                CREATE TABLE {table_name} (
                    userid INTEGER,
                    movieid INTEGER,
                    rating FLOAT
                );
            """)
        
        # Directly populate partitions using SQL
        for i in range(numberofpartitions):
            table_name = f"rrobin_part{i}"
            # Use the MOD function to distribute rows evenly
            cur.execute(f"""
                INSERT INTO {table_name} (userid, movieid, rating)
                SELECT userid, movieid, rating 
                FROM (
                    SELECT 
                        userid, 
                        movieid, 
                        rating,
                        ROW_NUMBER() OVER(ORDER BY userid) AS rn
                    FROM {ratingstablename}
                ) AS t
                WHERE MOD(t.rn - 1, {numberofpartitions}) = {i}
            """)
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error in roundrobinpartition: {e}")
        raise
    finally:
        cur.close()
        log_execution_time("roundrobinpartition", start_time)

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a row in a round-robin fashion.
    Uses stored file to track current partition index.
    """
    start_time = time.time()
    conn = openconnection
    cur = conn.cursor()
    
    try:
        # First, insert the record into the main ratings table
        cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)",
                   (userid, itemid, rating))
        
        # Count number of round-robin partitions
        # This approach uses a direct count instead of information_schema
        cur.execute("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE 'rrobin_part%'")
        num_partitions = cur.fetchone()[0]
        
        if num_partitions == 0:
            # No partitions exist, create the first one
            cur.execute("""
                CREATE TABLE IF NOT EXISTS rrobin_part0 (
                    userid INTEGER,
                    movieid INTEGER,
                    rating FLOAT
                )
            """)
            num_partitions = 1
        
        # Get the current index for round-robin insertion
        next_part_index = get_rr_index()
        
        # Determine which partition to insert into
        target_partition = next_part_index % num_partitions
        
        # Insert into target partition
        target_table = f"rrobin_part{target_partition}"
        cur.execute(f"""
            INSERT INTO {target_table} (userid, movieid, rating)
            VALUES (%s, %s, %s)
        """, (userid, itemid, rating))
        
        # Update the index for next insertion
        save_rr_index(next_part_index + 1)
        
        # Commit the transaction
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error in roundrobininsert: {e}")
        raise
    finally:
        cur.close()
        log_execution_time("roundrobininsert", start_time)


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    start_time = time.time()
    conn = openconnection
    cur = conn.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    
    try:
        # Insert vào bảng chính
        cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s)",
                   (userid, itemid, rating))
        
        # Tính toán partition index
        numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
        if numberofpartitions <= 0:
            raise ValueError("No range partitions found")
            
        delta = 5.0 / numberofpartitions
        
        # Xác định index của partition một cách tối ưu
        if rating == 0.0:
            # Edge case cho giá trị 0.0
            index = 0
        elif rating == 5.0:
            # Edge case cho giá trị 5.0
            index = numberofpartitions - 1
        else:
            # Các giá trị thông thường
            index = int(rating / delta)
            # Xử lý trường hợp rating nằm đúng biên giữa các khoảng
            if rating > 0 and index > 0 and abs(rating - index * delta) < 1e-9:
                index = index - 1
            
            # Đảm bảo index nằm trong khoảng hợp lệ
            index = min(index, numberofpartitions - 1)
        
        # Insert vào partition tương ứng
        table_name = f"{RANGE_TABLE_PREFIX}{index}"
        cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s)",
                   (userid, itemid, rating))
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error in rangeinsert: {e}")
        raise
    finally:
        cur.close()
        log_execution_time("rangeinsert", start_time)

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    start_time = time.time()
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
    log_execution_time("create_db", start_time)

def count_partitions(prefix, openconnection):
    """
    Function to count the number of tables which have the @prefix in their name somewhere.
    """
    start_time = time.time()
    con = openconnection
    cur = con.cursor()
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0]
    cur.close()
    log_execution_time("count_partitions", start_time)
    return count

def get_rr_index():
    """Get the current index for round robin insert"""
    start_time = time.time()
    try:
        with open("rr_index.txt", 'r') as f:
            index = int(f.read().strip())
        log_execution_time("get_rr_index", start_time)
        return index
    except:
        log_execution_time("get_rr_index", start_time)
        return 0


def save_rr_index(index):
    """Save the current index for round robin insert"""
    start_time = time.time()
    with open("rr_index.txt", 'w') as f:
        f.write(str(index))
    log_execution_time("save_rr_index", start_time)