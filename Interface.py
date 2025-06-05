#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection): 
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    Optimized version using temporary table to avoid ALTER TABLE operations.
    """
    create_db(DATABASE_NAME)
    con = openconnection
    cur = con.cursor()
    
    # Xóa bảng đích nếu tồn tại
    cur.execute("DROP TABLE IF EXISTS " + ratingstablename)
    
    # Tạo bảng đích với cấu trúc cuối cùng
    cur.execute("CREATE TABLE " + ratingstablename + " (userid INTEGER, movieid INTEGER, rating FLOAT)")
    
    # Tạo bảng tạm thời để nhận dữ liệu từ file
    temp_table = ratingstablename + "_temp"
    cur.execute("DROP TABLE IF EXISTS " + temp_table)
    cur.execute("CREATE TEMPORARY TABLE " + temp_table + 
                " (userid INTEGER, extra1 CHAR, movieid INTEGER, extra2 CHAR, " +
                "rating FLOAT, extra3 CHAR, timestamp BIGINT)")
    
    # Sử dụng COPY để tải dữ liệu vào bảng tạm thời
    with open(ratingsfilepath, 'r') as file:
        cur.copy_from(file, temp_table, sep=':')
    
    # Chèn dữ liệu từ bảng tạm thời vào bảng đích
    cur.execute("INSERT INTO " + ratingstablename + " (userid, movieid, rating) " +
                "SELECT userid, movieid, rating FROM " + temp_table)
    
    # Xóa bảng tạm thời (không cần thiết vì bảng tạm thời sẽ tự động bị xóa khi phiên kết thúc)
    cur.execute("DROP TABLE IF EXISTS " + temp_table)
    
    cur.close()
    con.commit()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    """
    con = openconnection
    cur = con.cursor()
    delta = 5 / numberofpartitions
    RANGE_TABLE_PREFIX = 'range_part'
    for i in range(0, numberofpartitions):
        minRange = i * delta
        maxRange = minRange + delta
        table_name = RANGE_TABLE_PREFIX + str(i)
        cur.execute("create table " + table_name + " (userid integer, movieid integer, rating float);")
        if i == 0:
            cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating >= " + str(minRange) + " and rating <= " + str(maxRange) + ";")
        else:
            cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating > " + str(minRange) + " and rating <= " + str(maxRange) + ";")
    cur.close()
    con.commit()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    for i in range(0, numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute("create table " + table_name + " (userid integer, movieid integer, rating float);")
        cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from (select userid, movieid, rating, ROW_NUMBER() over() as rnum from " + ratingstablename + ") as temp where mod(temp.rnum-1, 5) = " + str(i) + ";")
    cur.close()
    con.commit()

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
    """
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    delta = 5 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index = index - 1
    table_name = RANGE_TABLE_PREFIX + str(index)
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.close()
    con.commit()

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
