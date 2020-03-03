# coding=utf-8
import pymysql
import math
import logging

logging.basicConfig(level=logging.WARNING)  # 配置下日志器的日志级别
blocksize = (1024 ** 3) * 512


def row_count_mysql_query(db_name, table_name, etl_mode, *row_cnt_mysql_incre_query):
    if etl_mode == 'mysql-full':
        query = "SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA ='%s' AND TABLE_NAME ='%s';" % (
            db_name, table_name)
    elif etl_mode == 'mysql-incre' or etl_mode == 'mysql-incre-par':
        query = row_cnt_mysql_incre_query[0]
    else:
        SystemExit
        print('错误的的抽取方式 可选项（mysql-full  mysql-incre）')
    return query


## 考虑到mysql性能 将mappers 的大小改为人为配置
def get_mapper_num(row_cnt):
    mapper_nums = int(math.ceil((row_cnt * 20 * (10 ** 8)) / blocksize))
    if mapper_nums < 2:
        return 1
    elif mapper_nums > 3:
        return 3  ## 考虑集群性能
    else:
        return mapper_nums


def get_config(db_name, etl_mode):
    ## jdbc信息表
    # db = pymysql.connect("192.168.21.92", "root", "123456", "hera")

    cursor = db.cursor()

    if etl_mode == 'mysql-full' or etl_mode == 'mysql-incre' \
        or etl_mode == 'mysql-incre-par' or etl_mode == 'oracle-query' or etl_mode=='oracle-query-par':
        cursor.execute("""SELECT * from dmp_data_source where db_name='%s' and type ='%s'""" % (db_name, 'read'))
        try:
            data = cursor.fetchone()
            connect, user_name, password = data[3], data[4], data[5]
        except:
            logging.log(logging.ERROR, 'Error:具有读取权限的数据源 %s 尚未添加' % db_name)
            raise
        return connect, user_name, password

    elif etl_mode == 'load-mysql' or etl_mode == 'load-oracle'  or etl_mode == 'load-mysql-upsert':
        cursor.execute("""SELECT * from dmp_data_source where db_name='%s' and type ='%s'""" % (db_name.lower(), 'write'))
        try:
            data = cursor.fetchone()
            connect, user_name, password = data[3], data[4], data[5]
        except TypeError:
            logging.log(logging.ERROR, 'Error:具有写入权限的数据源 %s 尚未添加' % db_name)
            raise
        return connect, user_name, password
    else:
        raise TypeError('错误的的etl_mode:可选项(mysql-full mysql-incre mysql-incre-par oracle-query-par oracle-query load-mysql load-oracle)')

    # 关闭数据库连接
    db.close()


if __name__ == '__main__':
    get_config('hera', 'mysql-full')
    print("------------------------------------")
    get_config('hera_1', 'mysql-incre')
    print("------------------------------------")
