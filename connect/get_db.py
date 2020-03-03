# coding=utf-8
import pymysql
from connect.get_config import get_config
from retrying import retry
from exception.toolsexception import MysqlDatabaseError
import time
import re
from exception.toolsexception import retry_if_operationalError


# 间隔20秒，最多10次
@retry(wait_fixed=20 * 1000, retry_on_exception=retry_if_operationalError, stop_max_attempt_number=10)
def get_db(connect, user_name, password):
    matchObj = re.match(r'jdbc:mysql://(.*):(.*)/(.*)\?(.*)', connect, re.M | re.I)
    host = matchObj.group(1)
    port=int(matchObj.group(2))
    db_name=matchObj.group(3)

    db_mysql = pymysql.connect(host=host, user=user_name, passwd=password, port=port,db=db_name,
                               charset='utf8', cursorclass=pymysql.cursors.DictCursor)
    return db_mysql


def db_query_commit(db_mysql, pre_sql):
    cursor = db_mysql.cursor()
    try:
        # 执行sql语句
        cursor.execute(pre_sql)
        # 提交到数据库执行
        db_mysql.commit()
    except Exception as e:
        # 如果发生错误则回滚
        db_mysql.rollback()
        raise MysqlDatabaseError("执行: mysql语句 %s 时出错：%s" % (pre_sql, e))
    db_mysql.close()


if __name__ == '__main__':
    connect, user_name, password = get_config('hera', 'mysql-full')
    db_mysql = get_db(connect, user_name, password)
