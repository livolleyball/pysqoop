# coding=utf-8

import pymysql
from retrying import retry
from exception.toolsexception import retry_if_operationalError


# 间隔20秒，最多10次
@retry(wait_fixed=20 * 1000, retry_on_exception=retry_if_operationalError, stop_max_attempt_number=10)
def get_hive_db(env):
    if env=='dev':
        conn_hive = pymysql.connect(host='192.168.71.236'
                                        , user='bigdata'
                                        , passwd='12QWaszx'
                                        , port=3306
                                        , db='hive'
                                        , charset='utf8',
                                        cursorclass=pymysql.cursors.DictCursor
                                        )
    else:
        conn_hive = pymysql.connect(host='host'
                                        # , user='root'
                                        # , passwd='123456'
                                        , user='dw'
                                        , passwd='internal'
                                        , port=3306
                                        , db='hive'
                                        , charset='utf8',
                                        cursorclass=pymysql.cursors.DictCursor
                                        )

    return conn_hive

if __name__ == '__main__':
    conn_hive=get_hive_db('prd')
    print(conn_hive.host_info)