# coding=utf-8
import pymysql

from exception.toolsexception import NotFoundError
from connect.get_hive_db import get_hive_db
from retrying import retry
from exception.toolsexception import retry_if_operationalError



## 获取hive表备注
# 间隔20秒，最多10次
@retry(wait_fixed=20 * 1000, retry_on_exception=retry_if_operationalError, stop_max_attempt_number=10)
def get_hive_tb_comment(db_name, tb_name):
    conn_hive = get_hive_db('prd')
    conn_hive.ping(reconnect=True)
    cur = conn_hive.cursor()
    rv = cur.execute('''
    select C.name as db_name
      ,B.tbl_name as TABLE_NAME
      ,if(A.param_value is null ,'',A.param_value) as TABLE_COMMENT
    from TABLE_PARAMS A inner join TBLS B on A.tbl_id = B.tbl_id
    inner join DBS C on B.DB_ID = C.DB_Id
    where
    C.name = '%s' 
      and B.tbl_name = '%s'
      and  A.param_key = 'comment';
    ''' % (db_name, tb_name))

    if cur.rowcount == 1:
        hive_tb_comment = cur.fetchone()
        ###  dict {'db_name': 'oyodw', 'TABLE_NAME': 'user_activate', 'TABLE_COMMENT': '????'}
    else:
        raise NotFoundError("could not find table or table does not exist")
    return hive_tb_comment
    conn_hive.close()

@retry(wait_fixed=20 * 1000, retry_on_exception=retry_if_operationalError, stop_max_attempt_number=10)
def get_hive_column_comment(db_name, tb_name):
    conn_hive = get_hive_db('prd')
    conn_hive.ping(reconnect=True)
    cur = conn_hive.cursor()
    rv = cur.execute('''
    SELECT
    T4.name as db_name
    ,T1.tbl_name as TABLE_NAME
      ,T3.COLUMN_NAME
      ,T3.TYPE_NAME
      ,if(T3.COMMENT is null ,'',T3.COMMENT) as COLUMN_COMMENT
    FROM TBLS T1
    INNER JOIN SDS T2  ON T1.SD_ID=T2.SD_ID
    INNER JOIN COLUMNS_V2 T3 ON T2.CD_ID=T3.CD_ID
    INNER JOIN DBS T4 on T1.DB_ID = T4.DB_ID
    WHERE T3.column_name not in('etl_time')
    and T4.name='%s'
    and T1.tbl_name ='%s'
    order by T1.tbl_name,T3.INTEGER_IDX
    ''' % (db_name, tb_name))

    if cur.rowcount >= 1:
        hive_col_comment = cur.fetchall()
        ###  list[dict]
    else:
        raise NotFoundError("hive could not find table or table does not exist")
    return hive_col_comment
    conn_hive.close()


if __name__ == '__main__':
    hive_col_comment = get_hive_column_comment('oyodw', 'user_activate_range')
    print(hive_col_comment)
