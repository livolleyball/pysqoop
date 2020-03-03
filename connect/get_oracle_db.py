# coding=utf-8
import cx_Oracle
from exception.toolsexception import OracleDatabaseError

def get_oracle_db(connect, user_name, password):
    host = connect.split("@")[1]
    db_oracle = cx_Oracle.connect( user_name, password,host)
    return db_oracle

def db_delete_commit(db_oracle, pre_sql):
    cursor = db_oracle.cursor()
    print('正在执行 oracle SQL')
    try:
        # 执行sql语句
        cursor.execute(pre_sql)
        # 提交到数据库执行
        db_oracle.commit()
        message='执行成功'

    except Exception as e:
        # 如果发生错误则回滚
        db_oracle.rollback()
        message = 'oracle 执行失败'
        raise OracleDatabaseError("执行: oracle语句 %s 时出错：%s" % (pre_sql, e))

    print(message)
    db_oracle.close()

if __name__ == '__main__':
    user_name, password, connect=('oyo_dw',"eOD'F9A3q2m~om_tfCKz",'jdbc:oracle:thin:@10.200.71.247:1521/oyodw')
    db_oracle = get_oracle_db(connect, user_name, password)
    print(db_oracle)
    db_oracle.close()
