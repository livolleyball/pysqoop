# coding=utf-8


from exception.toolsexception import NotFoundError
from connect.get_config import get_config
from connect.get_db import get_db

def mysql_tb_comment(db_mysql,src_db, src_table):
    cur = db_mysql.cursor()
    rv = cur.execute('''
    select TABLE_SCHEMA  as db_name
      ,TABLE_NAME
      ,if(TABLE_COMMENT is null ,'',TABLE_COMMENT) as TABLE_COMMENT
    from information_schema.`TABLES`
    where TABLE_SCHEMA='%s' 
    and  TABLE_NAME='%s' ;
    ''' % (src_db, src_table))

    if cur.rowcount == 1:
        hive_tb_comment = cur.fetchone()
        ###  dict {'db_name': 'oyodw', 'TABLE_NAME': 'user_activate', 'TABLE_COMMENT': ''}
    else:
        raise NotFoundError("mysql could not find table or table does not exist")
    return hive_tb_comment
    db_mysql.close


def mysql_column_comment(db_mysql,src_db, src_table):
    cur = db_mysql.cursor()
    rv = cur.execute('''
    select
      TABLE_SCHEMA as db_name
      ,TABLE_NAME
      ,COLUMN_NAME
      ,if(COLUMN_COMMENT is null,'',COLUMN_COMMENT) as COLUMN_COMMENT
    from information_schema.`COLUMNS`
    
    where TABLE_SCHEMA='%s' 
    and  TABLE_NAME='%s';
    ''' % (src_db, src_table))

    if cur.rowcount >= 1:
        hive_col_comment = cur.fetchall()
        ### list[ dict ]
    else:
        raise NotFoundError("could not find table or table does not exist")
    return hive_col_comment
    db_mysql.close

if __name__ == '__main__':
    connect, user_name, password = get_config('hera', 'mysql-full')
    db_mysql=get_db(connect, user_name, password)
    mysql_tb_comment(db_mysql,'hera', 'hera_job')
    mysql_column_comment(db_mysql,'hera', 'hera_job')
