# coding=utf-8

from connect.get_config import get_config
from connect.get_oracle_db  import get_oracle_db,db_delete_commit



def oracle_exce_command(db_name, etl_mode,pre_sql):
    connect, user_name, password=get_config(db_name, etl_mode)
    db_oracle=get_oracle_db(connect, user_name, password)
    db_delete_commit(db_oracle,pre_sql)




if __name__ == '__main__':
    db_name, etl_mode, pre_sql=('','','')
    oracle_exce_command(db_name, etl_mode,pre_sql)

