# coding=utf-8
from command.oraclecommand import oracle_exce_command
from connect.get_config import get_config
from connect.get_db import get_db, db_query_commit
from command.shellcommand import shell_exce_command
from command.hivecommand import hive_command, hive_exce_command

import time

from connect.get_oracle_db import get_oracle_db

version = time.strftime("%Y%m%d%H%M%S", time.localtime())

def hive2oracle(etl_mode,pre_sql,src_db,src_tb,dst_db,dst_table,dst_columns,res_query, mapper_nums):
    export_tb = "sqoop_oracle_%s_%s" % (src_db, src_tb)
    hive_sql = """ drop table if EXISTS oyo_etl_tmp.%s; create TABLE oyo_etl_tmp.%s as  \
    %s 
    """ % (export_tb, export_tb,res_query)
    hivecommand = hive_command(hive_sql)
    hive_exce_command(hivecommand)  ## 将数据写到目标表
    oracle_exce_command(dst_db,etl_mode, pre_sql)  ## 先清除目标表数据
    sqoop_cmd=generate_sqoop_cmd(dst_db, dst_table,dst_columns, etl_mode, export_tb, mapper_nums)
    shell_exce_command(sqoop_cmd)
    hivecommand_rm=hive_command(""" drop table if EXISTS oyo_etl_tmp.%s """ %(export_tb))
    hive_exce_command(hivecommand_rm)


def generate_sqoop_cmd( dst_db, dst_table,dst_columns, etl_mode,export_tb,mapper_nums):
    mapreduce_job_name=''.join(["SQOOP_",etl_mode,"_",dst_db,".",dst_table])
    connect, user_name, password = get_config(dst_db, etl_mode)
    sqoop_cmd = """sqoop   \
    export  \
    --table %s \
    --connect %s \
    --username %s \
    --password "%s" \
    --columns %s \
    --input-fields-terminated-by '\\001' \
    --input-lines-terminated-by '\\n' \
    --input-null-string '\\\\N' \
    --input-null-non-string '\\\\N' \
    --num-mappers %s \
    --hcatalog-database oyo_etl_tmp \
    --hcatalog-table %s \
    --mapreduce-job-name %s 
     """ % (dst_table, connect, user_name, password,dst_columns,mapper_nums, export_tb , mapreduce_job_name)
    # logging.log(logging.INFO, 'Generated sqoop command: %s' % sqoop_cmd)
    return sqoop_cmd


if __name__ == '__main__':
    sqoop_cmd = generate_sqoop_cmd('oyo_dw', 'dw_datacenter_new_member_dim_dw_lihm', 'id,member_id,perfer_citys,live_city,base_city,sync_time', 'load-oracle','export_dir',1)
    print(sqoop_cmd)