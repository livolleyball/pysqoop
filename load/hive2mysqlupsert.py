# coding=utf-8
from connect.get_config import get_config
from connect.get_db import get_db, db_query_commit
from command.shellcommand import shell_exce_command
from command.hivecommand import hive_command, hive_exce_command

import time

version = time.strftime("%Y%m%d%H%M%S", time.localtime())

def hive2mysqlupsert(src_db, src_tb, dst_db, dst_table, etl_mode, res_query, mapper_nums,update_key):
    export_dir = "/user/tmp/sqoop/export/mysql/%s/%s/%s" % (src_db, src_tb, version)
    hive_sql = """ INSERT OVERWRITE  DIRECTORY '%s' \
        row format delimited
        fields terminated by '\\001' 
    %s 
    """ % (export_dir, res_query)
    hivecommand = hive_command(hive_sql)
    hive_exce_command(hivecommand)  ## 将数据写到目标路径
    connect, user_name, password = get_config(dst_db, etl_mode)
    db_mysql = get_db(connect, user_name, password)
    sqoop_cmd=generate_sqoop_cmd(dst_db, dst_table,etl_mode, export_dir, mapper_nums,update_key)
    shell_exce_command(sqoop_cmd)
    shell_exce_command("hdfs dfs -rm -r /user/tmp/sqoop/export/mysql/%s/%s/%s" % (src_db, src_tb,version))  ## 删除临时文件


def generate_sqoop_cmd( dst_db, dst_table, etl_mode,export_dir,mapper_nums,update_key):
    mapreduce_job_name=''.join(["SQOOP_",etl_mode,"_",dst_db,".",dst_table])
    connect, user_name, password = get_config(dst_db, etl_mode)
    sqoop_cmd = """sqoop   \
    export  \
    --table %s \
    --connect %s \
    --username %s \
    --password "%s" \
    --input-fields-terminated-by '\\001' \
    --input-lines-terminated-by '\\n' \
    --input-null-string '\\\\N' \
    --input-null-non-string '\\\\N' \
    --num-mappers %s \
    --export-dir %s \
    --mapreduce-job-name %s \
    --update-key %s \
    --update-mode allowinsert
     """ % (dst_table, connect, user_name, password,mapper_nums,export_dir , mapreduce_job_name,update_key)
    # logging.log(logging.INFO, 'Generated sqoop command: %s' % sqoop_cmd)
    return sqoop_cmd


if __name__ == '__main__':
    sqoop_cmd = generate_sqoop_cmd('hera', 'ods_stg', 'ods_hera_job_di', 'mysql-incre')