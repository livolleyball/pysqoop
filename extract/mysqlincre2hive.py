# coding=utf-8
from connect.get_config import get_config
from command.shellcommand import shell_exce_command
from command.hivecommand import hive_exce_command, hive_command
import time

version = time.strftime("%Y%m%d%H%M%S", time.localtime())


def mysqlincreimport(src_db, dst_db, dst_table, etl_mode, res_query, mapper_nums):
    print('mysql-incre')
    #drop_table_command = ' '.join(["""drop table if EXISTS """, dst_db + '.' + dst_table, ";"])
    #sql = hive_command(drop_table_command)
    #hive_exce_command(sql)  ## 全量抽取前先删除表，防止源表增加新列
    sqoop_cmd = generate_sqoop_cmd(src_db, dst_db, dst_table, etl_mode, res_query, mapper_nums)
    shell_exce_command(sqoop_cmd)


def generate_sqoop_cmd(src_db, dst_db, dst_table, etl_mode, res_query, mapper_nums):
    connect, user_name, password = get_config(src_db, etl_mode)
    sqoop_cmd = """sqoop   \
    import \
    --hive-import \
    --hive-overwrite \
    --null-string '\\\\N' \
    --null-non-string '\\\\N' \
    --connect %s \
    --username %s \
    --password '%s'  """ % (connect, user_name, password)
    query = "--query '%s' " % res_query
    mappers = '--num-mappers %s' % mapper_nums
    target_dir = '--target-dir /user/tmp/sqoop/%s/%s/%s ' % (
        dst_db, dst_table, version)  ## 防止程序异常中断，重跑时报  Output directory   already exists
    hive_commands = """
    --hive-database  %s  \
    --hive-table %s \
    --hive-delims-replacement " " 
    """ % (dst_db, dst_table)
    sqoop_cmd = ' '.join([sqoop_cmd, hive_commands, target_dir, query, mappers])
    # logging.log(logging.INFO, 'Generated sqoop command: %s' % sqoop_cmd)
    return sqoop_cmd


if __name__ == '__main__':
    sqoop_cmd = generate_sqoop_cmd('hera', 'ods_stg', 'ods_hera_job_di', 'mysql-incre', 'select 1 as id', 1)
    # mysqlincreimport('hera', 'hera_job', 'ods_stg', 'ods_hera_job_di', 'mysql-incre','select 1 as id','select power(10,5);')
