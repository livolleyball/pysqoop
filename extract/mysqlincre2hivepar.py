# coding=utf-8
from connect.get_config import get_config
from command.shellcommand import shell_exce_command
import time

version = time.strftime("%Y%m%d%H%M%S", time.localtime())



def mysqlincreimportpar(src_db, dst_db, dst_table, etl_mode, res_query, partition_key, partition_value,
                        mapper_nums):
    print('mysql-incre-par')
    sqoop_cmd = generate_sqoop_cmd(src_db, dst_db, dst_table, etl_mode, res_query, partition_key,
                                   partition_value,mapper_nums)
    shell_exce_command(sqoop_cmd)


def generate_sqoop_cmd(src_db, dst_db, dst_table, etl_mode, res_query, partition_key, partition_value,mapper_nums):
    connect, user_name, password= get_config(src_db, etl_mode)
    sqoop_cmd = """sqoop   \
    import \
    --hive-import \
    --hive-overwrite \
    --null-string '\\\\N' \
    --null-non-string '\\\\N' \
    --connect %s \
    --username %s \
    --password '%s'  """ % (connect, user_name, password)
    query = "--query '%s'" % res_query
    mappers = '--num-mappers %s' % mapper_nums
    target_dir = '--target-dir /user/tmp/sqoop/%s/%s/%s ' % (
        dst_db, dst_table, version)  ## 防止程序异常中断，重跑时报  Output directory   already exists
    hive_commands = """--hive-database  %s \
    --hive-table %s \
    --hive-delims-replacement " " \
    --hive-partition-key %s \
    --hive-partition-value "%s" """ % (dst_db, dst_table, partition_key, partition_value)
    sqoop_cmd = ' '.join([sqoop_cmd, hive_commands, target_dir, query, mappers])
    # logging.log(logging.INFO, 'Generated sqoop command: %s' % sqoop_cmd)
    return sqoop_cmd


if __name__ == '__main__':
    sqoop_cmd = generate_sqoop_cmd('hera', 'ods_stg', 'ods_hera_job_di', 'mysql-incre', 'select 1 as id',
                                   'day', '20180110', 1)
    # mysqlincreimportpar('hera', 'hera_job', 'ods_stg', 'ods_hera_job_di', 'mysql-incre', 'select 1 as id', 'day',
    #                        '20180110', 'select power(10,5);')
