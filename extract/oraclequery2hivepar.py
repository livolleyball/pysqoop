# coding=utf-8


from command.shellcommand import shell_exce_command
import logging

from connect.get_config import get_config

logging.basicConfig(level=logging.WARNING)  # 配置下日志器的日志级别
import time

version = time.strftime("%Y%m%d%H%M%S", time.localtime())


def generate_sqoop_cmd(src_db, dst_db, dst_table, etl_mode, res_query, mapper_nums, partition_key, partition_value):
    connect, user_name, password = get_config("oyo_dw", etl_mode)
    mapreduce_job_name = ''.join(["SQOOP_", etl_mode, "_", dst_db, ".", dst_table])
    sqoop_cmd = """sqoop   \
    import \
    --hive-import \
    --hive-overwrite \
    --null-string '\\\\N' \
    --null-non-string '\\\\N' \
    --connect %s \
    --username %s \
    --password "%s"  """ % (connect, user_name, password)
    query = "--query '%s' " % res_query
    mappers = '--num-mappers %s' % mapper_nums
    ## 防止程序异常中断，重跑时报  Output directory   already exists
    target_dir = '--target-dir /user/tmp/sqoop/%s/%s/%s ' % (dst_db, dst_table, version)
    hive_commands = """ 
    --hive-database  %s \
    --hive-table %s \
    --hive-delims-replacement " " \
    --hive-partition-key %s \
    --hive-partition-value "%s" \
    --driver oracle.jdbc.OracleDriver \
    --connection-manager org.apache.sqoop.manager.GenericJdbcManager \
    --mapreduce-job-name %s 
    """ % (dst_db, dst_table, partition_key, partition_value,mapreduce_job_name)
    sqoop_cmd = ' '.join([sqoop_cmd, query, target_dir, mappers,hive_commands])
    # logging.log(logging.INFO, 'Generated sqoop command: %s' % sqoop_cmd)
    return sqoop_cmd


def oracle_query_2_hive_import_par(src_db, dst_db, dst_table, etl_mode, res_query, partition_key,partition_value, mapper_nums):
    sqoop_cmd = generate_sqoop_cmd(src_db, dst_db, dst_table, etl_mode, res_query, mapper_nums, partition_key,
                                   partition_value)
    shell_exce_command(sqoop_cmd)


if __name__ == '__main__':
    pass