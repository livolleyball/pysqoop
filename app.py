# coding=utf-8
import sys
import os

from command.synmetadata import syn_table_comment,syn_column_comment
from extract.mysqlfull2hive import mysqlfullimport
from extract.mysqlincre2hive import mysqlincreimport
from extract.mysqlincre2hivepar import mysqlincreimportpar
from extract.oraclequery2hivepar import oracle_query_2_hive_import_par
from extract.oraclequery2hive import oracle_query_2_hive_import
from load.hive2mysql import hive2mysql
from load.hive2mysqlupsert import hive2mysqlupsert
from command.oraclecommand import oracle_exce_command
import logging

from load.hive2oracle import hive2oracle

logging.basicConfig(level=logging.WARNING)  # 配置下日志器的日志级别

sys.path.append('../')  # 把模块路径放到环境变量中作为全局变量

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

etl_mode = sys.argv[1]  # mysql-full mysql-incre


def exce(etl_mode):
    if etl_mode == 'mysql-full':
        src_db, src_table = sys.argv[2].split('.')  # 源库名.表名
        dst_db, dst_table = sys.argv[3].split('.')  # 目标库名.表名
        res_query=sys.argv[4]
        mapper_nums = sys.argv[5]
        print(src_db, src_table, dst_db, dst_table, etl_mode,res_query,mapper_nums)
        mysqlfullimport(src_db, src_table, dst_db, dst_table, etl_mode,res_query,mapper_nums)
        syn_table_comment(src_db, src_table, dst_db, dst_table, etl_mode)
        syn_column_comment(src_db, src_table, dst_db, dst_table, etl_mode)

    elif etl_mode == 'mysql-incre':
        src_db, src_table = sys.argv[2].split('.')  # 源库名.表名
        dst_db, dst_table = sys.argv[3].split('.')  # 目标库名.表名
        res_query = sys.argv[4]
        mapper_nums = sys.argv[5]
        print(src_db, src_table, dst_db, dst_table, etl_mode, res_query, mapper_nums)
        mysqlincreimport(src_db, dst_db, dst_table, etl_mode, res_query, mapper_nums)
        syn_table_comment(src_db, src_table, dst_db, dst_table, etl_mode)
        syn_column_comment(src_db, src_table, dst_db, dst_table, etl_mode)

    elif etl_mode == 'mysql-incre-par':
        src_db, src_table = sys.argv[2].split('.')  # 源库名.表名
        dst_db, dst_table = sys.argv[3].split('.')  # 目标库名.表名
        res_query = sys.argv[4]
        mapper_nums = sys.argv[5]
        partition_key = sys.argv[6]
        partition_value = sys.argv[7]
        print(src_db, src_table, dst_db, dst_table, etl_mode, res_query, mapper_nums,partition_key,partition_value)
        mysqlincreimportpar(src_db, dst_db, dst_table, etl_mode, res_query, partition_key,partition_value, mapper_nums)
        syn_table_comment(src_db, src_table, dst_db, dst_table, etl_mode)
        syn_column_comment(src_db, src_table, dst_db, dst_table, etl_mode)



    elif etl_mode == 'oracle-query':
        src_db, src_table = sys.argv[2].split('.')  # 源库名.表名
        dst_db, dst_table = sys.argv[3].split('.')  # 目标库名.表名
        res_query = sys.argv[4]
        mapper_nums = sys.argv[5]
        print(src_db, src_table, dst_db, dst_table, etl_mode, res_query, mapper_nums)
        oracle_query_2_hive_import(src_db, dst_db, dst_table, etl_mode, res_query, mapper_nums)

    elif etl_mode == 'oracle-query-par':
        src_db, src_table = sys.argv[2].split('.')  # 源库名.表名
        dst_db, dst_table = sys.argv[3].split('.')  # 目标库名.表名
        res_query = sys.argv[4]
        mapper_nums = sys.argv[5]
        partition_key = sys.argv[6]
        partition_value = sys.argv[7]
        print(src_db, src_table, dst_db, dst_table, etl_mode, res_query, mapper_nums,partition_key,partition_value)
        oracle_query_2_hive_import_par(src_db, dst_db, dst_table, etl_mode, res_query, partition_key,partition_value, mapper_nums)

    elif etl_mode == 'load-oracle':
        src_db, src_table = sys.argv[2].split('.')  # 源库名.表名
        dst_db, dst_table = sys.argv[3].split('.')  # 目标库名.表名
        dst_columns=sys.argv[4]
        res_query = sys.argv[5]
        mapper_nums = sys.argv[6]
        pre_sql = sys.argv[7]
        hive2oracle(etl_mode, pre_sql, src_db, src_table, dst_db, dst_table, dst_columns, res_query, mapper_nums)


    elif etl_mode == 'load-mysql':
        src_db, src_table = sys.argv[2].split('.')  # 源库名.表名
        dst_db, dst_table = sys.argv[3].split('.')  # 目标库名.表名
        dst_columns=sys.argv[4]
        res_query = sys.argv[5]
        mapper_nums = sys.argv[6]
        pre_sql = sys.argv[7]
        hive2mysql(src_db, src_table, dst_db, dst_table, dst_columns, etl_mode, res_query, mapper_nums, pre_sql)


    elif etl_mode == 'load-mysql-upsert':
        src_db, src_table = sys.argv[2].split('.')  # 源库名.表名
        dst_db, dst_table = sys.argv[3].split('.')  # 目标库名.表名
        update_key=sys.argv[4]
        res_query = sys.argv[5]
        mapper_nums = sys.argv[6]
        hive2mysqlupsert(src_db, src_table, dst_db, dst_table, etl_mode, res_query, mapper_nums,update_key)


    else:
        raise TypeError('Unknown etl_mode')
        print('错误的的抽取方式 可选项（mysql-full  mysql-incre  mysql-incre-par oracle-query  oracle-query-par load-mysql load-oracle  load-mysql-upsert）')


if __name__ == '__main__':
    exce(etl_mode)
