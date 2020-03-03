# coding=utf-8

from metadata.hivemetadatacomment import get_hive_column_comment, get_hive_tb_comment
from metadata.mysqlmetadatacomment import mysql_column_comment, mysql_tb_comment
from connect.get_db import get_db
from connect.get_config import get_config
from command.hivecommand import hive_exce_command, hive_command
from helper.utils import clean_str


def alter_column_comment_query(db_name, tb_name, hive_col_comment, mysql_col_comment):
    query = ''
    for m in range(len(hive_col_comment)):
        for n in range(len(mysql_col_comment)):
            if (hive_col_comment[m]["COLUMN_NAME"] == mysql_col_comment[n]["COLUMN_NAME"]
                and hive_col_comment[m]["COLUMN_COMMENT"] !=
                    clean_str( mysql_col_comment[n]["COLUMN_COMMENT"].strip())):

                query += "alter table " + db_name + "." + tb_name + " change column `" \
                         + hive_col_comment[m]["COLUMN_NAME"] + "` `" \
                         + hive_col_comment[m]["COLUMN_NAME"] + "` " \
                         + hive_col_comment[m]["TYPE_NAME"] \
                         + " comment '" + clean_str( mysql_col_comment[n]["COLUMN_COMMENT"].strip()) + "';"
            else:
                pass

    print("alter_column_comment_query is :  " + query)
    return query


def alter_tb_comment_query(db_name, tb_name, hive_tb_comment, mysql_tb_comment):
    query = ''
    if hive_tb_comment['TABLE_COMMENT'].strip() != mysql_tb_comment['TABLE_COMMENT'].replace("\n", "").strip():
        # ALTER TABLE table_name SET TBLPROPERTIES('comment' = '这是表注释!');
        query += "alter table " + db_name + "." + tb_name \
                 + " SET TBLPROPERTIES('comment' = '" \
                 + clean_str(mysql_tb_comment['TABLE_COMMENT'].strip()) + "');"

    else:
        pass

    print("alter_tb_comment_query is : " + query)
    return query


def hive_alter_comment(query):
    if len(query) < 10:
        pass
    else:
        hivecommand = hive_command(query)
        hive_exce_command(hivecommand)


def syn_table_comment(src_db, src_table, dst_db, dst_table, etl_mode):
    connect, user_name, password = get_config(src_db, etl_mode)
    db_mysql = get_db(connect, user_name, password)
    mysql_tb_comment_dict = mysql_tb_comment(db_mysql, src_db, src_table)
    hive_tb_comment = get_hive_tb_comment(dst_db, dst_table)
    query = alter_tb_comment_query(dst_db, dst_table, hive_tb_comment, mysql_tb_comment_dict)
    hive_alter_comment(query)


def syn_column_comment(src_db, src_table, dst_db, dst_table, etl_mode):
    connect, user_name, password = get_config(src_db, etl_mode)
    db_mysql = get_db(connect, user_name, password)
    mysql_col_comment = mysql_column_comment(db_mysql, src_db, src_table)
    hive_col_comment = get_hive_column_comment(dst_db, dst_table)
    query = alter_column_comment_query(dst_db, dst_table, hive_col_comment, mysql_col_comment)
    hive_alter_comment(query)




if __name__ == '__main__':
    syn_table_comment('hera', 'hera_profile', 'oyodw', 'hera_profile', 'mysql-full')
    syn_column_comment('hera', 'hera_profile', 'oyodw', 'hera_profile', 'mysql-full')
