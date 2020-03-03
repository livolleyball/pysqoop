# coding=utf-8

import logging
import sys,os
from command.oraclecommand import oracle_exce_command

logging.basicConfig(level=logging.WARNING)  # 配置下日志器的日志级别

sys.path.append('../')  # 把模块路径放到环境变量中作为全局变量

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

pre_sql = sys.argv[1]

def exec_oracle(pre_sql):
    oracle_exce_command("oyo_dw", "load-oracle", pre_sql)


if __name__ == '__main__':
    exec_oracle(pre_sql)
