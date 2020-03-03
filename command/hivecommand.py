# coding=utf-8
import subprocess
import shlex


def hive_command(sql):
    hive_type='hive -e '
    hivecommand=' '.join([hive_type,""" " """,sql,""" " """])  ## 添加引号
    print('hive command is %s' %hivecommand)
    return hivecommand

def hive_exce_command(hivecommand):
    args=shlex.split(hivecommand)
    out_bytes = subprocess.check_output(args)  ## 返回 hive -e 的执行状态
    out_text = out_bytes.decode('utf-8')  ## hive -e 的执行结果，一般不需要
    print(out_text) ## 打印输出结果
    # try:
    #     out_bytes = subprocess.check_output(args,stderr=subprocess.STDOUT)
    # except subprocess.CalledProcessError as e:
    #     out_bytes = e.output  # Output generated before error code = e.returncode
    #     out_text = out_bytes.decode('utf-8')
    #     print('------' ,out_text)
    # return out_text


if __name__ == '__main__':
    hivecommand=hive_command('select 1')
    hive_exce_command(hivecommand)

