# coding=utf-8
import subprocess
import shlex



def shell_exce_command(shellcommand):
    args=shlex.split(shellcommand)
    out_bytes = subprocess.check_output(args)



if __name__ == '__main__':
    shell_exce_command('ls ')

