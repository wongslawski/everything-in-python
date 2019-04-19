# -*- coding: utf-8 -*-

import sys
import os
import logging
import subprocess

reload(sys)
sys.setdefaultencoding("utf-8")
sys.path.append(os.getcwd())


def shell_command(**kwargs):
    try:
        proc = subprocess.Popen(kwargs['cmd'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        if kwargs.get("print_info", False):
            with proc.stdout:
                for line in iter(proc.stdout.readline, b''):
                    logging.info(line.strip())
        if kwargs.get("print_error", True):
            with proc.stderr:
                for line in iter(proc.stderr.readline, b''):
                    logging.error(line.strip())
        proc.wait()
        return proc.returncode == 0
    except KeyboardInterrupt:
        logging.exception("killed by KeyboardInterrupt", exc_info=True)
    return False


def shell_command_stdout(**kwargs):
    try:
        proc = subprocess.Popen(kwargs['cmd'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        with proc.stdout:
            for line in iter(proc.stdout.readline, b''):
                yield line.strip()
        proc.wait()
    except KeyboardInterrupt:
        logging.exception("killed by KeyboardInterrupt", exc_info=True)


if __name__ == "__main__":
    logging.basicConfig(
            level = logging.INFO,
            format = '[%(asctime)s - %(filename)s - %(levelname)s] %(message)s',
            datefmt = '%Y-%m-%d %H:%M:%S')
    for x in shell_command_stdout(cmd="hadoop fs -ls /user/adst/wangshuang"):
        print x
