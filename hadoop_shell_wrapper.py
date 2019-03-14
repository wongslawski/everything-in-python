# -*- coding: utf-8 -*-
#===============================================================
#   Copyright (xxx) 2019 All rights reserved.
#
#   @filename: hadoop_shell_wrapper.py
#   @author: xxx@xxx.com
#   @date: 2019/01/30/ 11:23:11
#   @brief:
#
#   @history:
#
#================================================================

import re
import sys
import json
import os
import logging
from collections import namedtuple

reload(sys)
sys.setdefaultencoding( "utf-8" )
sys.path.append(os.getcwd())

import shell_wrapper

HADOOP_HOME = None
if os.environ.get("HADOOP_HOME", "") != "":
    HADOOP_HOME = os.environ["HADOOP_HOME"].strip()
assert HADOOP_HOME, "error! HADOOP_HOME not set!"


"""
refers to http://hadoop.apache.org/docs/r1.0.4/cn/hdfs_shell.html
"""

def ls(*hdfs_paths, **kwargs):
    HadoopFile = namedtuple(
            "HadoopFile",
            ["name", "date", "time", "size", "is_file"])
    files = []
    try:
        uniq_set = set()

        for hdfs_path in hdfs_paths:
            if kwargs.get("recursive", False):
                cmd = '{}/bin/hadoop fs -lsr {}'.format(HADOOP_HOME, hdfs_path)
            else:
                cmd = '{}/bin/hadoop fs -ls {}'.format(HADOOP_HOME, hdfs_path)

            if kwargs.get("print_cmd", False):
                logging.info(cmd)

            for line in shell_wrapper.shell_command_stdout(cmd=cmd):
                items = line.strip().split(" ")
                if len(items) < 8:
                    continue
                f = HadoopFile(
                        name=items[-1],
                        time=items[-2],
                        date=items[-3],
                        size=int(items[-4]),
                        is_file=(items[0][0] == '-'))

                if f.is_file and f.size < kwargs.get("size_threshold", 0):
                    continue
                if not kwargs.get("keep_dir", True) and not f.is_file:
                    continue
                if not kwargs.get("keep_file", True) and f.is_file:
                    continue
                if kwargs.get("path_regex", "") and re.match(kwargs["path_regex"], f.name) is None:
                    continue
                if kwargs.get("distinct", True):
                    if f.name in uniq_set:
                        continue
                    uniq_set.add(f.name)
                files.append(f)

        if kwargs.get("sort", True):
            files.sort(key=lambda x: x.name)
        return files
    except:
        logging.exception("exception occur", exc_info=True)
    return None

def exists(hdfs_path, **kwargs):
    try:
        cmd = '{}/bin/hadoop fs -test -e {}'.format(HADOOP_HOME, hdfs_path)
        if kwargs.get("print_cmd", False):
            logging.info(cmd)
        return shell_wrapper.shell_command(cmd=cmd)
    except Exception as ex:
        logging.exception("exception occur", exc_info=True)
    return False

def rm(*hdfs_paths, **kwargs):
    for hdfs_path in hdfs_paths:
        if not exists(hdfs_path):
            continue
        cmd = '{}/bin/hadoop fs -rm {}'.format(HADOOP_HOME, hdfs_path)
        if kwargs.get("print_cmd", False):
            logging.info(cmd)
        if not shell_wrapper.shell_command(cmd=cmd):
            return False
    return True

def rmr(*hdfs_paths, **kwargs):
    for hdfs_path in hdfs_paths:
        if hdfs_path.replace("///", "/").replace("//", "/").rstrip("/").count('/') < kwargs.get("safe_depth", 4):
            logging.warning("hdfs path [%s] failed depth check, skip deleting.", hdfs_path)
            return False
        if not exists(hdfs_path):
            logging.warning("hdfs path [%s] not found, skip deleting.", hdfs_path)
            continue
        cmd = '{}/bin/hadoop fs -rm -r {}'.format(HADOOP_HOME, hdfs_path)
        if kwargs.get("print_cmd", False):
            logging.info(cmd)
        if not shell_wrapper.shell_command(cmd=cmd):
            return False
    return True

def touchz(*hdfs_files, **kwargs):
    for hdfs_file in hdfs_files:
        if exists(hdfs_file):
            logging.warning("hdfs file [%s] exists already, skip touching", hdfs_file)
            continue
        cmd = '{}/bin/hadoop fs -touchz {}'.format(HADOOP_HOME, hdfs_file)
        if kwargs.get("print_cmd", False):
            logging.info(cmd)
        if not shell_wrapper.shell_command(cmd=cmd):
            return False
    return True

def get(src, localdst, **kwargs):
    if not exists(src):
        logging.error("hdfs path [%s] not found, skip downloading.", src)
        return False
    cmd = '{}/bin/hadoop fs -get {} {}'.format(HADOOP_HOME, src, localdst)
    if kwargs.get("print_cmd", False):
        logging.info(cmd)
    return shell_wrapper.shell_command(cmd=cmd)

def getmerge(src, localdst, **kwargs):
    if not exists(src):
        logging.error("hdfs path [%s] not found, skip downloading.", src)
        return False
    cmd = '{}/bin/hadoop fs -getmerge {} {}'.format(HADOOP_HOME, src, localdst)
    if kwargs.get("print_cmd", False):
        logging.info(cmd)
    return shell_wrapper.shell_command(cmd=cmd)

def mkdir(*hdfs_paths, **kwargs):
    for hdfs_path in hdfs_paths:
        if exists(hdfs_path):
            logging.warning("hdfs path [%s] exists already, skip mkdir", hdfs_path)
            continue
        cmd = '{}/bin/hadoop fs -mkdir -p {}'.format(HADOOP_HOME, hdfs_path)
        if kwargs.get("print_cmd", False):
            logging.info(cmd)
        if not shell_wrapper.shell_command(cmd=cmd):
            return False
    return True

def put(*args, **kwargs):
    if len(args) < 2:
        raise Exception("error param for hadoop_put, at least src_path and dest_path")
    cmd = '{}/bin/hadoop fs -put {}'.format(HADOOP_HOME, " ".join(args))
    if kwargs.get("print_cmd", False):
        logging.info(cmd)
    return shell_wrapper.shell_command(cmd=cmd)

def stat(hdfs_path, **kwargs):
    HadoopFile = namedtuple(
            "HadoopFile",
            ["name", "date", "time", "size", "is_file"])
    cmd = "{}/bin/hadoop fs -stat '%F###%b###%y' {}".format(HADOOP_HOME, hdfs_path)
    if kwargs.get("print_cmd", False):
        logging.info(cmd)
    for line in shell_wrapper.shell_command_stdout(cmd=cmd):
        try:
            items = line.rstrip().split("###")
            if len(items) != 3:
                continue
            f = HadoopFile(
                    name=hdfs_path,
                    size=int(items[1]),
                    date=items[2].split(" ")[0],
                    time=items[2].split(" ")[1],
                    is_file=(items[0] == 'regular file'))
            return f
        except Exception as ex:
            logging.exception("exception occur", exc_info=True)
    return None

def mv(*args, **kwargs):
    if len(args) < 2:
        raise Exception("error param for hadoop_mv, at least src_path and dest_path")

    cmd = "{}/bin/hadoop fs -mv {}".format(HADOOP_HOME, " ".join(args))
    if kwargs.get("print_cmd", False):
        logging.info(cmd)

    if len(args) > 2:
        # This command allows multiple sources as well in which case the destination needs to be an existing directory.
        fstat = stat(args[-1])
        if fstat is None or fstat.is_file:
            logging.error("destination [%s] needs to be an existing directory.", args[-1])
            return False
    return shell_wrapper.shell_command(cmd=cmd)

def cp(*args, **kwargs):
    if len(args) < 2:
        raise Exception("error param for hadoop_cp, at least src_path and dest_path")
    cmd = "{}/bin/hadoop fs -cp {} {}".format(
            HADOOP_HOME,
            "-f" if kwargs.get("force", False) else "",
            " ".join(args))
    if kwargs.get("print_cmd", False):
        logging.info(cmd)

    if len(args) > 2:
        # This command allows multiple sources as well in which case the destination must be a directory.
        fstat = stat(args[-1])
        if fstat is None or fstat.is_file:
            logging.error("destination [%s] needs to be an existing directory.", args[-1])
            return False
    return shell_wrapper.shell_command(cmd=cmd)

def dus(*args, **kwargs):
    HadoopFile = namedtuple(
            "HadoopFile",
            ["name", "size"])
    cmd = "{}/bin/hadoop fs -du -s {}".format(HADOOP_HOME, " ".join(args))
    if kwargs.get("print_cmd", False):
        logging.info(cmd)
    outputs = []
    for line in shell_wrapper.shell_command_stdout(cmd=cmd):
        items = line.rstrip().replace("  ", " ").split(" ")
        if len(items) != 3:
            continue
        if items[0].isdigit() and items[1].isdigit():
            f = HadoopFile(
                    name=items[2],
                    size=int(items[0]))
            outputs.append(f)
    return outputs


# python hadoop_shell_wrapper.py
if __name__ == "__main__":
    logging.basicConfig(
            level = logging.INFO,
            format = '[%(asctime)s - %(filename)s - %(levelname)s] %(message)s',
            datefmt = '%Y-%m-%d %H:%M:%S')
    pass
