#!/usr/bin/env python
# -*- coding:utf-8 -*-

import argparse
import datetime
import sys
import time
import os
import threading
sys.path.append('../')


class CmdQueue(object):
    """command queue
    """
    def __init__(self):
        """initilize
        """
        self.queue = []
        self.mutex = threading.Lock()

    def add(self, cmd):
        """add a new command to the queue
        """
        self.mutex.acquire()
        self.queue.append(cmd)
        self.mutex.release()

    def get(self):
        """get a command from the queue and remove it from the queue
        """
        cmd = None
        self.mutex.acquire()
        if len(self.queue) > 0:
            cmd = self.queue.pop(0)
        self.mutex.release()
        return cmd

    def print_cmds(self):
        """print commands in the queue
        """
        for cmd in self.queue:
            print cmd


class CmdRunner(threading.Thread):
    """job in a thread
    """
    def __init__(self, queue):
        """initilize
        """
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        """continously fetch a command from the queue and execute it
        """
        while True:
            cmd = self.queue.get()
            if not cmd:
                break
            os.system(cmd)


class CmdManager(object):
    """manage multiple threads
    """
    def __init__(self, threads_num, cmd_queue):
        """initilize
        """
        self.threads_num = threads_num
        if self.threads_num < 1:
            self.threads_num = 1
        self.cmd_runners = []
        for i in range(self.threads_num):
            cmd_runner = CmdRunner(cmd_queue)
            cmd_runner.setDaemon(True)
            self.cmd_runners.append(cmd_runner)

    def run(self):
        """run multiple threads
        """
        for cmd_runner in self.cmd_runners:
            cmd_runner.start()
        for cmd_runner in self.cmd_runners:
            cmd_runner.join()


def run(scripts, end_date, days_num, threads_num, reverse):
    """main func
    """
    last_dates = last_n_day(end_date, days_num - 1)
    last_dates.append(end_date)
    last_dates.sort(reverse=reverse)
    cmd_queue = CmdQueue()
    for each_day in last_dates:
        each_date_str = each_day.strftime('%Y-%m-%d')
        cmds = []
        for script in scripts.split(','):
            cmd = '%s %s' % (script, each_date_str)
            cmds.append(cmd)
        cmd_queue.add(' && '.join(cmds))
    cmd_queue.print_cmds()
    cmd_manager = CmdManager(threads_num, cmd_queue)
    cmd_manager.run()


def last_n_day(date1, n):
    """get last n dates
    """
    last_date_set = []
    for i in range(0, n):
        lastdate = date1 - datetime.timedelta(days=(n - i))
        last_date_set.append(lastdate)
    return last_date_set


def perfect_date(string):
    """datestring is from yyyymmdd to date
    """
    time_struct = time.strptime(string, "%Y-%m-%d")
    date = datetime.date(time_struct.tm_year, time_struct.tm_mon, time_struct.tm_mday)
    return date


if __name__ == '__main__':
    parser = argparse.ArgumentParser(usage='%(prog)s -d yyyymmdd ' \
            '-n days_num -t threads_num -s scripts', description='mine 7 days')
    parser.add_argument('-d', '--date', dest = 'date', type = perfect_date, required=True)
    parser.add_argument('-n', '--num', dest = 'num', type = int, required=True)
    parser.add_argument('-s', '--scripts', dest = 'scripts', required=True)
    parser.add_argument('-t', '--threads', dest = 'threads', type = int, required=True)
    parser.add_argument("-r", "--reverse", action="store_true", dest="reverse")
    args = parser.parse_args()

    if not args.date or not args.num or not args.threads or not args.scripts:
        sys.exit(-1)
    if args.threads > args.num:
        args.threads = args.num
    try:
        run(args.scripts, args.date, args.num, args.threads, args.reverse)
    except Exception as ex:
        print str(ex)
        sys.exit(-1)
