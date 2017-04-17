# coding=utf-8


"""
    可以fork()出一个进程

    这个方法不用

"""

import pp
from dwetl.petl.etl_task import EtlWork
from dwetl.settings import *
from multiprocessing import cpu_count
from dwetl.plog.logtool import etllog


class PPServer(object):
    cpus = 4

    def __init__(self):
        self._get_os_core()
        self._get_groups()
        self.job_server = pp.Server(self.ncpus)

    def _get_os_core(self):
        self.ncpus = self.cpus or cpu_count()

    def _get_groups(self):
        self.groups = {groups for dbtype in DB_TABLE_STRUCTURE.keys() for groups in DB_TABLE_STRUCTURE[dbtype].keys()}

    def ppserver(self):

        try:
            for g in self.groups:
                task_groups = EtlWork(self.groups,)
            print self.job_server.print_stats()
        except Exception, e:
            etllog.error(str(e))
            pass


def main():
    if __name__ == '__main__':
        ps = PPServer()
        ps.ppserver()
