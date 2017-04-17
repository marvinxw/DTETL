# coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from consume_log import ReadFile
from sftp_get_file import SftpGFile
from dwetl.petl.clean_etl_conf import clean_datetime
from dwetl.plog.logtool import etllog
from dwetl.settings import LOG_TIME_PARAMS


class LogTask(object):

    def __init__(self, file=None, time_args=None):
        """
            @:time_args 时间参数, 如果有时间参数, 执行时间参数对应的文件
            @:file 文件
        """
        self.file = file

        if len(LOG_TIME_PARAMS) != 0:
            self.time_args = LOG_TIME_PARAMS.pop().strip()
            try:
                clean_datetime._time_strptime(self.time_args, '1')
            except Exception, e:
                etllog.error('[LogTask.py] [time_args] 时间参数错误, 程序退出, %s ' % str(e))
                from dwetl.petl.p_decorator import sys_exit1
                sys_exit1()

        else:
            self.time_args = None

        self.sftp_get_file = SftpGFile(self.file, self.time_args)
        self.read_file = ReadFile(self.file, self.time_args)
        self._start()

    def _start(self):
        # get file first
        etllog.lfinfo('========== ETL-LOG-FILE START %s ==========' % clean_datetime._today())
        self.sftp_get_file._start()
        # read file first
        self.read_file._start()

    def _bulk_get(self):
        return self.read_file._get_file_datas_to_queue()
