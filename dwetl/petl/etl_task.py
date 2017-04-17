# coding=utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from UD_Exception import ParamsError
from clean_etl_conf import clean_etl_conf, clean_file_server, clean_others_params
from extract_datas import ExtractDatas
from load_datas import LoadDatas
from dwetl.plog.logtool import etllog


'''
    执行task
'''


class EtlWork(object):
    def __init__(self, options, *args):
        """
            @:_groups_or_tables 文件 or 表 or 表组
        """
        self.options = options
        self._log_message = clean_others_params._get_log_message()

    def _start_groups(self):
        tables = clean_etl_conf._get_group_tables(self.options)
        self._log_message['TABLE_OR_VIEW'] = tables

        for table in tables:
            self._start_table(table)

    def _start_table(self, table):
        """
            @:table 表
        """
        etllog.info('========== CURRENT TABLE %s ==========' % table)
        self._log_message['TABLE_OR_VIEW'] = table
        task = EtlTask(table)
        task.start()

    def _start_file(self, file):
        etllog.info('========== CURRENT FILE %s ==========' % self.options)
        etllog.lfinfo('========== CURRENT FILE %s ==========' % self.options)
        self._log_message['TABLE_OR_VIEW'] = '[FILE]' + file
        task = EtlTask(file)
        task.start()

    def start(self):
        if self.options in clean_etl_conf._get_groups():
            self._start_groups()
        elif self.options in clean_etl_conf._get_tables():
            self._start_table(self.options)
        elif self.options in clean_file_server._set_file_or_table():
            self._start_file(self.options)
        else:
            raise ParamsError('[etl_task.py] EtlWork [start]', u'没有这个参数')


class EtlTask(object):
    def __init__(self, options):
        """
            @:options 表 或 文件
        """
        self.table = options
        try:
            self.extract_datas = ExtractDatas(options)._create_instance()
            # 判断是文件还是表, 是文件取得表
            if options in clean_file_server._set_file_or_table():
                options = clean_file_server._set_file_or_table(options)
            self.load_datas = LoadDatas(options)
        except Exception, e:
            etllog.error('[etl_task.py] EtlTask [__init__]' + str(e))

    def start(self):
        """
            主循环, 没有res就退出
        """
        while 1:
            res = self._get()
            if res == 0 or res == None:
                break
            self._put()

    def _get(self):
        try:
            res = self.extract_datas._bulk_get()
            return res
        except Exception, e:
            etllog.error('[etl_task.py] EtlTask [_get]' + str(e))
            from dwetl.petl.p_decorator import sys_exit1
            sys_exit1()

    def _put(self):
        try:
            self.load_datas.bulk_put()
        except Exception, e:
            etllog.error('[etl_task.py] EtlTask [_put]' + str(e))
            from dwetl.petl.p_decorator import sys_exit1
            sys_exit1()

