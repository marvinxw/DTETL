# coding=utf8
from DB_conn import ConnectDB
from dwetl.logfile.log_task import LogTask
from clean_etl_conf import clean_etl_conf, clean_file_server
from dwetl.plog.logtool import etllog
from dwetl.settings import DATAS
from delta_control import t_delta_conf

"""
    抽取数据
    1. db
    2. 文件
"""


class ExtractDatas(object):
    def __init__(self, type):
        self.type = type

    def _create_instance(self):
        """
            判断是log表, 还是db表
        """
        if self.type in clean_file_server._set_file_or_table():
            return LogTask(file=self.type)
        else:
            return DbDatas(self.type)


class DbDatas(ConnectDB):

    def __init__(self, table):
        super(DbDatas, self).__init__(clean_etl_conf._get_from_to("FROM"))
        self.table = table
        self._is_data = None
        self._query_sql()

    def _query_sql(self):
        _where_conditions = t_delta_conf._set_tables_where_conditions(self.table)
        self._sql = clean_etl_conf._get_sql_query(self.table).format(**_where_conditions)
        self._local_sql = self._sql

    def _query_sql_limit(self):
        """
            组成select语句
        """
        try:
            self._sql = self._local_sql + t_delta_conf._set_tables_limit()
        except Exception, e:
            etllog.error('[extract_datas.py] DbDatas [_query_sql]' + str(e))

    def _get_datas(self):
        """
            执行sql语句
        """
        try:
            self.query(self._sql)
            res = self.fetchallrows()
        except Exception, e:
            etllog.error('[extract_datas.py] DbDatas [_get_datas]' + str(e))
            res = None
        return res

    def _bulk_get(self):
        """
            self._is_data每次取数据的最后一行,
            每次都更新, 用来判断是否有新数据,并更新增量字段
            多发一次请求来确认是否还有新数据是应该的
        """
        self._query_sql_limit()
        _datas = self._get_datas()
        if _datas and len(_datas):
            DATAS.put(_datas)
            self._is_data = _datas[-1]
        else:
            t_delta_conf._update_max_delta_values(self.table, self._is_data)
        return DATAS.qsize()
