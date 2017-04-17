# coding=utf8
import json
from DB_conn import ConnectDB
from ConfigParser import ConfigParser
from p_decorator import check_params
from dwetl.plog.logtool import etllog
from clean_etl_conf import clean_etl_conf, clean_datetime, clean_others_params


"""
    增量控制
"""


class RwConfigFile(object):

    # todo

    def __init__(self):
        self.CONFIGFILE = '../conf.cfg'
        self.config = None
        self.read_config_file()

    def read_config_file(self):
        try:
            self.config = ConfigParser()
            self.config.read(self.CONFIGFILE)
        except Exception, e:
            etllog.error('[delta_control.py] RwConfigFile read_config_file' + str(e))

    @check_params(str, str)
    def get_config_info(self, section, option):
        try:
            return self.config.get(section, option)
        except Exception, e:
            etllog.error('[delta_control.py] RwConfigFile get_config_info' + str(e))

    @check_params(str, str)
    def set_config_context(self, section, option, value):
        try:
            self.config.set(section, option, value)
            self.config.write(open(self.CONFIGFILE, 'w'))
        except Exception, e:
            etllog.error('[delta_control.py] RwConfigFile set_config_context' + str(e))


class TDeltaConf(ConnectDB):
    """
        增量保存在postgre中t_delta_conf表中

        1. 一次查询
        2. 更新
        3. update
    """
    def __init__(self):
        super(TDeltaConf, self).__init__('postgresql')
        self._query_sql = '''
          SELECT rowid, tables, primarykey FROM t_delta_conf;
        '''
        self._update_sql = ''
        self._DETAL_CACHES = None
        self.limit = 0
        self.offset = clean_others_params._detal_num()

    def _detal_caches(self):
        """
            取出所有表的增量字段, 缓存为_DETAL_CACHES
        """
        try:
            self.query(self._query_sql)
            self._DETAL_CACHES = {_[1]: _[2] for _ in self.fetchallrows()}
            self._close_cursor()
        except Exception, e:
            etllog.error('[delta_control.py] TDeltaConf [_detal_caches] ' + str(e))

    def _set_where_paging(self, table):

        """
            配置好where condition and paging
        """
        self._where_values = self._DETAL_CACHES.get(table)
        _where_limit = ''' limit %s, %s''' % (self.limit, self.offset)
        self.limit += self.offset

        return self._where_values, _where_limit

    def _update_max_delta_values(self, table, _is_data):
        """
            @:table 要update的表
            @:_is_data 用来确定是否更新表;
                        若_is_data没有值,不更新直接返回
        """

        # todo 制表
        if not _is_data:
            etllog.info('[delta_control.py] [delta_control] %s 没有最新的数据' % table)
            return

        try:
            _delta_field = clean_etl_conf._get_delta_field(table)
            # where conditions
            _SQL = '''SELECT '''
            _SQL += ','.join(["MAX(%s)" % _ for _ in _delta_field])
            _SQL += " FROM %s WHERE " % table
            _where = '''1=1 '''
            for key, value in self._where_values.items():
                if isinstance(value, int) or isinstance(value, long):
                    _where += "and {0} >= {1} ".format(key, value)
                else:
                    _where += " and {0} >= to_date('{1}', 'yyyy-mm-dd HH:MI:SS')".format(key, value)

            # max sql
            _SQL = _SQL + _where

            # new dict
            self._conn_cursor()
            self.query(_SQL)
            _info = dict(zip(_delta_field, self.fetchallrows()[0]))

            self._close_cursor()

            # modify
            for key, value in _info.items(): #{_: _info[_] for _ in self._where_values.keys()}.items():
                if isinstance(value, int) or isinstance(value, long):
                    # number格式加1
                    value += 1
                else:
                    # 时间格式加一秒
                    value = clean_datetime._time_plus_second(value)
                _info[key] = value

            # update
            self._DETAL_CACHES.update({table: {_: _info[_] for _ in self._where_values.keys()}})
        except Exception, e:
            etllog.error('[delta_control.py] TDeltaConf [_update_max_delta_values]' + str(e))

    def _end_update_sql(self, table):
        """
            @:table, 需要update的表; 将_DETAL_CACHES存的表增量字段更新到t_delta_conf表中
        """
        try:
            self._conn_cursor()
            json_values = self._DETAL_CACHES[table]
            self._update_sql += '''
                UPDATE t_delta_conf SET primarykey = '%s', update_date = '%s' WHERE tables = '%s';
            ''' % (json.dumps(json_values), clean_datetime._today(), table)

            self.insert(self._update_sql)

            self._close_cursor()
            # 只实例化了一次, 每次保存后, 需要改为0
            self.limit = 0
        except Exception, e:
            etllog.info('[delta_control.py] TDeltaConf [t_delta_conf]======>' + self._update_sql)
            etllog.error('[delta_control.py] TDeltaConf [_end_update_sql]' + str(e))

t_delta_conf = TDeltaConf()
