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


class MaxValue(ConnectDB):
    """
        1. 连MySQL

        2. 先获得表的最大值和最小值的范围
            因为数据是实时插入的, 只有>= 会有无法停止的情况, 也会不准确
    """
    def __init__(self, _where_values, table):
        super(MaxValue, self).__init__('mysql')
        self._where_values = _where_values
        self.table = table
        self.max_field_cache = {}
        self._where_dict = None

    def _get_max_delta_field(self):
        """
            :return {field: value, max_field: value}
        """
        if self.max_field_cache.get(self.table):
            return self.max_field_cache.get(self.table)

        else:
            _delta_db_table = clean_etl_conf._get_delta_table(self.table)
            fields = self._where_values.keys()
            for table in _delta_db_table:
                # where conditions
                _SQL = '''SELECT '''
                _SQL += ','.join(["MAX(%s)" % _.replace(table+'_', '') for _ in fields])
                _SQL += " FROM %s WHERE " % table
                _where = '''1=1 '''
                for key, value in self._where_values.items():
                    if isinstance(value, int) or isinstance(value, long):
                        _where += "and {0} >= {1} ".format(key, value)
                    else:
                        _where += " and {0} >= '{1}'".format(key, value)

                # max sql
                _SQL = _SQL + _where

                # new dict
                self._conn_cursor()
                self.query(_SQL)

                # fetch
                _max_value = self.fetchallrows()
                self._close_cursor()

                # max dict
                # {'a': 1, 'max_a': 2, 'b': 2, 'max_b': 3, 'c': 3, 'max_c': 4}
                # 会存在没有新数据的情况
                if _max_value[0][0] == None:
                    _max_where_value = {'max_' + field: self._where_values[field] for field in fields}
                else:
                    _max_where_value = {'max_' + field: _ for field in fields for one in _max_value for _ in one}

                # the new dict
                self._where_values.update(_max_where_value)
                # cache
                self.max_field_cache[self.table] = self._where_values

            return self._where_values


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
            取出所有表的增量字段, 缓存在_DETAL_CACHES属性中
        """
        try:
            self.query(self._query_sql)
            self._DETAL_CACHES = {_[1]: _[2] for _ in self.fetchallrows()}
            self._close_cursor()
        except Exception, e:
            etllog.error('[delta_control.py] TDeltaConf [_detal_caches] ' + str(e))

    def _set_tables_where_conditions(self, table):
        """
            获得表的where范围
            field & max_field
        """
        self._where_values = self._DETAL_CACHES.get(table)
        try:
            if self._where_values:

                max_value = MaxValue(self._where_values, table)

                self._where_dict = max_value._get_max_delta_field()

                return self._where_dict
            else:
                return {}
        except Exception, e:
            etllog.error('[delta_control.py] TDeltaConf [_set_tables_where_conditions] ' + str(e))

    def _set_tables_limit(self):

        """
            表通用的limit
        """

        _where_limit = ''' limit %s, %s''' % (self.limit, self.offset)
        self.limit += self.offset

        return _where_limit

    def _update_max_delta_values(self, table, _is_data):
        """
            @:table 要update的表
            @:_is_data 用来确定是否更新表;
                        若_is_data没有值,不更新直接返回
        """
        if not _is_data:
            self.limit = 0
            etllog.info('[delta_control.py] [delta_control] %s 没有最新的数据' % table)
            return

        if not self._where_values:
            self.limit = 0
            return

        try:
            # {'a': 1, 'max_a': 2, 'b': 2, 'max_b': 3, 'c': 3, 'max_c': 4}
            # ===>
            # {'a': 2, 'b': 3, 'c': 4}

            _info = {_: self._where_dict.get('max_'+_) for _ in self._where_dict.keys() if 'max_' not in _}

            for key, value in _info.items():
                if isinstance(value, int) or isinstance(value, long):
                    # number格式加1
                    value += 1
                else:
                    # 时间格式加一秒
                    value = str(clean_datetime._time_plus_second(value))
                _info[key] = value

            # 当前表任务完成, 增量字段
            etllog.info('==========[ETL] UPDATE TABLES=%s DELTA VALUE ==========' % table)

            self._end_update_sql(table, _info)

        except Exception, e:
            etllog.error('[delta_control.py] TDeltaConf [_update_max_delta_values]' + str(e))

    @check_params(str, dict)
    def _end_update_sql(self, table, json_values):
        """
            @:table, 需要update的表; 将增量字段更新到表t_delta_conf中
        """
        try:
            self._update_sql += '''
                UPDATE t_delta_conf SET primarykey = '%s', update_date = '%s' WHERE tables = '%s';
            ''' % (json.dumps(json_values), clean_datetime._today(), table)

            self._conn_cursor()
            self.insert(self._update_sql)
            self._close_cursor()

            # 只实例化了一次, 每次保存后, 需要改为0
            self.limit = 0
        except Exception, e:
            etllog.info('[delta_control.py] TDeltaConf [t_delta_conf]======>' + self._update_sql)
            etllog.error('[delta_control.py] TDeltaConf [_end_update_sql]' + str(e))

t_delta_conf = TDeltaConf()
