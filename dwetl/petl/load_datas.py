# coding=utf8
import re
import sys
sys.setrecursionlimit(10000)
from DB_conn import ConnectDB
from clean_etl_conf import clean_etl_conf, clean_others_params
from transform import DBFieldClean
from dwetl.plog.logtool import etllog


"""
    将数据整理成insert的形式
    数据insert到数据库
"""


class LoadDatas(ConnectDB):

    def __init__(self, table):
        super(LoadDatas, self).__init__(clean_etl_conf._get_from_to("TO"))
        self.table = table
        self.field_type = clean_etl_conf._get_fieldType(table)
        self._str_datas = None
        self.fnone = lambda v: 'NULL' if v == None else v # 在Python中None是数据库中的NULL
        self._update_tables = clean_others_params._get_update_tables()
        self._common_delete_sql = '''
            DELETE FROM
                %s
            WHERE
        '''

    def insert_sql(self):
        """
            insert sql
        """
        self._sql = '''
            INSERT INTO
                %s (%s)
            VALUES
                %s
        ''' % (self.table, ','.join(clean_etl_conf._get_field(self.table)), self._str_datas)

    def delete_sql(self, _field, del_val):
        """
            发生异常时执行这个delete语句
        """
        _sql = self._common_delete_sql % (self.table)

        _where = "1=1"

        for key, value in zip(_field, map(self.fnone, del_val)):
            if value == 'NULL':
                _and = ' is '
                _value = '%s'
            else:
                _and = ' = '
                _value = "'%s'"
            _where += " and %s %s " % (key, _and) + _value % (value)

        return _sql + _where

    def _formatns(self):
        """
            字段个数
        """
        s = "(" + ','.join(['%s'] * len(self.field_type)) + ")"
        return s

    def transform_field(self):
        """
            类型转换
        """
        db_field_clean = DBFieldClean()
        self._datas = db_field_clean._clean_p2postgre(self.field_type)

    def _datas_str(self):
        """
            拼成(),().....();
        """
        _mogrify = self.mogrify_post
        try:
            self._str_datas = ','.join(_mogrify(self._formatns(), d) for d in self._datas)
        except Exception, e:
            etllog.error('[load_datas.py] LoadDatas [_datas_str]' + str(e))

    def _update_delete_sql(self):
        """
            对于update的表，先执行delete语句
        """
        _sql = self._common_delete_sql % (self.table)

        _primarykey = clean_etl_conf._get_table_primarykey(self.table)

        # [(), (), ()]
        # delete from tables where fielda in () and fieldb in ()
        _where = " 1=1 "
        for i, pk in enumerate(_primarykey): # 如果多个主键
            _where += " and " + pk + " in %s " % (tuple(map(lambda x: x[i], self.back_datas)), )

        return _sql + _where

    def bulk_put(self, exflag=None):
        """
            @exflag: 如果是异常数据,不用transform_field()
        """
        # 数据put到数据库
        if exflag == None:
            self.transform_field()
        self.back_datas = self._datas[:]
        self._datas_str()
        self.insert_sql() # 拼出insert sql

        try:
            # 如果是update的表先删除
            if self.table in self._update_tables:
                _delete_sql = self._update_delete_sql()
                self.delete(_delete_sql)

            self.insert(self._sql)
        except:
            # 异常处理
            self._exception_clean()

    def _exception_clean(self):
        """
            一次取得1W行数据

            1)插入多条数据
            2)如果遇到异常了, 循环异常
            3)找到异常后
            4)其它数据到1)步骤
            5)没有异常; 异常最大递归10000次;break

            如果异常在最后, 效率低
        """
        self._roll_back()
        _split_str = lambda s: s[1:-1].split(',')

        def _put():
            self._datas_str()
            self.insert_sql()
            self.insert(self._sql)

        def _delete(err_str):
            """
            调用删除
            :param err_str:
            :return:
            """
            _sql = None
            try:
                # 匹配duplicate data
                p = re.compile(r'(\(.*\))=(\(.*\))')
                m = re.search(p, err_str)
                _field, _values = m.group(1), m.group(2)
                _sql = self.delete_sql(_split_str(_field), _split_str(_values))
                self._roll_back()
                self.delete(_sql)
            except Exception, e:
                etllog.info(u'_delete异常数据===>' + _sql)
                etllog.error('[load_datas.py] LoadDatas [_exception_clean] _delete ' + str(e))

        def _insert(sql):
            """
            调用插入
            :param sql:
            :return:
            """
            try:
                self._roll_back()
                self.insert(sql)
            except Exception, e:
                etllog.info(u'_insert异常数据===>' + sql)
                etllog.error('[load_datas.py] LoadDatas [_exception_clean] _insert ' + str(e))

        try:
            while 1:
                if len(self.back_datas) != 0:
                    self._datas = [self.back_datas.pop()]
                else:
                    break
                try:
                    _put()
                except Exception, e:
                    # 删除数据,重新插入
                    err_str = str(e) # 需要匹配的异常数据
                    _ins_sql = self._sql # 需要再次插入的数据

                    if 'duplicate key' in err_str:
                        _delete(err_str)
                        _insert(_ins_sql)
                    else:
                        etllog.info(u'异常数据===>' + _ins_sql)

                    # 重新put后面的数据
                    self._roll_back()
                    self._datas = self.back_datas[:]
                    self._roll_back()
                    self.bulk_put(exflag=1)
                    break
        except Exception, e:
            etllog.error('[load_datas.py] LoadDatas [_exception_clean]' + str(e))
