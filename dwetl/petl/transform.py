# coding=utf8
from dwetl.plog.logtool import etllog
from dwetl.settings import DATAS


"""
    数据类型转换
"""


class DBFieldClean(object):
    """
        mysql 取出的数据,会被转成python的语法, 导致postgresql不能直接插入, 需要清洗

        INSERT INTO
            users(user_id,gender,birthday,register_date,district,province)
        VALUES
        (60415L, 2L, u'1985-06-11', u'2015-01-09 13:25:59', u'003', u'002'),
        (60416L, 2L, u'1988-06-15', u'2015-01-09 13:30:27', u'003', u'002'),
        (60417L, 2L, u'1992-10-15', u'2015-01-09 13:44:22', u'003', u'002');

        ===>

        INSERT INTO
            users(user_id,gender,birthday,register_date,district,province)
        VALUES
        (60415, 2, '1985-06-11', '2015-01-09 13:25:59', '003', '002'),
        (60416, 2, '1988-06-15', '2015-01-09 13:30:27', '003', '002'),
        (60417, 2, '1992-10-15', '2015-01-09 13:44:22', '003', '002');
    """
    def __init__(self):
        try:
            self.datas = DATAS.get(timeout=2)
        except Exception, e:
            etllog.error('[transform.py] DBFieldClean [__init__] DATAS.get(timeout=2)' + str(e))

    def _new_row_check(self, onerow):
        # todo
        _new = None
        try:
            if onerow[1] == 'B':
                _new = bytes(onerow[0])
            else:
                if onerow[0] == None or onerow[0] == '':
                    _new = onerow[0]
                else:
                    _new = eval(onerow[1])(onerow[0])
        except Exception, e:
            etllog.error('[transform.py] DBFieldClean [_new_row_check]' + str(e))
        return _new

    def _clean_p2postgre(self, field_type):
        try:
            _DATAS = [tuple(map(self._new_row_check, zip(odata, field_type))) for odata in self.datas]

            return _DATAS
        except Exception, e:
            etllog.error('[transform.py] DBFieldClean [_clean_p2postgre]' + str(e))
