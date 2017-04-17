# coding=utf-8
import re
import json
import itertools
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from common_file import FileUBase
from dwetl.petl.clean_etl_conf import clean_others_params, clean_datetime
from enum_filter import _interaction_type, _object_id, _user_id, _object_id_2
from dwetl.plog.logtool import etllog
from dwetl.settings import DATAS

"""
    读取文件数据
"""


class ReadFile(FileUBase):
    def __init__(self, file, time_args):
        super(ReadFile, self).__init__(file)
        self.time_args = time_args
        self._max_rows = clean_others_params._get_file_max_rows()
        self.lineno = 0

    def _read_files(self):
        """
            如果没有找到可以解析的文件, 程序退出
            否则
            生成器迭代文件
        """
        try:
            _files = self._get_local_file(self.time_args)
            if len(_files) == 0:
                etllog.warning(u'没有可以解析的文件,程序退出!')
                from dwetl.petl.p_decorator import sys_exit1
                sys_exit1()

            for onefile in _files:
                etllog.lfinfo('========== ETL-LOG-FILE: Read File %s ==========' % onefile)
                etllog.info('========== ETL-LOG-FILE: Read File %s ==========' % onefile)
                with open(onefile, 'rb') as onef:
                    for line in onef:
                        yield line
        except Exception, e:
            etllog.error('[consume_log.py] ReadFile [_read_files]' + str(e))

    def _get_rows(self):
        """
            读取多少行数据
        """
        return itertools.islice(self._rf, self._max_rows)

    def _start(self):
        self._rf = self._read_files()

    def access_log(self):
        # 取相应的数据
        def _rank_replace(line):
            new_line = line.replace('\r\n', '').split(',')
            clean_user_id = _user_id(new_line[3])
            if clean_user_id == 0:
                return
            else:
                return (clean_user_id,
                        new_line[0],
                        5 if new_line[2] == 2 else _interaction_type(new_line[5]),
                        _object_id(new_line[5]),
                        _object_id_2(),
                        new_line[2] if new_line[2] else 0,
                        new_line[5],)

        lines = self._get_rows()
        try:
            # _datas = [_rank_replace(line) for line in lines]
            _datas = map(_rank_replace, lines)
            _datas_clean_none = [_ for _ in _datas if _ != None]
            self.lineno += len(_datas)
            if len(_datas) == 0:
                etllog.lfinfo('========== ETL-LOG-FILE: TOTAL LINENUMBER: %s. ignore UUID: %s ==========' % (self.lineno, _datas.count(None)))
                etllog.info('========== ETL-LOG-FILE: Read File END ==========')
                etllog.lfinfo('========== ETL-LOG-FILE END %s ==========' % clean_datetime._today())
                return 0
            else:
                DATAS.put(_datas_clean_none)
                return DATAS.qsize()
        except Exception, e:
            etllog.error('[consume_log.py] ReadFile [_get_file_datas_to_queue]' + str(e))

    def location_log(self):
        # 取location相应的数据

        def _match_line(line):
            p = re.compile(r'\[.*\]')
            # p.search(line).group()
            new_line = json.loads(p.search(line).group())
            return new_line

        def _rank_replace(line):
            clean_user_id = _user_id(line['userId'])
            if clean_user_id == 0:
                return
            else:
                return (clean_user_id, line['time'], line['latitude'], line['longitude'],)

        lines = self._get_rows()
        try:
            match_datas = map(_match_line, lines)
            # match_datas = [_match_line(line) for line in lines]
            _dict_list_datas = [_ for detail_datas in match_datas for _ in detail_datas]
            # _datas_clean_none = [_rank_replace(_) for _ in _dict_list_datas]
            _datas_clean_none = map(_rank_replace, _dict_list_datas)
            self.lineno += len(match_datas)
            if len(match_datas) == 0:
                etllog.lfinfo('========== ETL-LOG-FILE: TOTAL LINENUMBER: %s, TOTAL Data ==========' % (self.lineno, ))
                etllog.info('========== ETL-LOG-FILE: Read File END ==========')
                etllog.lfinfo('========== ETL-LOG-FILE END %s ==========' % clean_datetime._today())
                return 0
            else:
                DATAS.put(_datas_clean_none)
                return DATAS.qsize()
        except Exception, e:
            etllog.error('[consume_log.py] ReadFile [_get_file_datas_to_queue]' + str(e))

    def _get_file_datas_to_queue(self):
        """
            不同的文件, 执行不同的函数
        """
        return getattr(self, self.file, self.file)()
