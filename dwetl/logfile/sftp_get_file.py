# coding=utf-8
import json
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from common_file import FileUBase
from dwetl.plog.logtool import etllog
from dwetl.petl.UD_Exception import FileNotFind
from dwetl.petl.clean_etl_conf import clean_file_server, clean_datetime


"""

1. 多台服务器
2. 多个目录

"""


class SftpGFile(FileUBase):
    def __init__(self, file, time_args):
        """
            @:file 获取file类型的文件, 如果有
            @:time_args 获取指定时间的文件, 如果有
        """
        super(SftpGFile, self).__init__(file)
        self.time_args = time_args
        self.server = clean_file_server._get_server_info()

    def _get_remote_file(self, time_str):
        file_time = time_str if time_str else clean_datetime._yesterday()
        remote_files = [path + file for path in self._remote_path for file in self.sftp.listdir(path) if self._file_format + file_time in file]
        if len(remote_files) == 0:
            raise FileNotFind('[sftp_get_file.py] FileUBase _get_remote_file', u'没有这个文件请检查')
        return remote_files

    def _get(self):
        try:
            if len(self._get_local_file(self.time_args)) != 0:
                # 如果有当前的文件, 属于执行过的文件
                etllog.error(' 文件已经执行过, 请在本地删除在执行! [* 不删除DB中相应的数据会有主键错误!]')
                from dwetl.petl.p_decorator import sys_exit1
                sys_exit1()

            for _server_info in self.server: # 多台服务器
                self._conn_server(_server_info)
                etllog.info('========== ETL-LOG-FILE:Conn Server %s ==========' % (json.dumps(_server_info.get('host'))))
                etllog.lfinfo('========== ETL-LOG-FILE:Server %s ==========' % (json.dumps(_server_info.get('host'))))
                for f in self._get_remote_file(self.time_args): # 多个目录文件
                    # 放文件
                    # 一对一的
                    fname = f.split('/')[-1]
                    fpath = '/'.join(f.split('/')[:-1]) + '/'
                    index = self._remote_path.index(fpath)
                    self.sftp.get(f, self._local_path[index]+fname)
                    etllog.lfinfo('========== ETL-LOG-FILE: Get File remote %s local %s==========' % (f, self._local_path[index]+fname))
                    etllog.info('========== ETL-LOG-FILE: Get File remote %s local %s==========' % (f, self._local_path[index]+fname))
            self._close()
        except Exception, e:
            etllog.error('[sftp_get_file.py] SftpGFile [_get]' + str(e))

    def _close(self):
        self.pt.close()

    def _start(self):
        self._get()
