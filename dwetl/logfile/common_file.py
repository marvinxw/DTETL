# coding=utf-8
import paramiko
import os
from dwetl.petl.clean_etl_conf import clean_file_server, clean_datetime
from dwetl.plog.logtool import etllog


class FileUBase(object):
    def __init__(self, file):
        self.file = file
        self._remote_path, self._local_path = clean_file_server._get_file_path()
        self._file_format = clean_file_server._get_file_format(file)

    def _get_local_file(self, time_str=None):
        """
            @:time_str 读取这个时间的文件
        """
        file_time = time_str if time_str else clean_datetime._yesterday()
        return [path + f for path in self._local_path for f in os.listdir(path) if self._file_format + file_time in f]

    def _conn_server(self, _server_info):
        """
            sftp, 下载服务器文件到本地
        """
        try:
            self.pt = paramiko.Transport((_server_info['host'], _server_info['port']))
            self.pt.connect(username=_server_info['username'], password=_server_info['password'])
            self.sftp = paramiko.SFTPClient.from_transport(self.pt)
        except Exception, e:
            etllog.error('[common_file.py] FileUBase [_get]' + str(e))
