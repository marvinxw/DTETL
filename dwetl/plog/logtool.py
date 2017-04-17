# coding=utf-8
import logging
import logging.config
from dwetl.petl.clean_etl_conf import clean_others_params, clean_datetime


"""
    log格式: 2016-09-26 18:32:07,629 [ERROR] logtool.py[line:26] error message
"""


class EtlBatchLog(object):
    def __init__(self):
        self.ilogger_name = 'infoLogger' # 上下文日志
        self.elogger_name = 'errorLogger' # 错误日志
        self.llogger_name = 'logfileLogger' # 文件log
        self.blogger_name = 'bashLogger' # 输出到terminal
        logging.config.fileConfig(clean_others_params._get_log_config())
        self._log_message = clean_others_params._get_log_message()

    @property
    def ilogger(self):
        return logging.getLogger(self.ilogger_name)

    @property
    def elogger(self):
        return logging.getLogger(self.elogger_name)

    @property
    def lflogger(self):
        return logging.getLogger(self.llogger_name)

    @property
    def blogger(self):
        return logging.getLogger(self.blogger_name)

    def error(self, msg):
        self.elogger.error(msg)
        self.ilogger.error(msg)
        self.blogger.error(msg)
        self._log_message['ERROR'].append(clean_datetime._today() + msg)

    def info(self, msg):
        self.ilogger.info(msg)
        self.blogger.info(msg)
        self._log_message['INFO'].append(clean_datetime._today() + msg)

    def warning(self, msg):
        self.ilogger.warning(msg)
        self.blogger.warning(msg)
        self._log_message['WARNING'].append(clean_datetime._today() + msg)

    def debug(self, msg):
        self.ilogger.debug(msg)
        self.blogger.debug(msg)

    def critical(self, msg):
        self.ilogger.critical(msg)
        self.blogger.critical(msg)

    def lfinfo(self, msg):
        self.lflogger.info(msg)
        self.blogger.info(msg)

etllog = EtlBatchLog()
