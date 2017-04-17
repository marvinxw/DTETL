# coding=utf-8
from dwetl.plog.logtool import etllog


class EtlExceptionBase(Exception):
    def __init__(self, type, message):
        self.type = type
        self.message = message

    def get_info(self):
        _msg = "(\"%s\" \"%s\")\n" % (self.type, self.message)
        etllog.error(_msg)
        return _msg


class ParamsError(EtlExceptionBase):

    """
    参数传入异常
    """

    def __init__(self, type, message):
        super(ParamsError, self).__init__(type, message)

    def __str__(self):
        return "ParamsError %s" % (self.get_info())


class FileNotFind(EtlExceptionBase):
    """
    文件没找到
    """

    def __init__(self, type, message):
        super(FileNotFind, self).__init__(type, message)

    def __str__(self):
        return "FileNotFind %s" % (self.get_info())

