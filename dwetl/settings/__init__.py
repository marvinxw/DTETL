# coding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
from Queue import Queue


# 参数
CONFIG = None
DB_INFO = None
DB_TABLE_STRUCTURE = None
FILE_SERVER = None
OTHERS_PARAMS = None
REFRESH_VIEW = None

# datas
DATAS = Queue()

# 增量
_DETAL_CACHES = None

# log time args
LOG_TIME_PARAMS = []


# if must, use settings.json instead config.py
# dev
# __package_json__ = 'D:/yoren-project/2017-mysql-To-postgres/lawson_etl/tblEtl/dwetl/settings/settings.json'

# prod
__package_json__ = '/data/lawson_etl/tblEtl/dwetl/settings/settings.json'


def global_setting():
    global CONFIG, DB_INFO, DB_TABLE_STRUCTURE, FILE_SERVER, OTHERS_PARAMS, REFRESH_VIEW

    if not CONFIG:
        with open(__package_json__, 'rb') as js:
            CONFIG = json.loads(js.read())

    DB_INFO = CONFIG.get('DB_INFO')
    DB_TABLE_STRUCTURE = CONFIG.get('DB_TABLE_STRUCTURE')
    FILE_SERVER = CONFIG.get('FILE_SERVER')
    OTHERS_PARAMS = CONFIG.get('OTHERS_PARAMS')
    REFRESH_VIEW = CONFIG.get('REFRESH_VIEW')

try:
    # from dev import *
    from prod import *
    
    # 不使用settings.json配置文件
    # try:
    #     # global_setting()
    #     pass
    # except:
    #     # from prod import *
    #     pass
except:
    from dwetl.petl.p_decorator import sys_exit1
    sys_exit1()
