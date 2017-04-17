# coding=utf-8
import datetime
from dwetl.settings import DB_TABLE_STRUCTURE, OTHERS_PARAMS, FILE_SERVER, DB_INFO, REFRESH_VIEW


'''
    配置文件的解析
'''


class CleanEtlConf(object):

    def __init__(self):
        """
            因为字典是没有顺序的，但也不是乱序的

            print DB_TABLE_STRUCTURE   >   此时TO-postgresql在前, FROM-mysql在后。因为只有2个元素，保持这个顺序使用就行

            后面要是有多个，最好加一个字典排序
        """
        # todo
        self.etl_table_conf = DB_TABLE_STRUCTURE

    def _common(self, type, i):
        """
            这里传i更通用
        """
        return {key: value.get(type) for tables_info in self.etl_table_conf.values()[i].values() for key, value in tables_info.items()}

    def _get_from_to(self, key):
        """
            @FROM
            @TO
        """
        etype = {etype.split('-')[0]: etype.split('-')[1] for etype in self.etl_table_conf.keys()}
        return etype[key]

    def _get_groups(self):
        """
            group
        """
        groups = {groups for type in self.etl_table_conf.keys() for groups in self.etl_table_conf[type].keys()}
        return groups

    def _get_group_tables(self, group):
        """
            group -> tables
        """
        return self.etl_table_conf.get('TO-postgresql').get(group).keys()

    def _get_tables(self):
        """
            table
        """
        tables = {table for db_info in self.etl_table_conf.values() for tables_info in db_info.values() for table in tables_info.keys()}
        return tables

    def _get_sql_query(self, table):
        """
            select
        """
        _sql_query = self._common('_sql_query', 1)
        return _sql_query[table]

    def _get_field(self, table):
        """
            field
        """
        _filed = self._common('field', 0)
        return _filed[table]

    def _get_fieldType(self, table):
        """
            fieldType
        """
        _filed_type = self._common('fieldType', 0)
        return _filed_type[table]

    # def _get_delta_field(self, table):
    #     """
    #         delta_field
    #     """
    #     _delta_field = self._common('delta_field', 0)
    #     return _delta_field[table]

    def _get_delta_table(self, table):
        """
            delta_field
        """
        _delta_field = self._common('delta_table', 0)
        return _delta_field[table]

    def _get_log_group(self):
        """
            log table
        """
        return self.etl_table_conf.get('TO-postgresql').get('LOGS-GROUP').keys()

    def _get_table_primarykey(self, table):
        """
            获得表的主键
        """
        primarykey = self._common('primarykey', 0)
        return primarykey[table]

clean_etl_conf = CleanEtlConf()


class CleanDBInfo(object):
    def __init__(self):
        self.DB_INFO = DB_INFO

    def _get_mysql_conn_info(self):
        return self.DB_INFO.get('MYSQL_CONN_INFO')

    def _get_postgresql_conn_info(self):
        return self.DB_INFO.get('POSTGRESQL_CONN_INFO')

clean_db_info = CleanDBInfo()


class CleanFileServer(object):
    def __init__(self):
        self.file_server = FILE_SERVER

    def _get_server(self):
        return self.file_server.keys()

    def _get_server_info(self):
        return self.file_server.get('server').values()

    def _get_file_path(self):
        return self.file_server.get('remotepath'), self.file_server.get('localpath')

    def _get_file_format(self, filetype):
        """
            @:filetype 文件
        """

        file_type_conf = self.file_server['file_type_conf']

        return file_type_conf[filetype]

    def _set_file_or_table(self, type=None):
        """
            @:type 文件
        """
        _file_table = self.file_server.get('file_table_mapping')
        if type:
            # 文件
            return _file_table[type]
        else:
            # 表
            return _file_table.keys()

clean_file_server = CleanFileServer()


class CleanOthersParams(object):
    def __init__(self):
        self.others_params = OTHERS_PARAMS

    def _fetch_table_rows(self):
        return self.others_params.get('SEARCH_TABLE_ROWS')

    def _mail_conf(self):
        return self.others_params.get('MAIL_CONF')

    def _detal_num(self):
        return self.others_params.get('DETAL_NUM')

    def _get_file_max_rows(self):
        return self.others_params.get('READ_FILE_MAX_ROWS')

    def _get_version(self):
        return self.others_params.get('___VERSION__')

    def _get_error_log(self):
        return self.others_params.get('ERROR_LOG')

    def _get_log_config(self):
        return self.others_params.get('LOG_CONFIG')

    def _get_log_message(self):
        return self.others_params.get('LOG_MESSAGE')

    def _get_update_tables(self):
        """
            update table
        """
        return self.others_params.get('UPDATE_TABLES')

clean_others_params = CleanOthersParams()


class CleanDatetime(object):
    def _today(self):
        return datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    def _time_plus_second(self, timeargs):
        if isinstance(timeargs, str) or isinstance(timeargs, unicode):
            try:
                if len(timeargs) > 10:
                    timeargs = self._time_strptime(timeargs, '2')
                else:
                    timeargs = self._time_strptime(timeargs, '1')
            except:
                return timeargs
        return str(timeargs+datetime.timedelta(seconds=1))

    def _yesterday(self):
        return (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')

    def _time_strptime(self, timestr, format):
        """
            str -> datetime type
        """
        _d = {
            '1': "%Y%m%d",
            '2': "%Y-%m-%d %H:%M:%S",
            '3': "%Y-%m-%d"
        }
        return datetime.datetime.strptime(timestr, _d[format])

    def _time_delta(self, value):
        return datetime.timedelta(hours=value)

    def _now(self):
        return datetime.datetime.now()

clean_datetime = CleanDatetime()


class CleanRefreshView(object):
    def __init__(self):
        self._refresh_view = REFRESH_VIEW

    def _get_refresh(self):
        return self._refresh_view

clean_refresh_view = CleanRefreshView()
