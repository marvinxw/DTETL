# coding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import MySQLdb
import psycopg2
from clean_etl_conf import clean_etl_conf, clean_db_info
from dwetl.plog.logtool import etllog


class ConnectDB(object):

    """
        连接数据库
    """

    def __init__(self, dbtype):

        DB = {
            clean_etl_conf._get_from_to('FROM'): (MySQLdb, clean_db_info._get_mysql_conn_info()),
            clean_etl_conf._get_from_to('TO'): (psycopg2, clean_db_info._get_postgresql_conn_info())
        }[dbtype]

        try:
            self.conn = DB[0].connect(**DB[1])
            self._conn_cursor()
        except DB[0].Error as e:
            etllog.error("[DB_conn.py] ConnectDB %s __init__ Error =====> %s" % (DB[0], str(e)))
            from dwetl.petl.p_decorator import sys_exit1
            sys_exit1()

    def query(self, sql):
        """
            mysql select
        """
        self.cursor.execute(sql)

    def insert(self, sql):
        """
            postgresql insert
        """
        self.cursor.execute(sql)
        self.conn.commit()

    def delete(self, sql):
        """
            postgresql delete
        """
        # self.insert(sql)
        self.cursor.execute(sql)
        self.conn.commit()

    def fetchmanyrows(self, rows):
        """
            mysql fetchmany
        """
        return self.cursor.fetchmany(rows)

    def fetchallrows(self):
        """
            mysql fetchall
        """
        return self.cursor.fetchall()

    def _roll_back(self):
        """
            postgresql rollback
        """
        return self.conn.rollback()

    @property
    def mogrify_post(self):
        """
            postgreSQL mogrify
        """
        return self.cursor.mogrify

    def _conn_cursor(self):
        self.cursor = self.conn.cursor()

    def _close_cursor(self):
        self.cursor.close()

    def __del__(self):
        """
            database close
        """
        try:
            self.cursor.close()
            self.conn.close()
        except Exception, e:
            etllog.error('[DB_conn.py] ConnectDB [__del__]' + str(e))

    def _close(self):
        self.__del__()
