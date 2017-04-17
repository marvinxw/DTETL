import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from dwetl.petl.DB_conn import ConnectDB
from dwetl.petl.clean_etl_conf import clean_etl_conf, clean_refresh_view, clean_others_params


class RefreshMaterializedView(ConnectDB):

    def __init__(self, view):
        super(RefreshMaterializedView, self).__init__(clean_etl_conf._get_from_to("TO"))
        self.view = view
        self._refresh_view = clean_refresh_view._get_refresh()
        self._log_message = clean_others_params._get_log_message()

    def _refresh_start(self):
        try:
            self.cursor.execute(self._refresh_view[self.view])
            self.conn.commit()
            self._log_message['TABLE_OR_VIEW'] = self.view
        except Exception, e:
            print str(e)
