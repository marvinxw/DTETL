import time
from dwetl.petl.clean_etl_conf import clean_datetime
from dwetl.petl.delta_control import t_delta_conf
from dwetl.petl.etl_task import EtlWork
from dwetl.plog.logtool import etllog
from dwetl.psmtp.smtpmail import mail_main
from dwetl.refresh.refresh_view import RefreshMaterializedView


def main_file():
    start_time = time.time()
    etllog.info('========== [ETL] START TIME: ==========')

    etllog.info('========== [ETL] START TASK ==========')
    etl_work = EtlWork('access_log')
    etl_work.start()
    etllog.info('========== [ETL] FINISH TASK ==========')

    etllog.info('========== [ETL] END TIME: ==========')
    etllog.info('========== [ETL] TIME ELAPSED %s s==========' % (time.time()-start_time))


def main_db(table):
    start_time = time.time()
    etllog.info('========== [ETL] START TIME: %s ==========' % clean_datetime._today())

    etllog.info('========== [ETL] READ DETAL TABLE ==========')
    t_delta_conf._detal_caches()

    etllog.info('========== [ETL] START TASK ==========')
    etl_work = EtlWork(table)
    etl_work.start()
    etllog.info('========== [ETL] FINISH TASK ==========')

    etllog.info('==========[ETL] SAVE DETAL TABLE==========')

    etllog.info('========== [ETL] END TIME: %s ==========' % clean_datetime._today())
    etllog.info('========== [ETL] TIME ELAPSED %s s==========' % (time.time()-start_time))


def main_mail():
    mail_main()


def _refresh_view():
    start_time = time.time()
    etllog.info('========== [ETL] REFRESH MATERIALIZED VIEW START TIME: %s ==========' % clean_datetime._today())

    rmv = RefreshMaterializedView('user_age_group')
    rmv._refresh_start()

    etllog.info('========== [ETL] REFRESH MATERIALIZED VIEW END TIME: %s ==========' % clean_datetime._today())
    etllog.info('========== [ETL] TIME ELAPSED %s s ==========' % (time.time()-start_time))

#
main_db('users')
#
#
# main_file()
#
#
# main_mail()


# _refresh_view()