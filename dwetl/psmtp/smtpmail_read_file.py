# coding=utf-8
import os
import smtplib
import os.path
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from email import encoders
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.header import Header
from email.utils import parseaddr, formataddr
from dwetl.petl.clean_etl_conf import clean_others_params, clean_datetime
from dwetl.plog.logtool import etllog
from dwetl.settings import EMAIL_SEND


class smtpMail(object):
    def __init__(self):
        self._mail = MIMEMultipart('alternative')
        self.pos = 0
        self.err = []
        self.hours = 24

    @classmethod
    def _format_addr(cls, s):
        name, addr = parseaddr(s)
        return formataddr((Header(name, 'utf-8').encode(), addr.encode('utf-8') if isinstance(addr, unicode) else addr))

    def _mail_header(self, FROM_ADDR, TO_ADDR, SUBJECT):
        FROM = ','.join([FROM_ADDR, '<'+FROM_ADDR+'>'])
        TO = ','.join(TO_ADDR)
        self._mail['From'] = self._format_addr(FROM)
        self._mail['To'] = TO
        self._mail['Subject'] = Header(SUBJECT, charset='UTF-8') # 邮件主题

    def _mail_contents(self):

        def _text_plain():
            msg = 'Success day'
            return msg

        def _html_plain():

            try:
                msg = '<h3> [ETL] 程序出现异常 (%s:  %s)</h3>' % EMAIL_SEND.items()[0]
            except:
                msg = '<h3> [ETL] 程序出现异常 </h3>'

            msg += '<h3> [ETL] 时间: %s </h3>' % clean_datetime._today()
            errmsg = msg + '\r\n'.join(['<p>' + _ + '</p>' for _ in self.err])
            return errmsg

        # 添加邮件内容
        # txt = MIMEText(_text_plain(), _subtype='plain', _charset='UTF-8')
        # self._mail.attach(txt)
        # 添加html的邮件内容
        txt = MIMEText(_html_plain(), _subtype='html',  _charset='UTF-8')
        self._mail.attach(txt)

    def _mail_attachment(self, file_name):
        # 构造MIMEBase对象做为文件附件内容并附加到根容器
        # 读入文件内容并格式化
        with open(file_name, 'rb') as f:
            # 设置附件头
            basename = os.path.basename(file_name)
            file_msg = MIMEBase('application', 'octet-stream', filename=basename)
            file_msg.add_header('Content-Disposition', 'attachment', filename=basename)
            file_msg.add_header('Content-ID', '<0>')
            file_msg.add_header('X-Attachment-Id', '0')
            file_msg.set_payload(f.read())
            encoders.encode_base64(file_msg)
            self._mail.attach(file_msg)

    def _mail_send(self, FROM_ADDR=None, PASSWORD=None, TO_ADDR=None, SUBJECT=None, SMTP_SERVER=None, PORT=None, ATTACHMENT=None):
        self._mail_header(FROM_ADDR, TO_ADDR, SUBJECT)
        self._mail_contents()

        if ATTACHMENT:
            self._read_log_lastn()
            self._mail_attachment(ATTACHMENT)

        try:
            #发送邮件
            smtp = smtplib.SMTP()
            smtp.set_debuglevel(1)
            smtp.connect(SMTP_SERVER, PORT)
            smtp.starttls()
            smtp.login(FROM_ADDR, PASSWORD)
            smtp.sendmail(FROM_ADDR, TO_ADDR, self._mail.as_string())
            smtp.quit()
        except Exception, e:
            etllog.error('smtpMail [_mail_send]' + str(e))

    def _read_log_lastn(self):
        with open(clean_others_params._get_error_log(), 'rb') as f:
            for line in range(100):
                while 1:
                    self.pos = self.pos - 1
                    try:
                        f.seek(self.pos, 2)
                        if f.read(1) == '\n':
                            break
                    except:
                        f.seek(0, 0)
                        self.err.append(f.readline().strip())
                        return
                self.err.append(f.readline().strip())

    def check_time(self):
        """
            如果日志的最后几行的时间, 在[1-n]小时内就发邮件
        """
        self._read_log_lastn()
        self.err.reverse()
        try:
            if self.err and self.err != ['']:
                index = -1
                while 1:
                    last_line = self.err[index].strip()
                    try:
                        last_line_time = last_line.split(',')[0]
                        file_time = clean_datetime._time_strptime(last_line_time, '2')
                        if file_time or index == -99:
                            break
                    except:
                        index -= 1
                        continue

                hours_delta = clean_datetime._time_delta(self.hours)
                ago_time = clean_datetime._now() - hours_delta

                if file_time < clean_datetime._now() and file_time > ago_time:
                    self._mail_send(**clean_others_params._mail_conf())
                    print u"邮件已经发出, 请查收"
                else:
                    print u"没有最新的错误邮件"
            else:
                print u"没有最新的错误邮件"
        except Exception, e:
            print u"smtpmail exception", str(e)

def mail_main():
    sms = smtpMail()
    sms.check_time()
