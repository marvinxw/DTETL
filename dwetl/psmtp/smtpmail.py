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


class smtpMail(object):
    def __init__(self):
        self._mail = MIMEMultipart('alternative')
        self._log_message = clean_others_params._get_log_message()

    @classmethod
    def _format_addr(cls, s):
        name, addr = parseaddr(s)
        return formataddr((Header(name, 'utf-8').encode(), addr.encode('utf-8') if isinstance(addr, unicode) else addr))

    def _mail_header(self, FROM_ADDR, TO_ADDR):

        def subject():
            """
                @邮件主题
            """
            res_msg = "ETL-Lawson结果:OK"
            if len(self._log_message['ERROR']) > 0:
                res_msg = "ETL-Lawson结果:NG"
            SUBJECT = res_msg + " - 表|视图： '%s' （周期：1日/1次、执行时间： '%s'）" % (self._log_message['TABLE_OR_VIEW'], clean_datetime._today())
            return SUBJECT

        FROM = ','.join([FROM_ADDR, '<'+FROM_ADDR+'>'])
        TO = ','.join(TO_ADDR)
        self._mail['From'] = self._format_addr(FROM)
        self._mail['To'] = TO
        self._mail['Subject'] = Header(subject(), charset='UTF-8')

    def _mail_contents(self):

        def _text_plain():
            msg = '-----需补充-----'
            return msg

        def _html_plain():

            msg = """
                <div>
                -------------任务执行输出-------------
                %s
                </div>
                <br />
                <div>
                -------------错误内容-------------
                %s
                </div>
            """ % ('\r\n'.join(['<p>' + _ + '</p>' for _ in self._log_message['INFO']]),
                   '\r\n'.join(['<p>' + _ + '</p>' for _ in self._log_message['ERROR']]))

            return msg

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

    def _mail_send(self, FROM_ADDR=None, PASSWORD=None, TO_ADDR=None, SMTP_SERVER=None, PORT=None, ATTACHMENT=None):
        """
            @FROM_ADDR 邮件发送人
            @PASSWORD 邮件发送人密码
            @TO_ADDR 发送给
            @SMTP_SERVER SMTP服务
            @PORT 端口号
            @ATTACHMENT 附件
        """
        self._mail_header(FROM_ADDR, TO_ADDR)
        self._mail_contents()

        if ATTACHMENT:
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

def mail_main():
    sms = smtpMail()
    sms._mail_send(**clean_others_params._mail_conf())
