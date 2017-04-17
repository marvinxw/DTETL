# coding=utf8
import sys
import functools
from UD_Exception import ParamsError
from dwetl.psmtp.smtpmail import mail_main


# 程序运行,需要特定类型的参数, 这个修饰器可以通用
def check_params(*type):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if False in [isinstance(bl[0], bl[1]) for bl in zip(args[1:], type)]:
                raise ParamsError(type, func)
            return func(*args, **kwargs)
        return wrapper
    return decorator


def sys_exit1():
    mail_main()
    sys.exit(1)
