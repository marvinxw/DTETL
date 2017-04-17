#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


___version__ = "0.0.3"
__author__ = "ETL"
__author_email__ = "twen.ma@yo-ren.com"
__url__ = ""
__packages__ = ["dwetl", "dwetl/bin", "dwetl/logfile", "dwetl/petl", "dwetl/refresh", "dwetl/plog", "dwetl/psmtp", "dwetl/settings"]
__scripts__ = ["dwetl/bin/etlbin"]
__license__ = ""
__description__ = "ETL"
__long_description__ = "ETL DB datas and ETL file datas"


setup(name="ETL",
      version=___version__,
      author=__author__,
      author_email=__author_email__,
      url=__url__,
      packages=__packages__,
      scripts=__scripts__,
      license=__license__,
      description=__description__,
      long_description=__long_description__)

