#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import sys

WHO = 'KEN'


CHROME_DRIVER_PATH = r'/home/ken/prog/chromedriver_linux64/chromedriver' if sys.platform == 'linux' else \
  ''
PHANTOMJS_PATH = r'/home/ken/prog/phantomjs-2.1.1-linux-x86_64/bin/phantomjs'  if sys.platform == 'linux' else \
  ''


class PATH:
  ALLHS300_STOCKLIST = r'C:\profile\2020\个人\投资\沪深300指数历史年分成分股名单.xlsx'