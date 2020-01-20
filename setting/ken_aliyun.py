#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import sys

WHO = 'KEN'


CHROME_DRIVER_PATH = r'/home/ken/prog/chromedriver_linux64/chromedriver' if sys.platform == 'linux' else \
  ''
PHANTOMJS_PATH = r'/home/ken/prog/phantomjs-2.1.1-linux-x86_64/bin/phantomjs'  if sys.platform == 'linux' else \
  ''


from .default import CONFIG as base
class CONFIG(base):
  ALLHS300_STOCKLIST = r'/root/workspace/code/pyStock/backtest-py/doc/沪深300指数历史年分成分股名单.xlsx'
  SAVE_PATH = r'/root/workspace/code/pyStock/backtest-py/doc/dv3'
  BACKTEST_COLLECNAME = 'all_dv2'
  EVERYDAY_STOCKLIST = r'/root/workspace/code/pyStock/backtest-py/doc/dv3detail.xlsx'
  START_YEAR = 0