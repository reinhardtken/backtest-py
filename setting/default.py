#!/usr/bin/env python
# -*- encoding: utf-8 -*-

class CONFIG:
  ALLHS300_STOCKLIST = r'C:\profile\2020\个人\投资\沪深300指数历史年分成分股名单.xlsx'
  SAVE_PATH = r'C:\workspace\tmp/dv3'
  BACKTEST_COLLECNAME = 'all_dv2'
  EVERYDAY_STOCKLIST = r'C:\workspace\tmp\dv3detail.xlsx'
  FORECAST = False
  CALC_DVYEAR = False

  #动态过滤
  DYNAMIC_FILTER_DVYEAR = True
  DYNAMIC_FILTER_HS300 = True
  
  START_YEAR = 2011
  START_DATE = '2011-01-01T00:00:00Z'
  END_YEAR = 2020
  END_DATE = '2020-12-31T00:00:00Z'