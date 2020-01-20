# -*- coding: utf-8 -*-

# sys
from datetime import datetime
from dateutil import parser
import msvcrt
from cProfile import Profile
import pstats

# thirdpart
import pandas as pd
from pymongo import MongoClient
import numpy as np
import tushare as ts


# this project
from const import stockList
import util
from filter import dvYear
from filter import hs300
from filter import ipoYear
from filter import dvYearAll
from filter import hs300All
import tools
import setting


#########################################################




def RunHS300AndDVYears():
  print(dir(setting.CONFIG))
  codes = []
  client = MongoClient()
  db = client["stock_backtest"]
  # collection = db["all_dv3"]
  collection = db[setting.CONFIG.BACKTEST_COLLECNAME]
  cursor = collection.find({'tradeCounter': {'$gte': 1}})
  # cursor = collection.find()
  for one in cursor:
    # print(one)
    codes.append({'_id': one['_id'], 'name': one['name'], 'percent': one['percent'],
                'holdStockNatureDate': one['holdStockNatureDate'],
                'tradeCounter': one['tradeCounter']})
  
  filter = [
    dvYearAll.Filter,
    # dvYear.Filter,
    hs300All.Filter,
    # hs300.Filter,
    ipoYear.Filter,
  ]
  # inData = out
  # for one in filter:
  #   inData, _ = one(inData)
  #
  #
  #
  # tmp = set(inData)
  # codes = []
  # for one in out:
  #   if one['_id'] in tmp:
  #     codes.append(one)
  #
  # # for one in stockList.VERSION_DV2.DVOK_NOT_HS300:
  # #   if one['_id'] not in in2:
  # #     codes.append(one)
  # #
  # # for one in stockList.VERSION_DV2.HS300_NOT_DVOK:
  # #   if one['_id'] not in in2:
  # #     codes.append(one)
  #
  # print('### final backtest stock list size {}'.format(len(codes)))
  # empty = []
  # for one in codes:
  #   print(one)
  #   tmp = util.LoadData('stock_statistcs_dvYears', one['_id'])
  #   if tmp is None:
  #     print('no dvdata !!! {}'.format(one))
  #     empty.append(one)
  #
  # tools.DoAction(empty, tools.CalcDV)
      

  # codes = [
  #   {'_id': '600252', 'name': '中恒集团'},
  #   {'_id': '601633', 'name': '长城汽车'},
  #   {'_id': '600795', 'name': '国电电力'},
  #   {'_id': '600004', 'name': '白云机场'},
  #   {'_id': '600177', 'name': 	'雅戈尔'},
  # ]
  tools.DoBacktest(codes,
                   {'check': False, 'backtest': True, 'saveDB': 'all_dv3', 'draw': None,
                    'saveFile': setting.CONFIG.SAVE_PATH,
                    'saveSignal': 'stock_signal_dv3'}, filter)




#########################################################
###这个文件每天执行一次，只回测runEveryWeek的输出结果集合，刷新最新的回测结果
if __name__ == '__main__':
  #对全部股票标的中，产生过交易并且属于沪深300，并且分红年份达标的标的回测
  # ncalls，是指相应代码 / 函数被调用的次数；
  # tottime，是指对应代码 / 函数总共执行所需要的时间（注意，并不包括它调用的其他代码 / 函数的执行时间）；
  # percall，就是上述两者相除的结果，也就是 tottime / ncalls;
  # cumtime，则是指对应代码 / 函数总共执行所需要的时间，这里包括了它调用的其他代码 / 函数的执行时间；
  # cumtime percall，则是 cumtime 和 ncalls 相除的平均结果。

  prof = Profile()
  prof.enable()
  RunHS300AndDVYears()
  prof.create_stats()

  p = pstats.Stats(prof)
  # p.print_stats()
  # p.sort_stats('calls').print_stats(20)
  p.sort_stats('cumulative').print_stats(20)
  # p.print_callees()
  # cProfile.run('RunHS300AndDVYears()')
  # RunHS300AndDVYears()
  #ch = msvcrt.getch()

  