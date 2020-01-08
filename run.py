# -*- coding: utf-8 -*-

# sys
from datetime import datetime
from dateutil import parser

# thirdpart
import pandas as pd
from pymongo import MongoClient
import numpy as np
import tushare as ts

import const
import util
from filter import dvYear
from filter import hs300

# this project
if __name__ == '__main__':
  import sys


#########################################################
def TestThree(codes, beginMoney, args):
  import strategy.dv3
  
  stock = strategy.dv3.TradeManager(codes, beginMoney)
  stock.LoadQuotations()
  stock.LoadIndexs()
  stock.Merge()
  stock.CheckPrepare()
  
  if 'saveprepare' in args and args['saveprepare']:
    stock.StorePrepare2DB()
  
  if 'backtest' in args and args['backtest']:
    stock.BackTest()
    stock.CloseAccount()
  
  if 'saveDB' in args:
    stock.StoreResult2DB(args['saveDB'])
  
  if 'check' in args and args['check']:
    assert stock.CheckResult()
  
  if 'draw' in args:
    stock.Draw()
  
  if 'saveFile' in args:
    stock.Store2File(args['saveFile'])
  
  return stock



def RunHS300AndDVYears():
  out = []
  client = MongoClient()
  db = client["stock_backtest"]
  # collection = db["all_dv3"]
  collection = db["dv2"]
  cursor = collection.find({'tradeCounter': {'$gte': 1}})
  # cursor = collection.find()
  for one in cursor:
    # print(one)
    out.append({'_id': one['_id'], 'name': one['name'], 'percent': one['percent'],
                'holdStockNatureDate': one['holdStockNatureDate'],
                'tradeCounter': one['tradeCounter']})
  
  inList, outList = dvYear.Filter(out)
  in2, out2 = hs300.Filter(inList)
  in3, out3 = hs300.Filter(outList)
  
  for one in out:
    if one['_id'] in out2:
      print('not hs300 {} {}'.format(one['_id'], one['name']))
  
  for one in out:
    if one['_id'] in in3:
      print('not dvYear {} {}'.format(one['_id'], one['name']))
  
  codes = []
  for one in out:
    if one['_id'] in in2:
      codes.append(one)
  
  for one in stockList.VERSION_DV2.DVOK_NOT_HS300:
    if one['_id'] not in in2:
      codes.append(one)
  
  for one in stockList.VERSION_DV2.HS300_NOT_DVOK:
    if one['_id'] not in in2:
      codes.append(one)
  
  print('### final backtest stock list size {}'.format(len(codes)))
  for one in codes:
    print(one)
  # TestThree(codes, 100000,
  #           {'check': False, 'backtest': True, 'saveDB': 'all_dv3', 'draw': None, 'saveFile': 'C:/workspace/tmp/dv3'})
  TestThree(codes, 100000,
            {'check': False, 'backtest': True, 'saveDB': 'all_dv3', 'draw': None, 'saveFile': r'd:/stock_python/out/dv3'})





if __name__ == '__main__':
  import strategy.dv3
  from const import stockList
  from fund_manage import hold
  
  
  #下面这段代码是为了生成分红年数数据，因为用这个做了过滤器。
  #如果不用分红年份做过滤器，可以不执行，执行一次即可
  # codes = []
  # df = util.QueryAll()
  # for code, row in df.iterrows():
  #   codes.append({'_id': code, 'name': row['名称']})
  #
  # strategy.dv3.CalcDV(codes)
  
  
  #对全部股票标的中，产生过交易并且属于沪深300，并且分红年份达标的标的回测
  RunHS300AndDVYears()
  
  # TestThree(
  #         # [
  #         #   {'_id': '600025', 'name': '华能水电', },
  #         #   {'_id': '601166', 'name': '兴业银行', 'money': 90205},
  #         #   {'_id': '600900', 'name': '长江电力', 'money': 63905},
  #         #  ],
  #   [
  #     # {'name': '东风股份', '_id': '601515', 'money': 133705},
  #     {'name': '皖通高速', '_id': '600012', 'money': 52105},
  #     {'name': '重庆水务', '_id': '601158', 'money': 58105},
  #     # {'name': '浦发银行', '_id': '600000', 'money': 74505},
  #     # {'name': '万科', '_id': '000002', 'money': 72705},
  #     # {'name': '宝钢股份', '_id': '600019', 'money': 70705},
  #     # {'name': '中国石化', '_id': '600028', 'money': 74405},
  #     # {'name': '双汇发展', '_id': '000895', 'money': 211205},
  #     #  {'name': '伟星股份', '_id': '002003', 'money': 80805},
  #   ],
  #   100000, {'check': False, 'backtest': True, 'saveDB': 'all_dv3', 'draw': None, 'saveFile': 'C:/workspace/tmp/dv3'})
  