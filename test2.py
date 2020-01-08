# -*- coding: utf-8 -*-

# sys
from datetime import datetime
from dateutil import parser

# thirdpart
import pandas as pd
from pymongo import MongoClient
import numpy as np
import matplotlib.pyplot as plt


import const
import util
import strategy.dv1


def Test2(code, beginMoney, name, save=False, check=False):
  import strategy.dvBase
  stock = strategy.dvBase.TradeUnit(code, name, beginMoney)
  stock.LoadQuotations()
  stock.LoadIndexs()
  stock.Merge()
  stock.CheckPrepare()
  
  print(stock.checkPoint)
  print(stock.dangerousPoint)
  for one in stock.dividendPoint:
    print(one)
  
  stock.BackTest()
  stock.CloseAccount()
  if save:
    stock.Store2DB()
  
  if check:
    assert stock.CheckResult()




def TestAll(codes, save, check):
  for one in codes:
    if len(one) == 3:
      Test2(one['code'], one['money'], one['name'], save, check)
    else:
      Test2(one['code'], 100000, one['name'], save, check)


if __name__ == '__main__':
  # {'name': '东风股份', '_id': '601515', 'money': 133705},
  # Test2('600971', 100000, '浙江永强', False, True)
  ##上证红利50
  # BONUS_CODES = const.stockList.BONUS_CODES
  # VERIFY_CODES = const.stockList.VERIFY_CODES
  # check
  # TestAll(VERIFY_CODES, True, False)
  # for row in BONUS_CODES:
  #   stock = strategy.dv1.TradeUnit(row['code'], row['name'], 100000)
  #   stock.collectionName = 'bonus50_dv1'
  #   stock.LoadQuotations()
  #   stock.LoadIndexs()
  #   stock.Merge()
  #   stock.CheckPrepare()
  #
  #   print(stock.checkPoint)
  #   print(stock.dangerousPoint)
  #   for one in stock.dividendPoint:
  #     print(one)
  #
  #   stock.BackTest()
  #   stock.CloseAccount()
  #   stock.Store2DB()
  # 沪深300计算#############################################
  # df = util.QueryHS300All()
  # out = []
  # for code, row in df.iterrows():
  #   stock = strategy.dv1.TradeUnit(code, row['股票名称'], 100000)
  #   stock.collectionName = 'hs300_dv1'
  #   stock.LoadQuotations()
  #   stock.LoadIndexs()
  #   stock.Merge()
  #   stock.CheckPrepare()
  #
  #   print(stock.checkPoint)
  #   print(stock.dangerousPoint)
  #   for one in stock.dividendPoint:
  #     print(one)
  #
  #   stock.BackTest()
  #   stock.CloseAccount()
  #   stock.Store2DB()

  #沪深300统计##############################################
  # df = util.QueryHS300All()
  # out = []
  # for code, row in df.iterrows():
  #   out.append({'code': code, 'name': row['股票名称']})
  #
  # strategy.dv1.CompareAll('hs300_dv1', out)

  # 作图



  df = util.LoadData('stock_result', 'dv_jusths300_w')
  df[['total', 'capital']].plot()
  plt.show()
  #全部股票################################################
  # df = util.QueryAll()
  # out = []
  # client = MongoClient()
  # db = client['stock_backtest']
  # collection = db['all_dv1']
  # cursor = collection.find()
  # already = set()
  # for c in cursor:
  #   already.add(c['_id'])
  #
  # for code, row in df.iterrows():
  #   # if code in already:
  #   #   continue
  #   stock = strategy.dv1.TradeUnit(code, row['名称'], 100000)
  #   stock.collectionName = 'all_dv1'
  #   stock.LoadQuotations()
  #   stock.LoadIndexs()
  #   stock.Merge()
  #   stock.CheckPrepare()
  #
  #   print(stock.checkPoint)
  #   print(stock.dangerousPoint)
  #   for one in stock.dividendPoint:
  #     print(one)
  #
  #   stock.BackTest()
  #   stock.CloseAccount()
  #   stock.Store2DB()

  # df = util.QueryAll()
  # out = []
  # for code, row in df.iterrows():
  #   out.append({'code': code, 'name': row['名称']})
  #
  # strategy.dv1.CompareAll('all_dv1', out)