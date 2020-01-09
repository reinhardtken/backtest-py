# -*- coding: utf-8 -*-

# sys
from datetime import datetime
from dateutil import parser

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


#########################################################
def TestThree(codes, beginMoney, args):
  import strategy.dv3
  
  stock = strategy.dv3.TradeManager(codes, beginMoney=beginMoney)
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
  
  # inList, outList = dvYearAll.Filter(out)
  inList, outList = dvYear.Filter(out)
  in2, out2 = hs300All.Filter(inList)
  in3, out3 = hs300All.Filter(outList)
  # in3, out3 = ipoYear.Filter(in2)
  
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
  
  # for one in stockList.VERSION_DV2.DVOK_NOT_HS300:
  #   if one['_id'] not in in2:
  #     codes.append(one)
  #
  # for one in stockList.VERSION_DV2.HS300_NOT_DVOK:
  #   if one['_id'] not in in2:
  #     codes.append(one)
  
  print('### final backtest stock list size {}'.format(len(codes)))
  empty = []
  for one in codes:
    print(one)
    tmp = util.LoadData('stock_statistcs_dvYears', one['_id'])
    if tmp is None:
      print('no dvdata !!! {}'.format(one))
      empty.append(one)

  tools.DoAction(empty, tools.CalcDV)
      
  # TestThree(codes, 100000,
  #           {'check': False, 'backtest': True, 'saveDB': 'all_dv3', 'draw': None, 'saveFile': 'C:/workspace/tmp/dv3'})
  TestThree(codes, 100000,
            {'check': False, 'backtest': True, 'saveDB': 'all_dv3', 'draw': None, 'saveFile': r'C:\workspace\tmp/dv3'})




#########################################################
###这个文件每天执行一次，只回测runEveryWeek的输出结果集合，刷新最新的回测结果
if __name__ == '__main__':
  #对全部股票标的中，产生过交易并且属于沪深300，并且分红年份达标的标的回测
  RunHS300AndDVYears()

  