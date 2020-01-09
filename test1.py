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
import tools

# this project
if __name__ == '__main__':
  import sys
  
 
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
    stock.StoreResult2DB( args['saveDB'])
  
  if 'check' in args and args['check']:
    assert stock.CheckResult()
    
  if 'draw' in args:
    stock.Draw()
  
  if 'saveFile' in args:
    stock.Store2File(args['saveFile'])
    
  return stock



def TestAll(codes, save, check):
  # # 皖通高速
  # TestOne('600012', 52105, save, check)
  # # 万华化学
  # TestOne('600309', 146005, save, check)
  # # 北京银行
  # TestOne('601169', 88305, save, check)
  # # 大秦铁路
  # # 2015年4月27日那天，预警价为14.33元，但收盘价只有14.32元，我们按照收盘价计算，
  # # 差一分钱才触发卖出规则。如果当时卖出，可收回现金14.32*12500+330=179330元。
  # # 错过这次卖出机会后不久，牛市见顶，股价狂泻，从14元多一直跌到5.98元。
  # # TODO 需要牛市清盘卖出策略辅助
  # TestOne('601006', 84305, save, check)
  # # 南京银行
  # TestOne('601009', 75005, save, check)
  for one in codes:
    if len(one) == 3:
      TestOne(one['code'], one['money'], one['name'], save, check)
    else:
      TestOne(one['code'], 100000, one['name'], save, check)
    


  
    
def CompareOne(code, name):
  client = MongoClient()
  db = client["stock_backtest"]
  collection = db["dv1"]
  cursor = collection.find({"_id": code})
  out = None
  for c in cursor:
    out = c
    break
  
  if out is not None:
    result = {'code': code, 'name': name}
    result['profit'] = out['result']['percent']
    result['hs300Profit'] = out['result']['hs300Profit']
    result['winHS300'] = result['profit'] - result['hs300Profit']
    return result
    

def CompareAll(codes):
  out = []
  TOTAL = len(codes)
  winNumber = 0
  allHS300Profit = 0
  allProfit = 0
  holdAllTimeHS300 = 0
  HOLD_ALL_HS300_PROFIT = 0.24
  for one in codes:
    tmp = CompareOne(one['code'], one['name'])
    holdAllTimeHS300 += HOLD_ALL_HS300_PROFIT
    if tmp['winHS300'] > 0:
      winNumber += 1
      allHS300Profit += tmp['hs300Profit']
      allProfit += tmp['profit']
    out.append(tmp)

  out.sort(key=lambda x: x['profit'])
  out.reverse()
  print("{}, {}, {}, {}, {}".format(winNumber, TOTAL, allProfit, allHS300Profit, holdAllTimeHS300))
  for one in out:
    print(one)
  


def TestBank():
  # 从所有的符合条件的银行股里面，判断最近是否符合买入卖出条件
  # 1 更新行情数据
  
  # 2回测
  client = MongoClient()
  db = client["stock_backtest"]
  collection = db["all_dv1_digest"]
  cursor = collection.find({'name': {'$regex': '银行', }})
  out = []
  for c in cursor:
    out.append(c)

  strategy.dv1.SignalDV(out)


def SignalAll():
  # 从所有的符合条件的银行股里面，判断最近是否符合买入卖出条件
  # 1 更新行情数据
  
  # 2回测
  client = MongoClient()
  db = client["stock_backtest"]
  collection = db["all_dv1_digest"]
  cursor = collection.find()
  out = []
  for c in cursor:
    out.append(c)
  
  strategy.dv1.SignalDV(out)


def CalcDVAll():
  client = MongoClient()
  db = client["stock_backtest"]
  collection = db["all_dv1_digest"]
  cursor = collection.find()
  out = []
  for c in cursor:
    out.append(c)
  
  strategy.dv1.CalcDV(out)
  
  
  


def HoldAll():
  client = MongoClient()
  db = client["stock_backtest"]
  collection = db["all_dv1"]
  cursor = collection.find({'status': 2})
  out = []
  for c in cursor:
    out.append(c)
  
  strategy.dv1.HoldDV(out)
  


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

  TestThree(codes, 100000,
            {'check': False, 'backtest': True, 'saveDB': 'all_dv3', 'draw': None, 'saveFile': 'C:/workspace/tmp/dv3'})
  # TestThree(codes, 100000,
  #           {'check': False, 'backtest': True, 'saveDB': 'all_dv3', 'draw': None, 'saveFile': '/home/ken/temp/dv3'})


def TestA():
  df1 = util.LoadData('stock_signal', '2019-12-21', condition={'操作': 1}, sort=[('百分比', -1)])
  df2 = util.LoadData('stock_statistcs', 'dvYears', condition={}, sort=[('百分比', -1)])
  
  mergeData = df1.join(df2, how='left', rsuffix='right')
  codes = []
  for code, row in mergeData.iterrows():
    codes.append(code)
  df3 = util.LoadData2('stock_backtest', 'all_dv1_digest', codes)
  df4 = util.LoadData2('stock_statistcs', 'quarterSpeed', codes)
  df5 = util.LoadData2('stock', 'stock_list', codes)
  df6 = util.LastPriceNone(codes)
  df5 = df5[['所属行业', '地区', '总股本(亿)']]
  mergeData = mergeData.join(df3, how='left', rsuffix='right2')
  mergeData = mergeData.join(df4, how='left', rsuffix='right3')
  mergeData = mergeData.join(df5, how='left', rsuffix='right4')
  mergeData = mergeData.join(df6, how='left', rsuffix='right5')
  mergeData['总市值(亿)'] = mergeData['price']*mergeData['总股本(亿)']
  # mergeData.eval('总市值 = price * 总股本(亿)', inplace=True)
  mergeData.to_excel("c:/workspace/tmp/1224.xlsx")
  return mergeData

def TestB():
  df1 = util.LoadData('stock_hold', 'dv1', condition={'diff': {'$lt': 0.1}}, sort=[('diff', 1)])
  df2 = util.LoadData('stock_statistcs', 'dvYears', condition={}, sort=[('百分比', -1)])
  
  mergeData = df1.join(df2, how='left', rsuffix='right')
  codes = []
  for code, row in mergeData.iterrows():
    codes.append(code)
  df3 = util.LoadData2('stock_backtest', 'all_dv1', codes)
  df4 = util.LoadData2('stock_statistcs', 'quarterSpeed', codes)
  df5 = util.LoadData2('stock', 'stock_list', codes)
  df6 = util.LastPriceNone(codes)
  df5 = df5[['所属行业', '地区', '总股本(亿)']]
  mergeData = mergeData.join(df3, how='left', rsuffix='right2')
  mergeData = mergeData.join(df4, how='left', rsuffix='right3')
  mergeData = mergeData.join(df5, how='left', rsuffix='right4')
  mergeData = mergeData.join(df6, how='left', rsuffix='right5')
  mergeData['markValue'] = mergeData['price'] * mergeData['总股本(亿)']
  mergeData.query('first > 0 and second > 0 and third > 0', inplace=True)
  mergeData.query('markValue >= 50', inplace=True)
  mergeData = mergeData[['name', 'diff', 'percent', '统计年数', '分红年数', 'maxValue:value', '所属行业', '地区', 'markValue']]
  
  # mergeData.eval('总市值 = price * 总股本(亿)', inplace=True)
  mergeData.to_excel("c:/workspace/tmp/1222-3.xlsx")
  return mergeData

  
if __name__ == '__main__':
  import strategy.dv1
  import strategy.dv2
  import strategy.dv3
  from const import stockList
  from fund_manage import hold
  VERIFY_CODES = stockList.VERIFY_CODES

  start = '2011-01-01T00:00:00Z'
  end = '2019-12-31T00:00:00Z'

  df = pd.read_excel(r'C:\profile\2020\个人\投资\沪深300指数历史年分成分股名单.xlsx', dtype=str)
  out = df.to_dict('list')
  all = set()
  for k, v in out.items():
    for one in v:
      try:
        if len(one) == 6:
          tmp = one
        else:
          tmp = '{:06}'.format(int(one))
        all.add(tmp)
      except Exception as e:
        util.PrintException(e)
  print(all)
  
  allCodes = util.QueryAll()
  out = []
  for k, row in allCodes.iterrows():
    if k in all:
      out.append({'_id': k, 'name': row['名称']})
  util.SaveMongoDBList(out, 'stock_codeList', 'allHS300')
  # df = pd.read_excel(r'C:\workspace\tmp\base.xlsx')
  # base = set()
  # for k, v in df.iterrows():
  #   tmp = '{:06}'.format(v['code'])
  #   base.add(tmp)
  #
  # df = pd.read_excel(r'C:\workspace\tmp\in.xlsx')
  # inSet = set()
  # for k, v in df.iterrows():
  #   tmp = '{:06}'.format(v['code'])
  #   inSet.add(tmp)
  #
  # df = pd.read_excel(r'C:\workspace\tmp\out.xlsx')
  # outSet = set()
  # for k, v in df.iterrows():
  #   tmp = '{:06}'.format(v['code'])
  #   outSet.add(tmp)
  #
  # print('base in out {} {} {}'.format(len(base), len(inSet), len(outSet)))
  # newOne = base.difference(outSet)
  # final = newOne.union(inSet)
  # print('new final {} {} '.format(len(newOne), len(final)))
  # for one in final:
  #   # print("'{}',".format(one))
  #   print(one)
  # out = []
  # client = MongoClient()
  # db = client["stock_backtest"]
  # # collection = db["all_dv3"]
  # collection = db["dv2"]
  # cursor = collection.find({'tradeCounter': {'$gte': 1}})
  # # cursor = collection.find()
  # for one in cursor:
  #   # print(one)
  #   out.append({'_id': one['_id'], 'name': one['name'], 'percent': one['percent'],
  #               'holdStockNatureDate': one['holdStockNatureDate'],
  #               'tradeCounter': one['tradeCounter']})
  #
  # # tools.DoAction(out, tools.CalcDV)
  # tools.CalcDVAll()
  # tools.CalcDV([{'_id': '600015', 'name': '华能水电', },
  #               {'_id': '000895', 'name': '格力电器', },])
  # df = ts.get_deposit_rate()
  # print(df)
  # hold.CalcHoldTime(stockList.VERSION_DV1.GOOD_LIST, 'dv2', start, end)
  # hold.CalcHoldTime(stockList.VERSION_DV2.TOP30_LIST, 'dv2', 'dv2_top30', start, end)
  # hold.CalcHoldTime(stockList.VERSION_DV2.BOTTOM30_LIST, 'all_dv2', 'dv2_bottom30', start, end)
  # client = MongoClient()
  # db = client["stock_backtest"]
  # collection = db["dv2"]
  # collection.rename('all_dv2')

  # TestTwo( [{'_id': '600025', 'name': '华能水电', },], 100000, {'check': False, 'backtest': True, 'save': True})
  # codes = []
  # for one in stockList.VERSION_DV1.BAD_LIST:
  #   codes.append(one['_id'])
  # strategy.dv2.Compare('all_dv1', 'dv2', codes)

  # for index in range(0, len(stockList.VERSION_DV1.GOOD_LIST), 5):
  #   codes = stockList.VERSION_DV1.GOOD_LIST[index:5]
  #   TestTwo(codes, 100000, {'check': True, 'backtest': True, 'save': False})

  # TestTwo(stockList.VERSION_DV2.BOTTOM30_LIST, 100000, {'check': False, 'backtest': True, 'save': True})
  # codes = []
  # for one in stockList.VERSION_DV1.GOOD_LIST:
  #   codes.append(one['_id'])
  # strategy.dv2.Compare('all_dv1', 'dv2', codes)

  # TestTwo( [{'name': '000070', '_id': '000070', }], 100000, {'check': False, 'backtest': True, 'save': True})
  
  # codes = []
  # df = util.QueryAll()
  # for code, row in df.iterrows():
  #   codes.append({'_id': code, 'name': row['名称']})
  #
  # strategy.dv3.CalcDV(codes)
  # RunHS300AndDVYears()
  # #
  # # #每次100个
  # for index in range(180, len(codes), 100):
  #   tmp = codes[index:index+100]
  #   print('now index  {}  #################'.format(index))
  #   TestTwo(tmp, 100000, {'check': False, 'backtest': True, 'save': True})
  # if index < len(codes):
  #   tmp = codes[index:]
  #   TestTwo(tmp, 100000, {'check': False, 'backtest': True, 'save': True})
  
  


  # df = util.QueryAll()
  # codes = []
  # for code, row in df.iterrows():
  #   codes.append(code)
  # trend.ProfitMarginTrend.Run(codes)

  # ones = stockList.VERSION_DV1.BAD_LIST
  # codes = []
  # for one in ones:
  #   codes.append(one['_id'])
  # trend.ProfitMarginTrend.Show(codes)

  # ones = stockList.VERSION_DV1.GOOD_LIST
  # trend.ProfitMarginTrend.Show(ones)
  
  # ones = stockList.MY_HOLD
  # # times.DangerousQuarterRatio.Run(ones)
  # times.DangerousQuarterRatio.Show(ones)

  # ones = stockList.VERSION_DV1.BAD_LIST
  # times.DangerousQuarterRatio.Run(ones)
  # times.DangerousQuarterRatio.Show(ones)
  #
  # ones = stockList.VERSION_DV1.GOOD_LIST
  # times.DangerousQuarterRatio.Run(ones)
  # times.DangerousQuarterRatio.Show(ones)
  
  # for one in VERIFY_CODES:
  #   stock = TradeUnit(one['_id'], one['money'])
  #   if not stock.ExistCheckResult():
  #     print(stock.code)
  # TestBank()
  # SignalAll()
  # CalcDVAll()
  


  # client = MongoClient()
  # db = client["stock_backtest"]
  # collection = db["all_dv1"]
  # cursor = collection.find({'tradeCounter': {'$gt': 0}})
  # out = []
  # for c in cursor:
  #   out.append(c)
  #
  # strategy.dv1.CalcQuarterSpeed(out, 2019)
  
  

  

  # TestThree(
  #         # [
  #         #   {'_id': '600025', 'name': '华能水电', },
  #         #   {'_id': '601166', 'name': '兴业银行', 'money': 90205},
  #         #   {'_id': '600900', 'name': '长江电力', 'money': 63905},
  #         #  ],
  #   [
  #     # {'name': '东风股份', '_id': '601515', 'money': 133705},
  #     {'name': '泸州老窖', '_id': '000651', 'money': 52105},
  #     # {'name': '重庆水务', '_id': '601158', 'money': 58105},
  #     # {'name': '浦发银行', '_id': '600000', 'money': 74505},
  #     # {'name': '万科', '_id': '000002', 'money': 72705},
  #     # {'name': '宝钢股份', '_id': '600019', 'money': 70705},
  #     # {'name': '中国石化', '_id': '600028', 'money': 74405},
  #     # {'name': '双汇发展', '_id': '000895', 'money': 211205},
  #     #  {'name': '伟星股份', '_id': '002003', 'money': 80805},
  #   ],
  #   100000, {'check': False, 'backtest': True, 'saveDB': 'all_dv3', 'draw': None, 'saveFile': 'C:/workspace/tmp/dv3'})
  
  # test
  # TestAll(CODE_AND_MONEY, True, False)
  #save
  # TestThree(VERIFY_CODES, 100000, {'check': True, 'backtest': True, 'save': False})
  # TestThree(stockList.VERSION_DV2.TOP30_LIST, 100000, {'check': True, 'backtest': True, 'save': False})
  # TestThree(stockList.VERSION_DV2.BOTTOM30_LIST, 100000,
  #           {'check': False, 'backtest': True, 'saveDB': 'all_dv3', 'draw': None, 'saveFile': 'C:/workspace/tmp/dv3'})
  
  # tmp = stockList.VERSION_DV2.TOP30_LIST
  # tmp.extend(stockList.VERSION_DV2.BOTTOM30_LIST)
  # TestThree(tmp, 100000, {'check': False, 'backtest': True, 'save': False})
  
  
  

  # TestTwo(stockList.VERSION_DV1.BAD_LIST, 100000, {'check': False, 'backtest': True, 'save': True})
  # TestAll(VERIFY_CODES, True, False)
  #check
  # TestAll(VERIFY_CODES, False, True)
  #compare
  # CompareAll(VERIFY_CODES)

  
  # strategy.dv1.Digest('all_dv1',   {"$where": "this.percent > this.hs300Profit"})
  