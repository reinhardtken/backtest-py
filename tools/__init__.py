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
import strategy.dv3

#########################################################
#计算上市以来的总分红年数，并计算第N年的累计分红总年数和对应的累计分红率
def CalcDV(codes):
  stock = strategy.dv3.TradeManager(codes, startDate='2001-01-01T00:00:00Z',
                                    endDate='2019-12-31T00:00:00Z')
  stock.LoadQuotations()
  stock.LoadIndexs()
  stock.Merge()
  stock.CheckPrepare()
  for k, v in stock.dvMap.items():
    data = v.dv2Index.statisticsYearsDF.to_dict('index')
    util.SaveMongoDBDict(data, 'stock_statistcs_dvYears', k)


def CalcDVAll():
  codes = util.QueryAllCode()
  DoAction(codes, CalcDV)
#########################################################

def DoAction(codes, action, size=100):
  # #每次100个
  index = 0
  for index in range(0, len(codes), size):
    tmp = codes[index:index+size]
    print('now index  {}  #################'.format(index))
    action(tmp)
  if index < len(codes):
    tmp = codes[index:]
    action(tmp)
########################################################
def AllHS300Code2DB(path):
  df = pd.read_excel(path, dtype=str)
  out = df.to_dict('list')
  all = set()
  out2 = {}
  code2Map = {}
  for k, v in out.items():
    if isinstance(k, int):
      id = util.String2pdTimestamp(str(k), '%Y%m')
    else:
      id = util.String2pdTimestamp(k, '%Y%m')
    tmpList = []
    for one in v:
      try:
        if len(one) == 6:
          tmp = one
        else:
          tmp = '{:06}'.format(int(one))
        all.add(tmp)
        tmpList.append(tmp)
      except Exception as e:
        util.PrintException(e)
    out2[id] = {}
    out2[id]['data'] = tmpList
  print(all)
  
  allCodes = util.QueryAll()
  out = []
  for k, row in allCodes.iterrows():
    if k in all:
      out.append({'_id': k, 'name': row['名称']})
    code2Map[k] = {'_id': k, 'name': row['名称']}
  util.SaveMongoDBList(out, 'stock_codeList', 'allHS300')
  
  notInAll = set()
  for k, v in out2.items():
    for index in range(0, len(v['data'])):
      try:
        v['data'][index] = code2Map[v['data'][index]]
      except Exception as e:
        util.PrintException(e)
        notInAll.add(v['data'][index])
  
  print(notInAll)
  util.SaveMongoDBDict(out2, 'stock_codeList', 'allHS300Detail')