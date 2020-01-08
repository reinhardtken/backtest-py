# -*- coding: utf-8 -*-

from datetime import datetime
from datetime import timedelta
from dateutil import parser
from pytz import timezone
import traceback
from queue import PriorityQueue

# thirdpart
import pandas as pd
from pymongo import MongoClient
import numpy as np

import const
import util



#以月为单位，计算区间内，每个月持有多少股票
def CalcHoldTime(stockList, collectionName, toDBName, beginDate, endDate):
  utcTZ = timezone('UTC')
  dfIndex = pd.date_range(start=beginDate, end=endDate, freq='M')
  df = pd.DataFrame(np.random.randn(len(dfIndex)), index=dfIndex, columns=['willDrop'])
  df = pd.concat([df, pd.DataFrame(columns=[
    'number', 'stockList',
  ])], sort=False)
  df.drop(['willDrop', ], axis=1, inplace=True)

  result = {}
  for date, row in df.iterrows():
    result[date] = row.to_dict()
    result[date]['number'] = 0
    result[date]['stockList'] = set()
    
  codes = []
  for one in stockList:
    codes.append(one['_id'])
  condition = {'_id': {'$in': codes}}
  df2 = util.LoadData('stock_backtest', collectionName, condition)
  for code, row in df2.iterrows():
    name = row['name']
    oneList = row['holdStockDateVec']
    for datePair in oneList:
      startDate = datetime(datePair[0].year, datePair[0].month, datePair[0].day, tzinfo=utcTZ)
      endDate = datetime(datePair[1].year, datePair[1].month, datePair[1].day, tzinfo=utcTZ)
      print(startDate)
      tmp = pd.date_range(start=startDate, end=endDate, freq='M')
      print(tmp)
      for oneDate in tmp:
        if oneDate in result:
          result[oneDate]['number'] += 1
          result[oneDate]['stockList'].add(name)
  
  for k, v in result.items():
    result[k]['stockList'] = list(result[k]['stockList'])
  util.SaveMongoDBDict(result, 'stock_hold', toDBName)