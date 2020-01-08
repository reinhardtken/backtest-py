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