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

from comm import TradeResult
from comm import TradeMark
from comm import Pump
from comm import Retracement
from comm import MaxRecord
from comm import Priority
from comm import Task



#过滤分红年数不符合要求的标的
def Filter(stocks):
  inSet = set()
  outSet = set()
  if len(stocks) > 0:
    tmp = stocks[0]
    if isinstance(tmp, str):
      codes = stocks
    elif isinstance(tmp, dict):
      codes = list(map(lambda x: x['_id'], stocks))
      
    client = MongoClient()
    db = client["stock_statistcs"]
    collection = db["dvYears"]
    cursor = collection.find({'_id': {'$in': codes}})
    for one in cursor:
      if one['统计年数'] >= 5 and one['百分比'] >= 0.8:
        inSet.add(one['_id'])
      else:
        outSet.add(one['_id'])
        
  return list(inSet), list(outSet)