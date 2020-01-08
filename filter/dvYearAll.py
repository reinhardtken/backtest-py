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


# 静态过滤自上市以来分红不超过5年的
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
    db = client["stock_statistcs_dvYears"]
    for one in codes:
      collection = db[one]
      cursor = collection.find().sort([('_id', -1)]).limit(1)
      for c in cursor:
        if c['acc'] >= 5:
          inSet.add(one)
        else:
          outSet.add(one)
        break
  
  return list(inSet), list(outSet)