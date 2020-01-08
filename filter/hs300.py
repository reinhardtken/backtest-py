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


# 过滤不属于沪深300的标的
def Filter(stocks):
  inSet = set()
  outSet = set()
  if len(stocks) > 0:
    tmp = stocks[0]
    if isinstance(tmp, str):
      codes = stocks
    elif isinstance(tmp, dict):
      codes = list(map(lambda x: x['_id'], stocks))

    hs300 = util.QueryHS300All()
    for one in codes:
      if one in hs300.index:
        inSet.add(one)
      else:
        outSet.add(one)
  
  return list(inSet), list(outSet)