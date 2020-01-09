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


#动态的方式检查分红年限是否符合要求
class Filter:
  def __init__(self, code):
    self.code = code
    self.accDividend = util.LoadData('stock_statistcs_dvYears', self.code)
    
  
  def Process(self, context, task):
    # 计算五年滚动分红率是否达标
    accDividend = False
    if context.date.year in self.accDividend.index:
      roll5 = self.accDividend.loc[context.date.year, 'roll5']
      if context.date.year <= 2018 and roll5 >= 4:
        accDividend = True
      elif context.date.year == 2019 and roll5 >= 3:
        accDividend = True
      elif context.date.year == 2020 and roll5 >= 2:
        accDividend = True
    return not accDividend