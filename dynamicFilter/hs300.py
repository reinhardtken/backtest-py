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


Message = const.Message

# 动态的方式检查是否在沪深300中
class Filter:
  def __init__(self):
    client = MongoClient()
    db = client['stock_codeList']
    collection = db['allHS300Detail']
    cursor = collection.find().sort([('_id', 1)])
    self.data = []
    self.current = None
    for c in cursor:
      id = c['_id']
      tmpSet = set()
      for one in c['data']:
        if isinstance(one, str):
          tmpSet.add(one)
        else:
          tmpSet.add(one['_id'])
      self.data.append((id.year*100+id.month, tmpSet))
    
    self.year = None
    self.month = None
      
    self.lastAccDivedendNegative = {}  # 记录因为分红不合格丢弃的开仓动作

  def Process(self, context, task):
    if task.key == Message.SUGGEST_BUY_EVENT:
      # 计算是否在沪深300里面
      jump = True
      if context.code in self.current:
        jump = False
      else:
        jump = True
        
      if jump:
        if context.code not in self.lastAccDivedendNegative or \
          context.date - self.lastAccDivedendNegative[context.code] < timedelta(days=30):
            pass
        else:
          self.lastAccDivedendNegative[context.code] = context.date
          print('### not in hs300 {} {} '.format(context.code, context.date))
          
      return jump
    elif task.key == Message.NEW_MONTH:
      if self.year != context.year or self.month != context.month:
        #寻找最合适的沪深300成分股集合
        self.year = context.year
        self.month = context.month
        tag = self.year*100+self.month
        last = None
        for one in self.data:
          if tag >= one[0]:
            last = one
            continue
          if last is not None:
            self.current = last[1]
            print('### change hs300 set {}-{} {} {}'.format(self.year, self.month, last[0], last[1]))
            break
        else:
          if last is not None:
            self.current = last[1]
            print('### change hs300 set {}-{} {} {}'.format(self.year, self.month, last[0], last[1]))
