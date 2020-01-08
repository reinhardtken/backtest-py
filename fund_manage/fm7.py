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
from comm import Move

Message = const.Message


#和没有资金管理的效果一样。。。
class FundManager:
  def __init__(self, stocks, tm, startDate, endDate):
    self.TOTALMONEY = 500000
    self.perShare = 50000
    # self.nowMax = self.TOTALMONEY / self.perShare
    self.stocks = stocks
    self.code2Name = {}
    for one in stocks:
      self.code2Name[one['_id']] = one['name']
    self.stockSize = len(stocks)
    self.startDate = startDate
    self.endDate = endDate
    self.TM = tm
    
    self.totalMoney = self.TOTALMONEY
    self.MaxMoney = 0
    self.stockMap = {}
    for one in stocks:
      self.stockMap[one['_id']] = {}
      self.stockMap[one['_id']]['allWinLoss'] = 0
    self.stockNowMap = {
    }  # 记录每个code借出的资金

  
  def Process(self, context, task):
    if task.key == Message.SUGGEST_BUY_EVENT:
      context.AddTask(
        Task(
          Priority(
            Message.STAGE_BUY_TRADE, Message.PRIORITY_BUY),
          Message.BUY_EVENT, None, *task.args))
    
  
  def Alloc(self, code, first):
    n = self.stockNowMap[code]['now']
    self.stockNowMap[code]['now'] = 0
    return n
  
  def Query(self, code):
    n = self.stockNowMap[code]['now']
    return n
  
  def Free(self, code, money, paybackAll):
    self.stockNowMap[code]['now'] += money
  
  def Register(self, code, money):
    self.stockNowMap[code] = {}
    self.stockNowMap[code]['start'] = money
    self.stockNowMap[code]['now'] = money
    return 0
  
  def StageChange(self, before):
    pass
