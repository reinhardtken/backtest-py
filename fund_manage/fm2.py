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


SUGGEST_BUY_EVENT = 11

from fund_manage import fm
class Money(fm.Money):
  def __init__(self, fm, startMoney, code):
    self.__money = fm.AllocOnce(code, startMoney)
    self.__fm = fm
    self.__code = code
    self.moveList = []
    
  #出金
  def withdraw(self, first):
    v = self.__money
    self.__money -= v
    if v > 0:
      self.moveList.append(-v)
    return v
  
  #入金
  def deposit(self, v):
    self.__money += v
    if v > 0:
      self.moveList.append(v)
    
  # def reset(self, other):
  #   super().reset(other)
  #   self.payback(self.__money)


class FundManager:
  def __init__(self, stockSize):
    self.TOTALMONEY = 500000
    self.stockSize = stockSize
    
    self.totalMoney = self.TOTALMONEY
    self.MaxMoney = 0
    self.stockMap = {}  # 记录每个code借入和归还的资金
  
  def AfterSellStage(self, stage):
    # 在这里做资金管理后再真正转发
    pass
  
  def Process(self, context, task):
    if task.key == SUGGEST_BUY_EVENT:
      context.AddTask(
        Task(
          Priority(
            4, 2500),
          5, None, *task.args))
  
  def Alloc(self, code, money):
    self.totalMoney -= money
    if self.MaxMoney < abs(self.totalMoney) and self.totalMoney < 0:
      self.MaxMoney = abs(self.totalMoney)
      print('###FundManager: new Max Money {} ###'.format(self.MaxMoney))
    
    print('###FundManager: alloc {} {} {}###'.format(code, money, self.totalMoney))
    pass
  
  def Free(self, code, money):
    self.totalMoney += money
    print('###FundManager: free {} {} {}###'.format(code, money, self.totalMoney))
    pass
  
  
  def AllocOnce(self, code, money):
    return money