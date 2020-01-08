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


class Money:
  def __init__(self, startMoney, code):
    self.__money = startMoney
    self.__code = code
  
  
  @property
  def value(self):
    return self.__money
  
  
  def __add__(self, other):
    if isinstance(other, (int, float)):
      return self.__money+other
    elif isinstance(other, Money):
      return self.__money+other.__money
    else:
      return NotImplemented
    
  def __radd__(self, other):
    return self.__add__(other)
    
    
  # def __iadd__(self, other):
  #   if isinstance(other, (int, float)):
  #     self.__money += other
  #     return self
  #   elif isinstance(other, Money):
  #     self.__money += other.__money
  #     return self
  #   else:
  #     return NotImplemented

  def __sub__(self, other):
    if isinstance(other, (int, float)):
      return self.__money - other
    elif isinstance(other, Money):
      return self.__money - other.__money
    else:
      return NotImplemented

  def __rsub__(self, other):
    return self.__sub__(other)
  
  # def __isub__(self, other):
  #   if isinstance(other, (int, float)):
  #     self.__money -= other
  #     return self
  #   elif isinstance(other, Money):
  #     self.__money -= other.__money
  #     return self
  #   else:
  #     return NotImplemented
    
  def __truediv__(self, other):
    if isinstance(other, (int, float)):
      return self.__money / other
    elif isinstance(other, Money):
      return self.__money / other.__money
    else:
      return NotImplemented
    
  def __gt__(self, other):
    if isinstance(other, (int, float)):
      return self.__money > other
    elif isinstance(other, Money):
      return self.__money > other.__money
    else:
      return NotImplemented
  
  # def copy(self):
  #   return Money(self.value, self.code)
  
  # def reset(self, n):
  #   if isinstance(n, (int, float)):
  #     self.__money = n
  #   elif isinstance(n, Money):
  #     self.__money = n.__money
  #   else:
  #     raise NotImplemented
    
  
  def __str__(self):
    return self.__money.__str__()



class FundManager:
  def __init__(self, stockSize):
    self.TOTALMONEY = 500000
    self.stockSize = stockSize
    
    self.totalMoney = 0
    self.MaxMoney = 0

  
  def AfterSellStage(self, stage):
    #在这里做资金管理后再真正转发
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