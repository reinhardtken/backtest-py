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


from fund_manage import fm
from fund_manage import fm2


Message = const.Message


class Money(fm2.Money):
  def __init__(self, fm, startMoney, code):
    # 像资金管理登记，告知本code希望的起始资金是多少
    self.__money = fm.Register(code, startMoney)
    self.__fm = fm
    self.__code = code
    self.moveList = []
  
  # 出金
  def withdraw(self, first):
    v1 = self.__fm.Alloc(self.__code, first)
    v2 = self.__money
    self.__money -= v2
    if v1 + v2 > 0:
      self.moveList.append(-(v1 + v2))
    return v1 + v2
  
  # 入金
  def deposit(self, v, paybackAll):
    self.__fm.Free(self.__code, v, paybackAll)
    self.moveList.append(v)
  
  @property
  def value(self):
    return self.__fm.Query(self.__code)

#########################################################
class FundManager:
  def __init__(self, stocks, tm, startDate, endDate):
    self.TOTALMONEY = 500000
    self.stocks = stocks
    self.stockSize = len(stocks)
    self.startDate = startDate
    self.endDate = endDate
    self.TM = tm
    
    self.totalMoney = self.TOTALMONEY
    self.MaxMoney = 0
    self.stockMap = {
    }  # 记录每个code借出的资金
    self.stockSet = set()
    self.eventCache = {}
    self.moveList = []
    self.lastPayback = {} #个股计算盈亏的时候，需要最后一次归还的资金

    
    dfIndex = pd.date_range(start=startDate, end=endDate, freq='M')
    self.df = pd.DataFrame(np.random.randn(len(dfIndex)), index=dfIndex, columns=['willDrop'])
    self.df = pd.concat([self.df, pd.DataFrame(columns=[
      'total', 'profit', 'percent', 'cash', 'marketValue', 'stockNumber'
    ])], sort=False)
    self.df.drop(['willDrop', ], axis=1, inplace=True)
    
    self.quarterDetail = {}

  
  def Process(self, context, task):
    if task.key == Message.SUGGEST_BUY_EVENT:
      #缓存
      self.eventCache[context] = Task(
          Priority(
            Message.STAGE_BUY_TRADE, Message.PRIORITY_BUY),
          Message.BUY_EVENT, None, *task.args)
      # context.AddTask(
      #   Task(
      #     Priority(
      #       Message.STAGE_BUY_TRADE, Message.PRIORITY_BUY),
      #     Message.BUY_EVENT, None, *task.args))
    elif task.key == Message.OTHER_WORK:
      if context.date in self.df.index:
        #计算月度终值
        digest, detail = self.TM.CalcNowValue()
        self.df.loc[context.date, 'cash'] = self.totalMoney
        self.df.loc[context.date, 'marketValue'] = digest['marketValue']
        self.df.loc[context.date, 'stockNumber'] = digest['stockNumber']
        self.df.loc[context.date, 'total'] = self.totalMoney + digest['marketValue']
        self.df.loc[context.date, 'profit'] = self.df.loc[context.date, 'total'] - self.TOTALMONEY
        self.df.loc[context.date, 'percent'] = self.df.loc[context.date, 'profit'] / self.TOTALMONEY
        self.quarterDetail[context.date] = detail
        pass
  
  
  def Alloc(self, code, first):
    # first 表示是否是建仓，建仓考虑大宗资金分配，否则只考虑动用分红资金
    n = 0
    if code in self.stockMap:
      n = self.stockMap[code]['total']
      self.stockMap[code]['old'] = n
      self.stockMap[code]['total'] = 0
    
    if n > 0:
      info = '### FundManager alloc {} {:.2f}'.format(code, n)
      self.moveList.append(info)
      print(info)
    return n
  
  def Query(self, code):
    n = 0
    if code in self.stockMap:
      n = self.stockMap[code]['change']
    elif code in self.lastPayback:
      n = self.lastPayback[code]
    return n
  
  def Free(self, code, money, paybackAll):
    self.totalMoney += money
    self.stockMap[code]['change'] += money
    info = '### FundManager free {} {:.2f}, {:.2f}'.format(code, money, self.totalMoney)
    self.moveList.append(info)
    print(info)
    if paybackAll:
      self.lastPayback[code] = self.stockMap[code]['change']
      winLoss = self.stockMap[code]['change'] - self.stockMap[code]['old']
      info = '### FundManager free winlose {} {:.2f}, {:.2f}, {:.2f}'.format(code, winLoss, self.stockMap[code]['change'], self.stockMap[code]['old'])
      self.moveList.append(info)
      print(info)
      self.stockMap.pop(code)
    
  
  def Register(self, code, money):
    # self.stockMap[code] = {}
    # self.stockMap[code]['start'] = money
    # self.stockMap[code]['now'] = money
    return 0
  
  
  def StageChange(self, before):
    if not before:
      if len(self.stockMap) >= 10:
        #最多持有十只股票
        self.eventCache = {}
        return
      left = 10 - len(self.stockMap)
      #看看可以买入的数目和发出买入信号的数目中哪个小
      small = left if left < len(self.eventCache) else len(self.eventCache)
      share = 0
      #计算每份可以给予支持的资金数目
      if small > 0:
        share = self.totalMoney / left
        
      counter = 0
      for k, v in self.eventCache.items():
        #如果买入信号的股票没有持仓
        if k.code not in self.stockMap:
          print('### FundManager alloc all {} {}'.format(k.code, share))
          self.stockMap[k.code] = {}
          self.stockMap[k.code]['total'] = share
          self.stockMap[k.code]['change'] = 0
          self.stockMap[k.code]['date'] = k.date
          self.stockSet.add(k.code)
          self.totalMoney -= share
          counter += 1
          k.AddTask(v)
          if counter == small:
            break
      self.eventCache = {}
