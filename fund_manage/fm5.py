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



#########################################################
#每份5万元，不限制份数。如果单前份数用光，则申请新份数
class FundManager:
  def __init__(self, stocks, tm, startDate, endDate):
    self.TOTALMONEY = 500000
    self.perShare = 50000
    self.nowMax = self.TOTALMONEY / self.perShare
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
    self.stockMap = {
    }  # 记录每个code借出的资金
    self.stockNowMap = {
    }
    self.stockSet = set()
    self.stockNowSet = set() #当前持有的股票
    self.eventCache = {}
    self.moveList = []
    self.lastPayback = {}  # 个股计算盈亏的时候，需要最后一次归还的资金
    self.lastDate = None
    
    dfIndex = pd.date_range(start=startDate, end=endDate, freq='M')
    self.df = pd.DataFrame(np.random.randn(len(dfIndex)), index=dfIndex, columns=['willDrop'])
    self.df = pd.concat([self.df, pd.DataFrame(columns=[
      'total', 'capital', 'profit', 'percent',
      # 'utilization',
      'cash', 'marketValue', 'stockNumber'
    ])], sort=False)
    self.df.drop(['willDrop', ], axis=1, inplace=True)
    
    self.quarterDetail = {}
  
  def Process(self, context, task):
    if task.key == Message.SUGGEST_BUY_EVENT:
      # 缓存
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
        # 计算月度终值
        digest, detail = self.TM.CalcNowValue()
        self.df.loc[context.date, 'cash'] = self.totalMoney
        self.df.loc[context.date, 'capital'] = self.TOTALMONEY
        self.df.loc[context.date, 'marketValue'] = digest['marketValue']
        self.df.loc[context.date, 'stockNumber'] = digest['stockNumber']
        self.df.loc[context.date, 'total'] = self.totalMoney + digest['marketValue']
        self.df.loc[context.date, 'profit'] = self.df.loc[context.date, 'total'] - self.TOTALMONEY
        self.df.loc[context.date, 'percent'] = self.df.loc[context.date, 'profit'] / self.TOTALMONEY
        # self.df.loc[context.date, 'utilization'] = len(self.stockNowSet) / self.nowMax
        self.quarterDetail[context.date] = detail
        pass
    elif task.key == Message.NEW_DAY:
      self.lastDate = context.date
  
  def Alloc(self, code, first):
    # first 表示是否是建仓，建仓考虑大宗资金分配，否则只考虑动用分红资金
    n = 0
    if code in self.stockNowMap:
      n = self.stockNowMap[code]['total']
      self.stockNowMap[code]['old'] = n
      self.stockNowMap[code]['total'] = 0
    
    if n > 0:
      info = '### FundManager alloc {} {:.2f}'.format(code, n)
      # self.moveList.append(info)
      print(info)
    return n
  
  def Query(self, code):
    n = 0
    if code in self.stockNowMap:
      n = self.stockNowMap[code]['change']
    elif code in self.lastPayback:
      n = self.lastPayback[code]
    return n
  
  def Free(self, code, money, paybackAll):
    self.totalMoney += money
    self.stockNowMap[code]['change'] += money
    info = '### FundManager free {} {:.2f}, {:.2f}'.format(code, money, self.totalMoney)
    # self.moveList.append(info)
    print(info)
    if paybackAll:
      self.lastPayback[code] = self.stockNowMap[code]['change']
      winLoss = self.stockNowMap[code]['change'] - self.stockNowMap[code]['old']
      diff = self.lastDate - self.stockNowMap[code]['date']
      move = Move(code, self.code2Name[code], self.stockNowMap[code]['date'], diff.days,
                  self.stockNowMap[code]['change'],
           self.stockNowMap[code]['old'], winLoss)
      print(move)
      self.moveList.append(move)

      self.stockNowMap.pop(code)
      self.stockNowSet.discard(code)
  
  def Register(self, code, money):
    # self.stockNowMap[code] = {}
    # self.stockNowMap[code]['start'] = money
    # self.stockNowMap[code]['now'] = money
    return 0
  
  def StageChange(self, before):
    if not before:
      #统计买入信号和持股的差，计算总的资金需求量
      request = set()
      for k, v in self.eventCache.items():
        request.add(k.code)
      
      diff = request-self.stockNowSet
      if len(diff) > 0:
        if self.nowMax - len(self.stockNowMap) >= len(diff):
          pass
        else:
          #扩充本金
          realDiff = len(diff) - (self.nowMax - len(self.stockNowMap))
          print('### money is not enough {} {} {}'.format(len(self.stockNowMap), self.nowMax, realDiff))
          self.TOTALMONEY += self.perShare*realDiff
          self.totalMoney +=  self.perShare*realDiff
          self.nowMax += realDiff
        
  
        # 计算每份可以给予支持的资金数目
        share = self.totalMoney / len(diff)
        
        counter = 0
        for k, v in self.eventCache.items():
          # 如果买入信号的股票没有持仓
          if k.code in diff:
            print('### FundManager alloc all {} {}'.format(k.code, share))
            self.stockNowMap[k.code] = {}
            self.stockNowMap[k.code]['total'] = share
            self.stockNowMap[k.code]['change'] = 0
            self.stockNowMap[k.code]['date'] = k.date
            self.stockSet.add(k.code)
            self.stockNowSet.add(k.code)
            self.totalMoney -= share
            counter += 1
            k.AddTask(v)
            if counter == len(diff):
              break
      self.eventCache = {}
