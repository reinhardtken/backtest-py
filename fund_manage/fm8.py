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
import matplotlib.pyplot as plt

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
from comm import MaxAndRetracement

Message = const.Message


#########################################################
# 每份5万元，不限制份数。如果单前份数用光，则申请新份数
# 1 对于总计赚钱的股票，总是追加这个股曾经归还的最大额资金（马太效应）
# 2 对于总计亏钱的股票，总是追加这个股曾经归还的最大额资金（避免持续亏损）
# 3 对于1和2，不得超过总本金的20%

# 在对比了fm6和fm8的move数据后，fm8提升了收益，也增加了回撤
# fm8 ver1：
### win movelist ###
### win movelist ###
### FundManager:Move 600741 华域汽车, 2016-02-25 00:00:00 1160天, 346737.60, 342802.97, 689540.57
### FundManager:Move 000651 格力电器, 2016-05-03 00:00:00 729天, 332925.00, 222877.80, 555802.80
### FundManager:Move 000002 万科, 2018-07-18 00:00:00 539天, 199418.42, 395523.19, 594941.60
### FundManager:Move 002242 九阳股份, 2018-04-23 00:00:00 619天, 194776.00, 325354.54, 520130.54
### FundManager:Move 601166 兴业银行, 2015-08-24 00:00:00 1598天, 181144.40, 247798.69, 428943.09
### loss movelist ###
### FundManager:Move 000651 格力电器, 2015-04-29 00:00:00 366天, -89478.00, 312355.80, 222877.80
### FundManager:Move 600027 华电国际, 2015-08-25 00:00:00 252天, -48900.00, 311519.72, 262619.72
### FundManager:Move 600011 华能国际, 2015-08-24 00:00:00 253天, -48087.00, 360000.00, 311913.00
### FundManager:Move 002003 伟星股份, 2019-04-30 00:00:00 253天, -47452.50, 385434.13, 337981.63
### FundManager:Move 600816 安信信托, 2018-01-02 00:00:00 241天, -44366.40, 96329.98, 51963.58
# fm6：
### win movelist ###
### FundManager:Move 000651 格力电器, 2016-05-03 00:00:00 729天, 133170.00, 89169.00, 222339.00
### FundManager:Move 600741 华域汽车, 2016-05-03 00:00:00 1092天, 97275.20, 132766.00, 230041.20
### FundManager:Move 600660 福耀玻璃, 2013-05-02 00:00:00 1498天, 94829.00, 38052.00, 132881.00
### FundManager:Move 601818 光大银行, 2014-05-05 00:00:00 399天, 90570.64, 51615.14, 142185.78
### FundManager:Move 600036 招商银行, 2013-05-02 00:00:00 1533天, 90145.50, 69747.00, 159892.50
### loss movelist ###
### FundManager:Move 000651 格力电器, 2015-05-04 00:00:00 361天, -32256.00, 121425.00, 89169.00
### FundManager:Move 600027 华电国际, 2015-08-25 00:00:00 252天, -29200.00, 185818.75, 156618.75
### FundManager:Move 000981 ST银亿, 2018-06-19 00:00:00 314天, -29043.00, 50000.00, 20957.00
### FundManager:Move 002269 美邦服饰, 2012-07-09 00:00:00 297天, -24564.00, 50000.00, 25436.00
### FundManager:Move 300134 大富科技, 2011-09-05 00:00:00 121天, -21060.00, 50000.00, 28940.00
class FundManager:
  def __init__(self, stocks, tm, startDate, endDate):
    self.TOTALMONEY = 500000
    self.NAME = 'fm8'
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
    self.stockSet = set()
    self.stockNowSet = set()  # 当前持有的股票
    self.eventCache = {}
    self.moveList = []
    self.lastPayback = {}  # 个股计算盈亏的时候，需要最后一次归还的资金
    self.lastDate = None
    # 最大值与回撤
    self.maxAndRetracementM = MaxAndRetracement(self.TOTALMONEY, self.startDate)
    self.maxAndRetracementW = MaxAndRetracement(self.TOTALMONEY, self.startDate)
    
    dfIndex = pd.date_range(start=startDate, end=endDate, freq='W-FRI')
    self.dfW = pd.DataFrame(np.random.randn(len(dfIndex)), index=dfIndex, columns=['willDrop'])
    self.dfW = pd.concat([self.dfW, pd.DataFrame(columns=[
      'total', 'capital', 'profit', 'percent',
      'utilization',
      'cash', 'marketValue', 'stockNumber',
      'maxValue', 'retracementP', 'retracementD'
    ])], sort=False)
    self.dfW.drop(['willDrop', ], axis=1, inplace=True)
    
    dfIndex = pd.date_range(start=startDate, end=endDate, freq='BM')
    self.dfM = pd.DataFrame(np.random.randn(len(dfIndex)), index=dfIndex, columns=['willDrop'])
    self.dfM = pd.concat([self.dfM, pd.DataFrame(columns=[
      'total', 'capital', 'profit', 'percent',
      'utilization', 'utilizationP',
      'cash', 'marketValue', 'stockNumber',
      'maxValue', 'retracementP', 'retracementD'
    ])], sort=False)
    self.dfM.drop(['willDrop', ], axis=1, inplace=True)
    
    self.holdDetail = []
    # self.lastAccDivedendNegative = {} #记录因为分红不合格丢弃的开仓动作
  
  def gather(self, date, df, maxAndRetracement, month):
    digest, detail = self.TM.CalcNowValue()
    df.loc[date, 'cash'] = self.totalMoney
    df.loc[date, 'capital'] = self.TOTALMONEY
    df.loc[date, 'marketValue'] = digest['marketValue']
    df.loc[date, 'stockNumber'] = digest['stockNumber']
    df.loc[date, 'total'] = self.totalMoney + digest['marketValue']
    df.loc[date, 'profit'] = df.loc[date, 'total'] - self.TOTALMONEY
    df.loc[date, 'percent'] = df.loc[date, 'profit'] / self.TOTALMONEY
    df.loc[date, 'utilization'] = (df.loc[date, 'capital'] - df.loc[date, 'cash'])
    df.loc[date, 'utilizationP'] = df.loc[date, 'utilization'] / df.loc[date, 'capital']
    # 新高与回撤
    maxAndRetracement.Calc(df.loc[date, 'total'], date)
    df.loc[date, 'maxValue'] = maxAndRetracement.M.value
    df.loc[date, 'retracementP'] = maxAndRetracement.R.history.value
    df.loc[date, 'retracementD'] = maxAndRetracement.R.history.days
    # detail['date'] = date
    self.holdDetail.append((date,detail))
  
  def Process(self, context, task):
    if task.key == Message.SUGGEST_BUY_EVENT:
      # 缓存
      self.eventCache[context] = Task(
        Priority(
          Message.STAGE_BUY_TRADE, Message.PRIORITY_BUY),
        Message.BUY_EVENT, None, const.TASK_BROADCAST, *task.args)

    elif task.key == Message.OTHER_WORK:
      if context.date in self.dfW.index:
        # 计算每周终值
        self.gather(context.date, self.dfW, self.maxAndRetracementW, False)
      if context.date in self.dfM.index:
        # 计算月度终值
        self.gather(context.date, self.dfM, self.maxAndRetracementM, True)
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
      
      self.stockMap[code]['lastWinLoss'] = winLoss
      # 记录在这个股票上的得失
      self.stockMap[code]['allWinLoss'] += winLoss
      #最后一次盈利率，下次投资的时候奖励盈利率高的
      self.stockMap[code]['lastWinLossP'] = 0
      if winLoss > 0:
        self.stockMap[code]['lastWinLossP'] = winLoss/self.stockNowMap[code]['old']
      self.stockMap[code]['old'] = self.stockNowMap[code]['change']
      self.moveList.append(move)
      print(info)
      self.stockNowMap.pop(code)
      self.stockNowSet.discard(code)
  
  def Register(self, code, money):
    # self.stockNowMap[code] = {}
    # self.stockNowMap[code]['start'] = money
    # self.stockNowMap[code]['now'] = money
    return 0
  
  
  def CalcMoney(self, code):
    if self.stockMap[code]['allWinLoss'] > 0:
      tmp = self.stockMap[code]['old']
      if self.stockMap[code]['lastWinLossP'] > 0:
        #额外奖励，马太效应
        old = tmp
        tmp *=(1+self.stockMap[code]['lastWinLossP']*0.5)
        print('### award winner {} {} {}'.format(code, old, tmp))
      # 单支股票不能超过总额度20%
      if tmp > self.TOTALMONEY * 0.2:
        tmp = self.TOTALMONEY * 0.2
      return tmp
    elif self.stockMap[code]['allWinLoss'] == 0:
      return self.perShare
    else:
      return self.stockMap[code]['old']
  
  
  def StageChange(self, before):
    if not before:
      # 统计买入信号和持股的差，计算总的资金需求量
      request = set()
      for k, v in self.eventCache.items():
        request.add(k.code)
      
      diff = request - self.stockNowSet
      
      if len(diff) > 0:
        # 计算需要的资金总量
        requestMoneyTotal = 0
        requestMoneyMap = {}
        for code in diff:
          money = self.CalcMoney(code)
          requestMoneyTotal += money
          requestMoneyMap[code] = money
          # if self.stockMap[code]['allWinLoss'] > 0:
          #   tmp = self.stockMap[code]['old']
          #   # 单支股票不能超过总额度20%
          #   if tmp > self.TOTALMONEY * 0.2:
          #     tmp = self.TOTALMONEY * 0.2
          #   requestMoneyTotal += tmp
          #   requestMoneyMap[code] = tmp
          # elif self.stockMap[code]['allWinLoss'] == 0:
          #   requestMoneyTotal += self.perShare
          #   requestMoneyMap[code] = self.perShare
          # else:
          #   requestMoneyTotal += self.stockMap[code]['old']
          #   requestMoneyMap[code] = self.stockMap[code]['old']
        
        if self.totalMoney >= requestMoneyTotal:
          pass
        else:
          n = (requestMoneyTotal - self.totalMoney) // self.perShare + 1
          print('### money is not enough {} {} {}'.format(requestMoneyTotal, self.totalMoney, n))
          self.TOTALMONEY += self.perShare * n
          self.totalMoney += self.perShare * n
        
        counter = 0
        for k, v in self.eventCache.items():
          # 如果买入信号的股票没有持仓
          if k.code in diff:
            print('### FundManager alloc all {} {}'.format(k.code, requestMoneyMap[k.code]))
            self.stockNowMap[k.code] = {}
            self.stockNowMap[k.code]['total'] = requestMoneyMap[k.code]
            self.stockNowMap[k.code]['change'] = 0
            self.stockNowMap[k.code]['date'] = k.date
            self.stockSet.add(k.code)
            self.stockNowSet.add(k.code)
            self.totalMoney -= requestMoneyMap[k.code]
            counter += 1
            k.AddTask(v)
            if counter == len(diff):
              break
      self.eventCache = {}
  
  def Draw(self, collectionName):
    self.dfW['_id'] = self.dfW.index
    util.SaveMongoDB_DF(self.dfW, 'stock_result', collectionName)
    self.dfW['profit'].fillna(method='ffill', inplace=True)
    self.dfW['total'].fillna(method='ffill', inplace=True)
    self.dfW[['total', 'capital', ]].plot()
    plt.show()
  
  def Store2File(self, fileName):
    self.dfM.to_excel(fileName + "_M.xlsx")
    self.dfW.to_excel(fileName + "_W.xlsx")
    out = []
    for one in self.moveList:
      out.append(one.ToDict())
    df = pd.DataFrame(out)
    df.to_excel(fileName + "_move.xlsx")
    detail = pd.DataFrame(self.holdDetail[-1][1])
    detail.to_excel(fileName + "detail.xlsx")