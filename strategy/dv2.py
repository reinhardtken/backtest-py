# -*- coding: utf-8 -*-

# sys
from datetime import datetime
from datetime import timedelta
from dateutil import parser
import traceback
from queue import PriorityQueue

# thirdpart
import pandas as pd
from pymongo import MongoClient
import numpy as np

import const
import util
from index import dv2

# this project
if __name__ == '__main__':
  import sys

# https://www.cnblogs.com/nxf-rabbit75/p/11111825.html

VERSION = '2.0.0.7'

DIR_BUY = const.DV2.DIR_BUY
DIR_NONE = const.DV2.DIR_NONE
DIR_SELL = const.DV2.DIR_SELL
HOLD_MONEY = const.DV2.HOLD_MONEY
HOLD_STOCK = const.DV2.HOLD_STOCK
BUY_PERCENT = const.DV2.BUY_PERCENT
SELL_PERCENT = const.DV2.SELL_PERCENT
INVALID_SELL_PRICE = const.DV2.INVALID_SELL_PRICE
INVALID_BUY_PRICE = const.DV2.INVALID_BUY_PRICE
YEAR_POSITION = const.DV2.YEAR_POSITION
MIDYEAR_POSITION = const.DV2.MIDYEAR_POSITION

GPFH_KEY = const.GPFH_KEYWORD.KEY_NAME



# #交易日当天的信息##########################################
# class DayInfo:
#   def __init__(self):
#     self.year = None
#     self.date = None
#     self.index = None
#     self.price = None
#     self.pb = None
#     self.priceVec = []
#     #5日平均线等等，都放在这里


# 回撤相关信息
class RetracementMark:
  def __init__(self):
    self.clear()
  
  def copy(self):
    one = RetracementMark()
    one.value = self.value
    one.days = self.days
    one.beginPrice = self.beginPrice
    one.endPrice = self.endPrice
    one.begin = self.begin
    one.end = self.end
    return one
  
  def clear(self):
    self.value = 0  # 最大回撤
    self.days = 0  # 连续不创新高天数
    self.beginPrice = 0
    self.endPrice = 0
    self.begin = None
    self.end = None
  
  def ToDict(self, head):
    return {head + ':value': self.value, head + ':days': self.days, head + ':begin': self.begin,
            head + ':end': self.end,
            head + ':beginPrice': self.beginPrice, head + ':endPrice': self.endPrice}


class Retracement:
  def __init__(self):
    self.__current = RetracementMark()
    self.__history = RetracementMark()
    self.maxRetracementDaysLastCheck = None  # 上个检查持股时点
  
  def Record(self, date):
    old = self.current.days
    self.current.end = date
    # 如果回测创新高，记录
    if self.current.value > self.history.value:
      self.updateHistory(self.current.copy())
    # 回撤记录清零
    self.current.clear()
    self.maxRetracementDaysLastCheck = None
    return old
  
  def ToDict(self, head):
    return self.__history.ToDict(head)
  
  @property
  def current(self):
    return self.__current
  
  @property
  def history(self):
    return self.__history
  
  def updateHistory(self, one):
    self.__history = one


# 记录新高
class MaxRecord:
  def __init__(self):
    self.value = 0
    self.date = None
  
  def ToDict(self, head):
    return {head + ':value': self.value, head + ':date': self.date}


#########################################################
# 代表当天发生的所有事件，比如price，indexPrice，pb，除权，年报季报等等
class DayContext:
  # 除权日，吃需要分红和配股
  DIVIDEND_POINT = 1
  # 除权日，买卖预期价格需要调整
  DIVIDEND_ADJUST = 2
  # 季报已出，季报利润下跌需要卖出
  DANGEROUS_POINT = 3
  # 冷冻期，不交易
  COOLDOWN_RANGE = 4
  # buy
  BUY_EVENT = 5
  # sell
  SELL_EVENT = 6
  SELL_ALWAYS_EVENT = 7
  # 新的卖出目标价格
  TARGET_SELL_PRICE_EVENT = 8
  # 出买卖价格
  MAKE_DECISION = 9
  #统计
  OTHER_WORK = 10
  
  # priority##############################
  # 其实策略和账户响应根本不会在一起，没必要设置先后
  PRIORITY_JUMP = 0  # 跳过循环，这种必须有util字段
  PRIORITY_DIVIDEND_ADJUST = 50  # 除权引发价格调整
  
  PRIORITY_MAKEDECISION = 900
  PRIORITY_STRATEGY_END = 1000
  
  PRIORITY_BEFORE_DIVIDEND = 1100  # 发生在除权前
  PRIORITY_BEFORE_TRADE = 1200  # 买卖前
  PRIORITY_TRADE = 1300  #
  PRIORITY_AFTER_TRADE = 1400  #
  
  class Task:
    def __init__(self, priority, key, util=None, *args, **kwargs):
      self.priority = priority
      self.key = key
      self.util = util
      self.args = args
      self.kwargs = kwargs
    
    # def __cmp__(self, other):
    #   if self.priority < other.priority:
    #     return -1
    #   elif self.priority == other.priority:
    #     return 0
    #   else:
    #     return 1
    def __lt__(self, other):
      return self.priority < other.priority
  
  def __init__(self, code):
    self.code = code
    self.year = None
    self.date = None
    self.index = None
    self.price = None
    self.pb = None
    self.priceVec = []
    self.AH = []  # Account注册处理handle
    self.SH = []  # Account注册处理handle
    
    # self.extra = {}
    # 带优先级的任务队列，Account
    self.taskPQAccount = PriorityQueue()
    self.taskPQStrategy = PriorityQueue()
    # 临时存放策略信息，回头挪走
    self.dvInfo = None

    self.cooldown = False
    self.cooldownEnd = None
    
    
  
  def Add_A(self, priority, key, util=None, *args, **kwargs):
    # util 为None，标示当天有效
    # util 为一个timstamp，表示时间戳之前有效
    self.taskPQAccount.put(DayContext.Task(priority, key, util, *args, **kwargs))
  
  def Add_S(self, priority, key, util=None, *args, **kwargs):
    # util 为None，标示当天有效
    # util 为一个timstamp，表示时间戳之前有效
    self.taskPQStrategy.put(DayContext.Task(priority, key, util, *args, **kwargs))
  
  def Add_AH(self, handle):
    self.AH.append(handle)
  
  def Add_SH(self, handle):
    self.SH.append(handle)
  
  def Loop(self):
    # 先策略，后账户
    self.__loopInner(self.taskPQStrategy, self.SH)
    self.__loopInner(self.taskPQAccount, self.AH)
  
  def __loopInner(self, taskPQ, H):
    tmp = []
    while taskPQ.qsize() != 0:
      task = taskPQ.get_nowait()
      for handle in H:
        handle(self, task)
      tmp.append(task)
    
    for one in tmp:
      if one.util is None:
        # del self.extra[k]
        pass
      elif self.date >= one.util:
        # del self.extra[k]
        pass
      else:
        taskPQ.put(one)
  

  
  def NewDay(self, date, row):
    self.year = date.year
    self.date = date
    self.price = row[self.code]
    self.index = row['close']
    # pb = row['pb']
    if len(self.priceVec) > 5:
      self.priceVec = self.priceVec[1:]
    self.priceVec.append((date, self.price, self.index))


#########################################################
# 代表账号信息
class Account:
  def __init__(self, code, beginMoney, startDate, endDate):
    self.code = code
    self.status = HOLD_MONEY  # 持仓还是持币
    # self.money = 100000 #持币的时候，这个表示金额，持仓的的时候表示不够建仓的资金
    self.startDate = startDate
    self.endDate = endDate
    self.BEGIN_MONEY = beginMoney
    self.money = self.BEGIN_MONEY
    self.oldMoney = self.money
    self.oldPrice = None  # 建仓价格，不包括追加买入除权买入，等于当初广播的买点价格triggerPrice
    self.number = 0  # 持仓的时候表示持仓数目(手)
    
    self.indexBuyPoint = None
    self.tradeCounter = 0
    self.tradeList = []
    self.holdStockDate = 0  # 持股总交易天数
    self.holdStockNatureDate = 0  # 持股总自然天数
    self.holdStockDateVec = []  # 持股区间
    self.holdStockNatureDateLastCheck = None  # 上个检查持股自然日的时点
    self.beforeProfit = None
    
    self.result = None  # 最后的交易结果
    self.sellPrice = None  # 卖出价格
    
    self.indexProfit = 1  # 沪深300指数收益
    self.indexBuyPoint = None  # 股票开仓时候沪深300指数点位
    
    # 最大账户净值
    self.maxValue = MaxRecord()
    self.maxValue.value = self.BEGIN_MONEY
    self.maxValue.date = self.startDate
    # 回撤相关
    self.Retracement = Retracement()
    # 事件处理
  
  def isHoldStock(self):
    return self.status == HOLD_STOCK
  
  def isHoldMoney(self):
    return self.status == HOLD_MONEY
  
  def buyInner(self, price, money):
    # 计算多少钱买多少股，返回股数，钱数
    number = money // (price * 100)
    restMoney = money - number * 100 * price
    return (number, restMoney)
  
  def Before(self, context: DayContext):
    # 永久有效
    context.Add_A(DayContext.PRIORITY_TRADE, DayContext.SELL_ALWAYS_EVENT,
                  pd.Timestamp(self.endDate))
    context.Add_A(DayContext.PRIORITY_AFTER_TRADE, DayContext.OTHER_WORK,
                  pd.Timestamp(self.endDate))
  
  def After(self, context: DayContext):
    # self.processOther(context.date, context.price)
    pass
  
  def Buy(self, date, triggerPrice, sellPrice, price, indexPrice, where, reason=''):
    try:
      # 如果持币，以固定价格买入
      self.oldPrice = triggerPrice
      if price <= triggerPrice:
        if self.isHoldMoney():
          # 卖出的价格是在建仓的时候决定的
          self.sellPrice = sellPrice
          self.oldMoney = self.money
          
          number, money = self.buyInner(price, self.money)
          self.number = number
          self.money = money
          self.status = HOLD_STOCK
          
          # 记录指数变化
          self.indexBuyPoint = indexPrice
          # 交易次数+1
          self.tradeCounter += 1
          self.holdStockDateVec.append(date)
          
          # 记录
          mark = TradeMark()
          mark.reason('买入').date(date).dir(DIR_BUY).total(self.number * 100 * price).number(self.number).price(
            price).where(where).extraInfo('{}'.format(reason)).Dump()
          self.tradeList.append(mark)
          # print("买入： 日期：{}, 触发价格：{}, 价格：{}, 数量：{}, 剩余资金：{}, 原因：{}".format(date, triggerPrice, price, self.number, self.money, reason))
        elif self.isHoldStock():
          number, money = self.buyInner(price, self.money)
          if number > 0:
            # 追加买入
            self.number += number
            self.money = money
            
            mark = TradeMark()
            mark.reason('追加买入').date(date).dir(DIR_BUY).total(self.number * 100 * price).number(self.number).price(
              price).where(where).extraInfo('剩余资金：{}'.format(self.money)).Dump()
            self.tradeList.append(mark)
            # print("买入（追加买入）： 日期：{}, 触发价格：{}, 价格：{}, 追加数量：{}, 数量：{}, 剩余资金：{}, 原因：{}".format(date, triggerPrice, price,
            #                                                                                nm[0], self.number, self.money, reason))
    except Exception as e:
      print(e)
  
  def Sell(self, date, sellPrice, price, indexPrice, reason=''):
    if self.isHoldStock():
      # 在由年报决定买卖的情况下，如果卖出价格是INVALID_SELL_PRICE
      # 说明当年没有分红，那就必须无条件卖出了
      self.SellNoCodition(date, price, indexPrice, reason)
  
  def SellNoCodition(self, date, price, indexPrice, reason=''):
    if self.isHoldStock():
      self.money = self.money + self.number * 100 * price
      winLoss = (self.money - self.oldMoney) / self.oldMoney
      self.status = HOLD_MONEY
      # 计算指数收益
      self.indexProfit *= indexPrice / self.indexBuyPoint
      self.holdStockDateVec[-1] = (self.holdStockDateVec[-1], date)
      
      # 记录
      mark = TradeMark()
      mark.reason('卖出').date(date).dir(DIR_SELL).total(self.number * 100 * price).number(self.number).price(
        price).extraInfo(
        '盈亏：{}, 原因：{}'.format(winLoss, reason)).Dump()
      self.tradeList.append(mark)
  
  def ProcessDividend(self, date, buyPrice, price, indexPrice, dividend):
    if self.isHoldStock():
      if buyPrice >= price:
        self.oldPrice = buyPrice
        oldMoney = self.money
        oldNumber = self.number
        dividendMoney = self.number * 100 * dividend.dividendAfterTax
        money = dividendMoney + self.money
        
        # 如果有转送股，所有的价格，股数需要变动
        if dividend.gift > 0:
          self.number *= 1 + dividend.gift
          oldSellPrice = self.sellPrice
          # 因为除权导致的价格变动，checkPoint处已经处理，并会广播，所以无需在此处理
          # self.sellPrice /= 1 + dividend.gift
          # 此时价格已经变化，无需处理
          # price /= 1+dividend.gift
          print('发生转送股 {} {} {}'.format(self.number, oldSellPrice, self.sellPrice))
        
        number = money // (price * 100)
        self.money = money - number * 100 * price
        self.number += number
        
        # 记录
        mark = TradeMark()
        mark.reason('除权买入').date(date).dir(DIR_BUY).total(number * 100 * price).number(number). \
          price(price).extraInfo('分红金额：{}'.format(dividendMoney)).Dump()
        self.tradeList.append(mark)
      else:
        oldMoney = self.money
        dividendMoney = self.number * 100 * dividend.dividendAfterTax
        self.money += dividendMoney
        
        # 如果有转送股，所有的价格，股数需要变动
        if dividend.gift > 0:
          self.number *= 1 + dividend.gift
          oldSellPrice = self.sellPrice
          # 因为除权导致的价格变动，checkPoint处已经处理，并会广播，所以无需在此处理
          # self.sellPrice /= 1 + dividend.gift
          # price /= 1 + dividend.gift
          print('发生转送股2 {} {} {}'.format(self.number, oldSellPrice, self.sellPrice))
        
        # 记录
        mark = TradeMark()
        mark.reason('除权不买入').date(date).dir(DIR_NONE).total(dividendMoney).number(0).price(0).extraInfo(
          '分红金额：{}'.format(dividendMoney)).Dump()
        self.tradeList.append(mark)
  
  def processOther(self, date, price):
    # 最大净值
    if self.isHoldMoney():
      pass
    elif self.isHoldStock():
      # 新高
      if self.number * 100 * price + self.money > self.maxValue.value:
        self.maxValue.value = self.number * 100 * price + self.money
        self.maxValue.date = date
        old = self.Retracement.Record(date)
        print('新高, 日期：{}, 净值：{}, 最近最大不创新高天数：{}'.format(date, self.maxValue.value, old))
    
    # 最大回撤
    if self.isHoldMoney():
      pass
    elif self.isHoldStock():
      nowValue = self.number * 100 * price + self.money
      # 回撤创新低
      if (self.maxValue.value - nowValue) / self.maxValue.value > self.Retracement.current.value:
        self.Retracement.current.value = (self.maxValue.value - nowValue) / self.maxValue.value
        if self.Retracement.maxRetracementDaysLastCheck is None:
          self.Retracement.maxRetracementDaysLastCheck = date
          self.Retracement.current.begin = date
          self.Retracement.current.beginPrice = price
        else:
          diff = date - self.Retracement.maxRetracementDaysLastCheck
          self.Retracement.maxRetracementDaysLastCheck = date
          self.Retracement.current.days += diff.days
        print('最大回撤, 日期：{}, 回撤：{}, 持续：{}'.format(date, self.Retracement.current.value, self.Retracement.current.days))
    
    # 持股交易日
    if self.isHoldStock():
      self.holdStockDate += 1
      # 持股自然日
      if self.holdStockNatureDateLastCheck is None:
        self.holdStockNatureDateLastCheck = date
      else:
        diff = date - self.holdStockNatureDateLastCheck
        self.holdStockNatureDateLastCheck = date
        self.holdStockNatureDate += diff.days
  
  def CloseAccount(self, current):
    money = 0
    if self.isHoldStock():
      money = self.money + self.number * 100 * current.price
      # 计算指数收益
      self.indexProfit *= current.index / self.indexBuyPoint
      # 可能结束回测的时候，都没有创新高，导致最大回撤记录没有刷新，在这里刷新下
      self.Retracement.Record(current.date)
      #刷新最后持股日期
      self.holdStockDateVec[-1] = (self.holdStockDateVec[-1], current.date)
    else:
      money = self.money
    
    one = TradeResult()
    one.code = self.code
    one.beginDate(pd.Timestamp(self.startDate)).endDate(current.date).status(self.status).beginMoney(
      self.BEGIN_MONEY). \
      total(money).days(self.holdStockDate).hs300Profit(self.indexProfit - 1)
    one.Calc()
    self.result = one
    one.Dump()
  
  def Process(self, context: DayContext, task):
    if task.key == DayContext.DIVIDEND_POINT:
      self.ProcessDividend(context.date, context.dvInfo[1], context.price, context.index, task.args[0].dividendPointOne)
    elif task.key == DayContext.DANGEROUS_POINT:
      # self.SellNoCodition()
      self.SellNoCodition(context.date, context.price, context.index,
                     reason='扣非卖出: {}'.format(task.args[0].point[4]))
      print('cooldownbegin2 {}'.format(task.args[0].point[0]))
      context.cooldown = True
      context.cooldownEnd = task.args[0].point[1]
      
    elif task.key == DayContext.BUY_EVENT:
      self.Buy(context.date, task.args[0], task.args[1], context.price, context.index, task.args[2], reason='低于买点')
    elif task.key == DayContext.SELL_EVENT:
      # 监控到价格变化的时候，必然符合当前价格高于建仓时候的卖出价格，建仓不应该再需要记录卖出价格
      # 除非策略变化，卖出价格和建仓有关，而非和年报有关
      reason = '年报未分红'
      if task.args[0] != INVALID_SELL_PRICE:
        reason = '高于卖点'
        if task.args[0] == self.sellPrice:
          pass
        else:
          print("wrong")
      self.Sell(context.date, task.args[0], context.price, context.index, reason)
    elif task.key == DayContext.TARGET_SELL_PRICE_EVENT:
      # if self.isHoldStock():
      # 如果已经持股，需要根据历年股息调整卖出价格
      self.sellPrice = task.args[0]
    elif task.key == DayContext.SELL_ALWAYS_EVENT:
      if self.isHoldStock() and context.price >= self.sellPrice:
        self.SellNoCodition(context.date, context.price, context.index, reason='高于卖点')
    elif task.key == DayContext.OTHER_WORK:
      self.processOther(context.date, context.price)
#########################################################
class DividendGenerator:
  class Event:
    def __init__(self, dp):
      self.dividendPointOne = dp
      pass
  
  def __init__(self, dv, dp):
    self.DV = dv
    self.dividendPoint = dp
    pass
  
  def __call__(self, *args, **kwargs):  # (self, context : DayContext):
    # 处理除权,
    # 除权日不可能不是交易日
    context = args[0]

    if not np.isnan(self.DV.eventDF.loc[context.date, 'dividend']):
      context.Add_A(DayContext.PRIORITY_BEFORE_TRADE, DayContext.DIVIDEND_POINT, None,
                    DividendGenerator.Event(self.DV.dividendMap[context.date]))

    # if len(self.dividendPoint) > 0:
    #   if context.date > self.dividendPoint[0].date:
    #     # 比如600900,在除权期间停牌。。。，对于这种目前简化为弹出这些除权，避免影响后续
    #     # TODO 如何处理停牌？
    #     while len(self.dividendPoint) > 0 and context.date > self.dividendPoint[0].date:
    #       print(' 弹出除权信息！！！ {}， {}'.format(context.priceVec, self.dividendPoint[0]))
    #       self.dividendPoint = self.dividendPoint[1:]
    #   elif len(self.dividendPoint) > 0 and context.date == self.dividendPoint[0].date:
    #     context.Add_A(DayContext.PRIORITY_BEFORE_TRADE, DayContext.DIVIDEND_POINT, None,
    #                   DividendGenerator.Event(self.dividendPoint[0]))
    #     self.dividendPoint = self.dividendPoint[1:]


#########################################################
class DangerousGenerator:
  class Event:
    def __init__(self, point):
      self.cooldown = True
      self.point = point

  
  def __init__(self, dv, dp):
    self.DV = dv
    self.dangerousPoint = dp

  
  def __call__(self, *args, **kwargs):  # (self, context : DayContext):
    context = args[0]
    # 处理季报，检查是否扣非-10%
    if not np.isnan(self.DV.eventDF.loc[context.date, 'dangerousPoint']):
      print('cooldownbegin {}'.format(self.DV.dangerousQuarterMap[context.date][0]))
      # context.Add_A(DayContext.PRIORITY_AFTER_TRADE, DayContext.DANGEROUS_POINT, self.DV.dangerousQuarterMap[context.date][1],
      #               DangerousGenerator.Event(self.DV.dangerousQuarterMap[context.date]))

      context.Add_A(DayContext.PRIORITY_AFTER_TRADE, DayContext.DANGEROUS_POINT,
                    None,
                    DangerousGenerator.Event(self.DV.dangerousQuarterMap[context.date]))

    # if len(self.dangerousPoint) > 0 and context.date >= self.dangerousPoint[0][0]:
    #   # 记录因为扣非为负的区间，在区间内屏蔽开仓
    #   print('cooldownbegin {}'.format(self.dangerousPoint[0][0]))
    #   context.Add_A(DayContext.PRIORITY_AFTER_TRADE, DayContext.DANGEROUS_POINT, self.dangerousPoint[0][1],
    #                 DangerousGenerator.Event(self.dangerousPoint[0]))
    #   # cooldown = True
    #   # cooldownEnd = self.dangerousPoint[0][1]
    #   self.dangerousPoint = self.dangerousPoint[1:]


#########################################################
class StrategyDV:
  def __init__(self, code, tu, startYear, startDate, endDate):
    self.strategyName = 'dv2'
    self.code = code
    self.startYear = startYear
    self.startDate = startDate
    self.endDate = endDate
    self.TU = tu
    self.dv2Index = dv2.DV2Index(code, startYear, startDate, endDate)
    # self.dv2Index.checkPoint = {}  # 所有的年报季报除权等影响买卖点的特殊时点
    # self.dangerousPoint = []  # 利润同比下滑超过10%的位置
    # self.dv2Index.dividendPoint = []  # 除权的日期
    # self.data = None  # 行情
    # self.MAXEND = Quater2Date(2099, 'first')  # 默认的冻结开仓截止日期
    
    # self.dividendAdjust = {}  # 除权日，调整买入卖出价格
    self.oldSellPrice = None  # 老的卖出目标价，如果年报半年报更新，这个价格可能更新
    # self.decisionCache = None
    # self.statisticsYears = None  # 参与统计的总年数
    # self.dividendYears = 0  # 有过分红的年数
    # 特殊年报时间
    self.specialPaper = {}
    # 事件生成函数数组
    self.generator = {
      # 'dividendPoint': DividendGenerator(),
    }
    #事件发生dataFrame
    self.eventDF = None
    self.dividendMap = {}  # 除权的详细信息，和eventDF配合使用
    self.dangerousQuarterMap = {}  # 危险季报详细信息，和eventDF配合使用
  
  
  def BuildGenerator(self):
    self.generator['dividendPoint'] = DividendGenerator(self, self.dv2Index.dividendPoint)
    self.generator['dangerousPoint'] = DangerousGenerator(self, self.dv2Index.dangerousPoint)
    
    
  def Before(self, context: DayContext):
    # 永久有效
    # context.Add_S(DayContext.PRIORITY_DIVIDEND_ADJUST, DayContext.DIVIDEND_ADJUST,
    #               pd.Timestamp(self.endDate))
    context.Add_S(DayContext.PRIORITY_MAKEDECISION, DayContext.MAKE_DECISION,
                  pd.Timestamp(self.endDate))
    pass
  
  def After(self, context: DayContext):
    pass
  
  def Process(self, context: DayContext, task):
    if task.key == DayContext.DIVIDEND_ADJUST:
      # 处理除权日时，需要调整买入卖出价格
      # if context.date in self.dividendAdjust:
      #   self.ProcessDividendAdjust(self.dividendAdjust[context.date])
      pass
    elif task.key == DayContext.MAKE_DECISION:
      self.MakeDecision(context)
  
  
  def genEventDF(self):
    self.eventDF = pd.concat([self.TU.baseIndex2, pd.DataFrame(columns=[
      'buyPrice', 'sellPrice', 'where',
      'dividend', 'dangerousPoint',
      # 'yearGift', 'yearDividend', 'midYearGift', 'midYearDividend', 'allDividend', 'yearDividendDate',
      # 'midYearDividendDate', 'earningsPerShare',
    ])], sort=False)
    # self.eventDF.drop(['willDrop', ], axis=1, inplace=True)
    
    #登记除权信息
    for one in self.dv2Index.dividendPoint:
      if one.date in self.eventDF.index:
        row = self.eventDF.loc[one.date]
        #除权日肯定是交易日
        self.eventDF.loc[one.date, 'dividend'] = True
        self.dividendMap[one.date] = one
        
    for one in self.dv2Index.dangerousPoint:
      if one[0] in self.eventDF.index:
        row = self.eventDF.loc[one[0]]
        # #not work
        # row['dangerousPoint'] = True
        # # not work
        # self.eventDF.loc[one[0]]['dangerousPoint'] = True
        #季报日期未必是交易日，需要找到下一个最近的交易日
        index = one[0]
        while np.isnan(self.eventDF.loc[index, 'close']):
          index += timedelta(days=1)
          
        self.eventDF.loc[index, 'dangerousPoint'] = True
        self.dangerousQuarterMap[index] = one

    self.checkPoint2DF()
    
  def CheckPrepare(self):
    print('### {} CheckPrepare########'.format(self.code))
    self.dv2Index.Run()
    
    #根据分析数据生成eventDF
    self.genEventDF()
    #作废原始数据
    # self.dv2Index.checkPoint = {}
    # self.dangerousPoint = []
    # self.dv2Index.dividendPoint = []
    # self.dividendAdjust = {}
    
    
  
  # def processSong(self, value):
  #   # 处理送股
  #   index = value.find('送')
  #   index2 = value.find('转')
  #   if index != -1:
  #     newValue = value[index + 1:]
  #     out = util.String2Number(newValue)
  #     return out
  #   elif index2 != -1:
  #     newValue = value[index2 + 1:]
  #     out = util.String2Number(newValue)
  #     return out
  #   else:
  #     return 0
  #
  # def processPai(self, value):
  #   # 处理派息
  #   index = value.find('派')
  #   if index != -1:
  #     newValue = value[index + 1:]
  #     out = util.String2Number(newValue)
  #     return out
  #   else:
  #     return 0
  #
  # def processPai2(self, value):
  #   # 处理派息，税后
  #   index = value.find('扣税后')
  #   if index != -1:
  #     newValue = value[index + 1:]
  #     out = util.String2Number(newValue)
  #     return out
  #   else:
  #     return 0
  #
  # def CalcDividend(self, year, position):
  #   # '10送3.00派4.00元(含税,扣税后3.30元)'
  #   if const.GPFH_KEYWORD.KEY_NAME['AllocationPlan'] in self.dv2Index.checkPoint[year][position]:
  #     value = self.dv2Index.checkPoint[year][position][const.GPFH_KEYWORD.KEY_NAME['AllocationPlan']]
  #     number = util.String2Number(value)
  #     profit = self.processPai(value)
  #     self.dv2Index.checkPoint[year][position]['dividend'] = profit / number
  #     profit2 = self.processPai2(value)
  #     self.dv2Index.checkPoint[year][position]['dividend_aftertax'] = profit2 / number
  #     gift = self.processSong(value)
  #     self.dv2Index.checkPoint[year][position]['gift'] = gift / number
  #
  #     if const.GPFH_KEYWORD.KEY_NAME['CQCXR'] in self.dv2Index.checkPoint[year][position]:
  #       tmpDate = self.dv2Index.checkPoint[year][position][const.GPFH_KEYWORD.KEY_NAME['CQCXR']]
  #       # 农业银行，2010年
  #       if tmpDate == '-':
  #         if position == 'midYear':
  #           # 半年默认在当年年底除权
  #           tmpDate = pd.Timestamp(datetime(year, 12, 1))
  #         else:
  #           tmpDate = pd.Timestamp(datetime(year + 1, 6, 30))
  #       else:
  #         tmpDate = pd.to_datetime(np.datetime64(tmpDate))
  #       self.dv2Index.dividendPoint.append(DividendPoint(
  #         tmpDate,
  #         self.dv2Index.checkPoint[year][position]['dividend'],
  #         self.dv2Index.checkPoint[year][position]['dividend_aftertax'],
  #         self.dv2Index.checkPoint[year][position]['gift'],
  #         year, position))
  #
  # def ProcessQuarterPaper(self, year, position):
  #   if 'sjltz' in self.dv2Index.checkPoint[year][position]:
  #     speed = -11
  #     try:
  #       # 存在这个值为“-”的情形
  #       speed = float(self.dv2Index.checkPoint[year][position]['sjltz'])
  #       self.dv2Index.checkPoint[year][position]['sjltz'] = speed
  #     except Exception as e:
  #       pass
  #
  #     if speed < -10:
  #       self.dangerousPoint.append((Quater2Date(year, position), self.MAXEND, year, position, speed))
  #     elif speed > 0:
  #       # 增速转正，如果之前有负的，要结合负的计算出冷冻区间（此区间不开仓）
  #       if len(self.dangerousPoint) > 0:
  #         # 找到所有没有填充终止的条目，全部填上当前时间点
  #         dirtyFlag = False
  #         for index in range(len(self.dangerousPoint)):
  #           if self.dangerousPoint[index][1] == self.MAXEND:
  #             dirtyFlag = True
  #             self.dangerousPoint[index] = (
  #               self.dangerousPoint[index][0], Quater2Date(year, position), self.dangerousPoint[index][2],
  #               self.dangerousPoint[index][3], self.dangerousPoint[index][4])
  #
  #         # 清理在起始时间小于终止时间的区间（比如从2011年2季度开始，2,3,4季度都是负，2012年一季度转正）
  #         # 此时dangerousPoint里面有多个条目：（2011-2，2012-1），（2011-3，2012-1），（2011-4，2012-1）
  #         # 其中（2011-3，2012-1），（2011-4，2012-1）是没有意义的，需要删除
  #         if dirtyFlag:
  #           tmp = []
  #           have = set()
  #           for index in range(len(self.dangerousPoint)):
  #             if self.dangerousPoint[index][1] not in have:
  #               have.add(self.dangerousPoint[index][1])
  #               tmp.append(self.dangerousPoint[index])
  #             else:
  #               pass
  #           self.dangerousPoint = tmp
  
  # def MakeDecisionPrice(self, date):
  #   # 决定使用哪个年的checkpoit，返回对应的buy和sell
  #
  #   anchor0 = pd.Timestamp(datetime(date.year, 4, 30))
  #   anchor1 = pd.Timestamp(datetime(date.year, 8, 31))
  #   # 特殊年报调整
  #   if date.year in self.specialPaper:
  #     if 0 in self.specialPaper[date.year]:
  #       anchor0 = self.specialPaper[date.year][0]
  #     if 1 in self.specialPaper[date.year]:
  #       anchor1 = self.specialPaper[date.year][1]
  #
  #   where = None
  #   try:
  #     if date <= anchor0:
  #       # 在4月30日之前，只能使用去年的半年报，如果半年报没有，则无法交易
  #       where = str(date.year - 1) + '-midYear'
  #       if self.dv2Index.checkPoint[date.year - 1]['buyPrice2'] > 0:
  #         return True, self.dv2Index.checkPoint[date.year - 1]['buyPrice2'], self.dv2Index.checkPoint[date.year - 1]['sellPrice2'], where
  #     elif date <= anchor1:
  #       # 在8月31日之前，需要使用去年的年报
  #       where = str(date.year - 1) + '-year'
  #       if self.dv2Index.checkPoint[date.year]['buyPrice'] > 0:
  #         return True, self.dv2Index.checkPoint[date.year]['buyPrice'], self.dv2Index.checkPoint[date.year]['sellPrice'], where
  #     else:
  #       # 在8月31日之后，使用去年的allDividend和今年半年报中dividend中大的那个决定
  #       buy = self.dv2Index.checkPoint[date.year]['buyPrice']
  #       midBuy = self.dv2Index.checkPoint[date.year]['buyPrice2']
  #       if buy > midBuy and buy > 0:
  #         return True, buy, self.dv2Index.checkPoint[date.year]['sellPrice'], str(date.year - 1) + '-year2'
  #       else:
  #         if midBuy > 0:
  #           return True, midBuy, self.dv2Index.checkPoint[date.year]['sellPrice2'], str(date.year) + '-midYear2'
  #         else:
  #           return False, INVALID_BUY_PRICE, INVALID_SELL_PRICE, str(date.year - 1) + '-year3'
  #   except Exception as e:
  #     pass
  #
  #   return False, INVALID_BUY_PRICE, INVALID_SELL_PRICE, where
  
  def MakeDecisionPrice2(self, date):
    return self.eventDF.loc[date, 'buyPrice'], self.eventDF.loc[date, 'sellPrice'], self.eventDF.loc[date, 'where']
    # 决定使用哪个年的checkpoit，返回对应的buy和sell
    # if self.decisionCache is not None:
    #   if date <= self.decisionCache[0]:
    #     return self.decisionCache[1]
    #   else:
    #     self.decisionCache = None
    #
    # anchor0 = pd.Timestamp(datetime(date.year, 4, 30))
    # anchor1 = pd.Timestamp(datetime(date.year, 8, 31))
    # anchor2 = pd.Timestamp(datetime(date.year, 12, 31))
    # # 特殊年报调整
    # if date.year in self.specialPaper:
    #   if 0 in self.specialPaper[date.year]:
    #     anchor0 = self.specialPaper[date.year][0]
    #   if 1 in self.specialPaper[date.year]:
    #     anchor1 = self.specialPaper[date.year][1]
    #
    # where = None
    # try:
    #   if date <= anchor0:
    #     # 在4月30日之前，只能使用去年的半年报，如果半年报没有，则无法交易
    #     where = str(date.year) + '-midYear'
    #     tmp = self.dv2Index.checkPoint[date.year - 1]['buyPrice2'], self.dv2Index.checkPoint[date.year - 1]['sellPrice2'], where
    #     self.decisionCache = (anchor0, tmp)
    #     return tmp
    #   elif date <= anchor1:
    #     # 在8月31日之前，需要使用去年的年报
    #     where = str(date.year - 1) + '-year'
    #     tmp = self.dv2Index.checkPoint[date.year]['buyPrice'], self.dv2Index.checkPoint[date.year]['sellPrice'], where
    #     self.decisionCache = (anchor1, tmp)
    #     return tmp
    #   else:
    #     # 在8月31日之后，使用去年的allDividend和今年半年报中dividend中大的那个决定
    #     buy = self.dv2Index.checkPoint[date.year]['buyPrice']
    #     midBuy = self.dv2Index.checkPoint[date.year]['buyPrice2']
    #     tmp = None
    #     if buy > midBuy and buy > 0:
    #       tmp = buy, self.dv2Index.checkPoint[date.year]['sellPrice'], str(date.year - 1) + '-year2'
    #     else:
    #       if midBuy > 0:
    #         tmp = midBuy, self.dv2Index.checkPoint[date.year]['sellPrice2'], str(date.year) + '-midYear2'
    #       else:
    #         tmp = INVALID_BUY_PRICE, INVALID_SELL_PRICE, str(date.year - 1) + '-year3'
    #     self.decisionCache = (anchor2, tmp)
    #     return tmp
    # except Exception as e:
    #   pass
    # # should not run here
    # return INVALID_BUY_PRICE, INVALID_SELL_PRICE, where

  def checkPoint2DF(self):
    
    for year in range(self.startDate.year, self.endDate.year+1):
      #今年一阶段：2010年：[1月1, 4月30]，由2009半年报决定
      #今年二阶段：2010年：(4月30, 8月31]，有2009年报决定
      #今年三阶段：2010年：(8月31, 12月31]，由2009年报2010年半年报中收益高的决定
      anchor0 = pd.Timestamp(datetime(year, 1, 1))
      anchor1 = pd.Timestamp(datetime(year, 4, 30))
      anchor2 = pd.Timestamp(datetime(year, 8, 31))
      anchor3 = pd.Timestamp(datetime(year, 12, 31))
      # 特殊年报调整
      if year in self.specialPaper:
        if 0 in self.specialPaper[year]:
          anchor1 = self.specialPaper[year][0]
        if 1 in self.specialPaper[year]:
          anchor2 = self.specialPaper[year][1]

      # TODO wrong should year-1
      self.eventDF.loc[anchor0:anchor1, 'where'] = str(year) + '-midYear'
      self.eventDF.loc[anchor0:anchor1, 'buyPrice'] = self.dv2Index.checkPoint[year-1]['buyPrice2']
      self.eventDF.loc[anchor0:anchor1, 'sellPrice'] = self.dv2Index.checkPoint[year - 1]['sellPrice2']

      self.eventDF.loc[anchor1 + timedelta(days=1):anchor2, 'where'] = str(year - 1) + '-year'
      self.eventDF.loc[anchor1+timedelta(days=1):anchor2, 'buyPrice'] = self.dv2Index.checkPoint[year]['buyPrice']
      self.eventDF.loc[anchor1+timedelta(days=1):anchor2, 'sellPrice'] = self.dv2Index.checkPoint[year]['sellPrice']
      
      buy = self.dv2Index.checkPoint[year]['buyPrice']
      sell = self.dv2Index.checkPoint[year]['sellPrice']
      where = str(year-1) + '-year2'
      midBuy = self.dv2Index.checkPoint[year]['buyPrice2']
      tmp = None
      flag = 1
      if buy > midBuy and buy > 0:
        pass
      else:
        if midBuy > 0:
          flag = 2
          buy = midBuy
          sell = self.dv2Index.checkPoint[year]['sellPrice2']
          where = str(year) + '-midYear2'

      self.eventDF.loc[anchor2 + timedelta(days=1):anchor3, 'buyPrice'] = buy
      self.eventDF.loc[anchor2 + timedelta(days=1):anchor3, 'sellPrice'] = sell
      self.eventDF.loc[anchor2 + timedelta(days=1):anchor3, 'where'] = where
      
      #除权调整
      #1 去年半年报的除权，要影响今年一阶段
      if 'dividendAdjust' in self.dv2Index.checkPoint[year-1]['midYear']:
        tmp = self.dv2Index.checkPoint[year-1]['midYear']['dividendAdjust']
        self.eventDF.loc[anchor0:anchor1, 'buyPrice'] = tmp['buyPriceX']
        self.eventDF.loc[anchor0:anchor1, 'sellPrice'] = tmp['sellPriceX']
        # 如果用了今年半年报，今年半年报除权影响三阶段部分
        if flag == 2 and 'dividendAdjust' in self.dv2Index.checkPoint[year]['midYear']:
          tmp = self.dv2Index.checkPoint[year]['midYear']['dividendAdjust']
          self.eventDF.loc[tmp['date']:anchor3, 'buyPrice'] = tmp['buyPriceX']
          self.eventDF.loc[tmp['date']:anchor3, 'sellPrice'] = tmp['sellPriceX']
      #2 去年年报除权，影响今年二阶段部分
      if 'dividendAdjust' in self.dv2Index.checkPoint[year]['year']:
        tmp = self.dv2Index.checkPoint[year]['year']['dividendAdjust']
        self.eventDF.loc[tmp['date']:anchor2, 'buyPrice'] = tmp['buyPriceX']
        self.eventDF.loc[tmp['date']:anchor2, 'sellPrice'] = tmp['sellPriceX']
        #如果今年三阶段用了去年年报
        if flag == 1:
          self.eventDF.loc[tmp['date']:anchor3, 'buyPrice'] = tmp['buyPriceX']
          self.eventDF.loc[tmp['date']:anchor3, 'sellPrice'] = tmp['sellPriceX']
  
  
  def MakeDecision(self, context: DayContext):
    buySignal, sellSignal = self.BuySellSignal(context.date, context.price)
    context.dvInfo = (None, buySignal[1], sellSignal[1], None)
    if buySignal[0]:
      context.Add_A(DayContext.PRIORITY_TRADE, DayContext.BUY_EVENT, None, buySignal[1], sellSignal[1], buySignal[2])
    
    if sellSignal[0]:
      context.Add_A(DayContext.PRIORITY_TRADE, DayContext.SELL_EVENT, None, sellSignal[1], sellSignal[2])
    
    if sellSignal[1] != INVALID_SELL_PRICE:
      notify = False
      if self.oldSellPrice is None:
        self.oldSellPrice = sellSignal[1]
        notify = True
      elif self.oldSellPrice != sellSignal[1]:
        self.oldSellPrice = sellSignal[1]
        notify = True
      if notify:
        context.Add_A(DayContext.PRIORITY_BEFORE_DIVIDEND, DayContext.TARGET_SELL_PRICE_EVENT, None, sellSignal[1])
  
  def BuySellSignal(self, date, price):
    buyPrice, sellPrice, where = self.MakeDecisionPrice2(date)
    buySignal = (False, buyPrice, where)
    sellSignal = (False, sellPrice, where)
    # 买卖价格有效则都有效，无效则都无效
    if buyPrice != INVALID_BUY_PRICE:
      if buyPrice >= price:
        buySignal = (True, buyPrice, where)
      elif sellPrice <= price:
        sellSignal = (True, sellPrice, where)
    elif sellPrice == INVALID_SELL_PRICE and isinstance(where, str) and where.find('-year') != -1:
      sellSignal = (True, sellPrice, where)
    
    return buySignal, sellSignal
  
  # def ProcessDividendAdjust(self, data):
  #   # checkPoint发生调整，缓存清掉
  #   self.decisionCache = None
  #   if data['p'] == 'midYear':
  #     self.dv2Index.checkPoint[data['y']]['buyPrice2'] = data['buyPriceX']
  #     self.dv2Index.checkPoint[data['y']]['sellPrice2'] = data['sellPriceX']
  #   elif data['p'] == 'year':
  #     self.dv2Index.checkPoint[data['y']]['buyPrice'] = data['buyPriceX']
  #     self.dv2Index.checkPoint[data['y']]['sellPrice'] = data['sellPriceX']


#########################################################
class TradeManager:
  # 沪深300，如果跑多个实例无需重复load数据
  DF_HS300 = None
  baseIndex = None  # 自然日index
  baseIndex2 = None  # 自然日index，带了hs300数据，也就是知道那些日子是交易日
  
  # 代表交易管理
  def __init__(self, stocks, beginMoney):
    
    self.startYear = 2011  # 起始年份
    start = str(self.startYear) + '-01-01T00:00:00Z'
    self.startDate = parser.parse(start, ignoretz=True)
    self.endYear = 2019  # 结束年份
    # end = str(self.endYear) + '-12-13T00:00:00Z'
    end = str(self.endYear) + '-12-20T00:00:00Z'
    self.endDate = parser.parse(end, ignoretz=True)

    # self.MAXEND = Quater2Date(2099, 'first')  # 默认的冻结开仓截止日期
    self.BEGIN_MONEY = beginMoney
    
    self.mongoClient = MongoClient()
    self.stocks = stocks
    self.data = []  # 行情
    self.index = None  # 沪深300指数

    self.collectionName = 'dv2'  # 存盘表名
    
    #支持多个品种测试，每个品种一个dv和一个account
    self.dvMap = {}
    self.accountMap = {}
    self.context = {}#DayContext()  # 代表全部的事件
    self.codes = [] #单独存放所有的股票代码
    self.listen = {} #ListenOne
    for one in stocks:
      tmpBeginMoney = beginMoney
      self.codes.append(one['_id'])
      if 'money' in one:
        tmpBeginMoney = one['money']
      A = Account(one['_id'], tmpBeginMoney, self.startDate, self.endDate)
      DV = StrategyDV(one['_id'], self, self.startYear, self.startDate, self.endDate)
      self.dvMap[one['_id']] = DV
      self.accountMap[one['_id']] = A
      context = DayContext(one['_id'])
      context.Add_AH(A.Process)
      context.Add_SH(DV.Process)
    
      self.context[one['_id']] = context


      listen = ListenOne(one['_id'], one['name'], DV.strategyName)
      context.Add_AH(listen.Process)
      self.listen[one['_id']] = listen


    
    
    
    
    # 特殊年报时间
    self.ALL_SPECIAL_PAPER = {}
    # self.specialPaper = {}
    
    self.ALL_SPECIAL_PAPER['601009'] = {
      2015: {
        # 0: pd.Timestamp(datetime(date.year, 4, 30)),
        1: pd.Timestamp(datetime(2015, 8, 20))
      },
    }
    
    self.ALL_SPECIAL_PAPER['600660'] = {
      2011: {
        0: pd.Timestamp(datetime(2011, 3, 20)),
        # 1: pd.Timestamp(datetime(2015, 8, 20))
      },
      2015: {
        0: pd.Timestamp(datetime(2015, 3, 20)),
        # 1: pd.Timestamp(datetime(2015, 8, 20))
      },
    }
    
    self.ALL_SPECIAL_PAPER['601818'] = {
      2015: {
        0: pd.Timestamp(datetime(2015, 4, 20)),
        # 1: pd.Timestamp(datetime(2015, 8, 20))
      },
    }
    
    self.ALL_SPECIAL_PAPER['600585'] = {
      2019: {
        0: pd.Timestamp(datetime(2019, 3, 25)),
        # 1: pd.Timestamp(datetime(2015, 8, 20))
      },
    }
    
    for one in self.codes:
      if one in self.ALL_SPECIAL_PAPER:
        self.dvMap[one].specialPaper = self.ALL_SPECIAL_PAPER[one]
  
  
  def CheckPrepare(self):
    for one in self.codes:
      self.dvMap[one].CheckPrepare()
      self.dvMap[one].BuildGenerator()
  
  
  # def LoadYearPaper(self, y, code):
  #   # 加载年报，中报
  #   midYear = {}
  #   year = {}
  #   db = self.mongoClient["stock"]
  #   collection = db["gpfh-" + str(y) + "-06-30"]
  #   cursor = collection.find({"_id": code})
  #   for c in cursor:
  #     midYear[const.GPFH_KEYWORD.KEY_NAME['CQCXR']] = c[const.GPFH_KEYWORD.KEY_NAME['CQCXR']]
  #     midYear[const.GPFH_KEYWORD.KEY_NAME['AllocationPlan']] = c[const.GPFH_KEYWORD.KEY_NAME['AllocationPlan']]
  #     midYear[const.GPFH_KEYWORD.KEY_NAME['EarningsPerShare']] = c[const.GPFH_KEYWORD.KEY_NAME['EarningsPerShare']]
  #     break
  #   else:
  #     midYear.update({'notExist': 1})
  #
  #   collection = db["gpfh-" + str(y) + "-12-31"]
  #   cursor = collection.find({"_id": code})
  #   for c in cursor:
  #     year[const.GPFH_KEYWORD.KEY_NAME['CQCXR']] = c[const.GPFH_KEYWORD.KEY_NAME['CQCXR']]
  #     year[const.GPFH_KEYWORD.KEY_NAME['AllocationPlan']] = c[const.GPFH_KEYWORD.KEY_NAME['AllocationPlan']]
  #     year[const.GPFH_KEYWORD.KEY_NAME['EarningsPerShare']] = c[const.GPFH_KEYWORD.KEY_NAME['EarningsPerShare']]
  #     break
  #   else:
  #     year.update({'notExist': 1})
  #
  #   return (midYear, year)
  
  
  # def LoadQuaterPaper(self, year, code):
  #   # 加载季报
  #   first = {}
  #   second = {}
  #   third = {}
  #   forth = {}
  #   db = self.mongoClient["stock"]
  #   collection = db["yjbg-" + code]
  #   strYear = str(year)
  #   # 一季度
  #   cursor = collection.find({"_id": strYear + "-03-31"})
  #   for c in cursor:
  #     first['sjltz'] = c['sjltz']
  #     break
  #
  #   # 二季度
  #   cursor = collection.find({"_id": strYear + "-06-30"})
  #   for c in cursor:
  #     second['sjltz'] = c['sjltz']
  #     break
  #
  #   # 三季度
  #   cursor = collection.find({"_id": strYear + "-09-30"})
  #   for c in cursor:
  #     third['sjltz'] = c['sjltz']
  #     break
  #
  #   # 四季度
  #   cursor = collection.find({"_id": strYear + "-12-31"})
  #   for c in cursor:
  #     forth['sjltz'] = c['sjltz']
  #     break
  #
  #   return (first, second, third, forth)
  
  
  def loadData(self, dbName, collectionName, condition):
    db = self.mongoClient[dbName]
    collection = db[collectionName]
    cursor = collection.find(condition).sort([('_id', 1)])
    out = []
    for c in cursor:
      out.append(c)
    
    if len(out):
      df = pd.DataFrame(out)
      df.drop('date', axis=1, inplace=True)
      df.set_index('_id', inplace=True)
      return df
    
    return None

  def loadData2(self, dbName, collectionName, condition):
    db = self.mongoClient[dbName]
    collection = db[collectionName]
    cursor = collection.find(condition).sort([('_id', 1)])
    out = []
    for c in cursor:
      out.append(c)
  
    if len(out):
      df = pd.DataFrame(out)
      df.drop('date', axis=1, inplace=True)
      df.drop('open', axis=1, inplace=True)
      df.drop('high', axis=1, inplace=True)
      df.drop('low', axis=1, inplace=True)
      df.drop('volume', axis=1, inplace=True)
      df.rename(columns={'close': collectionName, }, inplace=True)
      df.set_index('_id', inplace=True)
      return df
  
    return None
  # def loadPB(self):
  #   db = self.mongoClient['stock2']
  #   collection = db['pb2']
  #   cursor = collection.find({'_id': self.code})
  #   out = None
  #   for c in cursor:
  #     out = c['data']
  #     break
  #
  #   for one in out:
  #     one['date'] = pd.Timestamp(datetime.strptime(one['date'], '%Y-%m-%d'))
  #
  #   if len(out):
  #     df = pd.DataFrame(out)
  #     df.drop('timestamp', axis=1, inplace=True)
  #     df.drop('price', axis=1, inplace=True)
  #     df.set_index('date', inplace=True)
  #     return df
  #
  #   return None
  
  def LoadQuotations(self):
    for one in self.codes:
      self.data.append(self.loadData2("stock_all_kdata_none", one, {"_id": {"$gte": self.startDate}}))
    try:
      pass
      # beforeData = self.loadData("stock_all_kdata", self.code, {"_id": {"$gte": datetime1}})
      # startPrice = beforeData.iloc[0]['close']
      # endPrice = beforeData.iloc[-1]['close']
      # self.beforeProfit = (endPrice - startPrice)/startPrice
    except Exception as e:
      print(e)
  
  
  def LoadIndexs(self):
    if TradeManager.DF_HS300 is None:
      #引入每天的日程坐标，这样就不会遗漏任何一个非交易日信息，可以用精准的日期匹配触发事件了
      index = pd.date_range(start=self.startDate, end=self.endDate)
      TradeManager.baseIndex = pd.DataFrame(np.random.randn(len(index)), index=index, columns=['willDrop'])
      hs300 = self.loadData("stock_all_kdata_none", '000300', {"_id": {"$gte": self.startDate, "$lte": self.endDate}})
      TradeManager.baseIndex2 = TradeManager.baseIndex.join(hs300, how='left', lsuffix='_index')
      TradeManager.baseIndex2.drop(['willDrop', 'high', 'open', 'low', 'volume'], axis=1, inplace=True)
      TradeManager.DF_HS300 = TradeManager.baseIndex2
    self.index = TradeManager.DF_HS300
    
  
  
  def Merge(self):
    if self.index is not None:
      self.mergeData = self.index.join(self.data, how='left', lsuffix='_index')
      try:
        # pb = self.loadPB()
        # if pb is not None:
        #   self.mergeData = self.mergeData.join(pb, how='left')
        pass
      except Exception as e:
        pass
      #填充股票的空值，但是不处理沪深300指数的空值
      for one in self.codes:
        self.mergeData[one].fillna(method='ffill', inplace=True)
      # self.mergeData.fillna(method='ffill', inplace=True)  # 用前面的值来填充
  
  def BackTest(self):
    self.backTestInner(self.mergeData)
  
  def backTestOne(self, date, row, code):
    context = self.context[code]
    DV = self.dvMap[code]
    A = self.accountMap[code]
    if context.cooldown:
      if date >= context.cooldownEnd:
        context.cooldown = False
        print('cooldownend {}, {}'.format(code, context.cooldownEnd))
        context.cooldownEnd = None
      else:
        return
  
    # 环境数据更新
    context.NewDay(date, row)
  
    # 事件生成
    for k, v in DV.generator.items():
      v(context)
  
    context.Loop()
      
      
  def backTestInner(self, backtestData):
    # 回测
    print('BackTest {} ###########################'.format(self.codes))
    
    context = self.context
    
    # 环境处理bofore
    for one in self.codes:
      self.dvMap[one].Before(self.context[one])
      self.accountMap[one].Before(context[one])
    
    for date, row in backtestData.iterrows():
      if np.isnan(row['close']):
        continue
      try:
        for code in self.codes:
          self.backTestOne(date, row, code)
      
      
      except TypeError as e:
        util.PrintException(e)
      except KeyError as e:
        util.PrintException(e)
    
    # 环境处理after
    for one in self.codes:
      self.accountMap[one].After(context[one])


  
  def CloseAccount(self):
    for one in self.codes:
      self.accountMap[one].CloseAccount(self.context[one])
      self.listen[one].Dump()
  
  
  
  def StorePrepare2DB(self):
    for one in self.stocks:
      code = one['_id']
      name = one['name']
      
    pass
  
  def StoreResult2DB(self):
    # 保存交易记录到db，用于回测验证
    for one in self.stocks:
      code = one['_id']
      A = self.accountMap[code]
      out = {"_id": code, 'ver': VERSION, 'name': one['name']}
      out["beginMoney"] = A.BEGIN_MONEY
      tl = []
      for one in A.tradeList:
        tl.append(one.ToDB())
      out['tradeMarks'] = tl
      out.update(A.result.ToDB())
      out['beforeProfit'] = A.beforeProfit
      out['tradeCounter'] = A.tradeCounter
      out.update(A.maxValue.ToDict('maxValue'))
      out.update(A.Retracement.ToDict('Retracement'))
      
      out['holdStockNatureDate'] = A.holdStockNatureDate
      out['holdStockDateVec'] = A.holdStockDateVec
      util.SaveMongoDB(out, 'stock_backtest', self.collectionName)
  
  
  # def ExistCheckResult(self):
  #   db = self.mongoClient["stock_backtest"]
  #   collection = db[self.collectionName]
  #   cursor = collection.find({"_id": self.code})
  #   out = None
  #   for c in cursor:
  #     out = c
  #     break
  #
  #   if out is not None:
  #     return True
  #   else:
  #     return False
  
  
  def CheckResult(self):
    for one in self.stocks:
      code = one['_id']
      A = self.accountMap[code]
      db = self.mongoClient["stock_backtest"]
      collection = db['dv2']
      # collection = db[self.collectionName]
      cursor = collection.find({"_id": code})
      out = None
      for c in cursor:
        out = c
        break
      else:
        collection = db['all_dv2']
        cursor = collection.find({"_id": code})
        out = None
        for c in cursor:
          out = c
          break
      
      flag = False
      where = 0
      tmp = TradeResult.FromDB(out)
      marks = []
      for index in range(len(out['tradeMarks'])):
        marks.append(TradeMark.FromDB(out['tradeMarks'][index]))
      
      if A.BEGIN_MONEY == out['beginMoney']:
        if len(A.tradeList) == len(out['tradeMarks']):
          if A.result == tmp:
            for index in range(len(A.tradeList)):
              if A.tradeList[index] != marks[index]:
                where += 1
                print('### CheckResult failed {} {} {}'.format(code, one['name'], where))
                # return False
            else:
              flag = True
          else:
            where = 3
        else:
          where = 2
      else:
        where = 1
      if flag is not True:
        print('### CheckResult failed {} {} {}'.format(code, one['name'], where))
        # return False
    
    return True

# 交易记录#################################################
class TradeMark:
  def __init__(self):
    self.__date = None  # 交易发生的时间
    self.__dir = None  # 交易发生的方向，1 买入，-1 卖出
    self.__fee = None
    self.__number = None  # 股票交易数量
    self.__price = None  # 股票交易的金额
    self.__total = None  # 涉及的总金额
    self.__reason = None  # 交易原因
    self.__where = ''  # 根据哪一年的中报年报分红决定买入，仅开仓有效
    self.__extraInfo = None  # 其他附带信息
  
  def date(self, d):
    self.__date = d
    return self
  
  def dir(self, d):
    self.__dir = d
    return self
  
  def fee(self, d):
    self.__fee = d
    return self
  
  def number(self, d):
    self.__number = d
    return self
  
  def price(self, d):
    self.__price = d
    return self
  
  def total(self, d):
    self.__total = d
    return self
  
  def reason(self, d):
    self.__reason = d
    return self
  
  def extraInfo(self, d):
    self.__extraInfo = d
    return self
  
  def where(self, d):
    self.__where = d
    return self
  
  def Dump(self):
    print(self)
  
  def __str__(self):
    return "操作：{}, 日期：{}, 方向：{}, 金额：{}, 价格：{}, 数量：{}, 年报：{}, 附加信息：{}, ".format(
      self.__reason, self.__date, self.__dir, self.__total, self.__price, self.__number, self.__where, self.__extraInfo)
  
  def ToDB(self):
    return {'op': self.__reason, 'date': self.__date, 'dir': self.__dir, 'total': self.__total, 'number': self.__number,
            'price': self.__price,
            'where': self.__where, 'extraInfo': self.__extraInfo}
  
  def FromDB(db):
    one = TradeMark()
    one.reason(db['op']).date(db['date']).dir(db['dir']).total(db['total']).number(db['number']).price(db['price']). \
      where(db['where']).extraInfo(db['extraInfo'])
    return one
  
  def __eq__(self, obj):
    return self.__reason == obj.__reason and self.__date == obj.__date and self.__dir == obj.__dir \
           and self.__total == obj.__total and self.__price == obj.__price and self.__number == obj.__number \
           and self.__fee == obj.__fee and self.__where == obj.__where and self.__extraInfo == obj.__extraInfo


#########################################################

class TradeResult:
  def __init__(self):
    self.__beginDate = None  # 开始的时间
    self.__endDate = None  # 结算的时间
    self.__status = None  # 结算状态
    self.__profit = None  # 总收益
    self.__percent = None  # 股票收益率
    self.__total = None  # 涉及的总金额
    self.__beginMoney = None  # 开始资金
    self.__days = None  # 持股天数
    self.__hs300Profit = None  # 同期沪深300指数收益
  
  def beginDate(self, d):
    self.__beginDate = d
    return self
  
  def endDate(self, d):
    self.__endDate = d
    return self
  
  def status(self, d):
    self.__status = d
    return self
  
  def beginMoney(self, d):
    self.__beginMoney = d
    return self
  
  def number(self, d):
    self.__number = d
    return self
  
  def days(self, d):
    self.__days = d
    return self
  
  def total(self, d):
    self.__total = d
    return self
  
  def profit(self, d):
    self.__profit = d
    return self
  
  def percent(self, d):
    self.__percent = d
    return self
  
  def hs300Profit(self, d):
    self.__hs300Profit = d
    return self
  
  def Calc(self):
    self.__profit = self.__total - self.__beginMoney
    self.__percent = self.__profit / self.__beginMoney
  
  def Dump(self):
    print(self)
  
  def __str__(self):
    return "结算：开始：{}, 结束：{}, 持仓：{}, 开始资金：{}, 结束资金：{}, 绝对收益：{}, 相对收益：{}, 持股天数：{}, 同期沪深300：{}, ".format(
      self.__beginDate, self.__endDate, self.__status, self.__beginMoney, self.__total, self.__profit, self.__percent,
      self.__days, self.__hs300Profit)
  
  def ToDB(self):
    growth = []
    if self.code == '601515':
      growth.append('1.0.0.17.5 ==> 2.0.0.5:低版本默认在4月30日之前使用去年半年报。而此股18年年报规定19年4月23除权，新版本此日期起使用除权设定的买卖价格，导致差异')
    if self.code == '002014':
      growth.append('1.0.0.17.5 ==> 2.0.0.5:低版本默认在4月30日之前使用去年半年报。而此股17年年报规定18年4月18除权，新版本此日期起使用除权设定的买卖价格，导致差异')
      
      
    return {'beginDate': self.__beginDate, 'endDate': self.__endDate, 'status': self.__status,
            'beginMoney': self.__beginMoney, 'total': self.__total,
            'profit': self.__profit, 'percent': self.__percent, 'days': self.__days,
            'hs300Profit': self.__hs300Profit, 'growth': growth,}
  
  def FromDB(db):
    one = TradeResult()
    one.beginDate(db['beginDate']).endDate(db['endDate']).status(db['status']).beginMoney(db['beginMoney']).total(
      db['total']).profit(db['profit']). \
      percent(db['percent']).days(db['days']).hs300Profit(db['hs300Profit'])
    return one
  
  def __eq__(self, obj):
    # return self.__beginDate == obj.__beginDate and self.__endDate == obj.__endDate and self.__status == obj.__status \
    #        and self.__total == obj.__total and self.__beginMoney == obj.__beginMoney and self.__profit == obj.__profit \
    #        and self.__days == obj.__days and self.__percent == obj.__percent and self.__hs300Profit == obj.__hs300Profit
    
    return self.__beginDate == obj.__beginDate and self.__status == obj.__status \
           and self.__total == obj.__total and self.__beginMoney == obj.__beginMoney and self.__profit == obj.__profit \
           and self.__percent == obj.__percent and self.__hs300Profit == obj.__hs300Profit


#########################################################
class ListenOne:
  def __init__(self, code, name, strategyName):
    self.actionList = []
    self.code = code
    self.name = name
    self.strategyName = strategyName
    
    self.lastBuyPrice = None
    self.lastSellPrice = None
  
  def Process(self, context: DayContext, task):
    if task.key == DayContext.BUY_EVENT:
      diff = task.args[0] - context.price
      self.actionList.append((context.date, 1, context.price, diff, diff / context.price))
      self.lastBuyPrice = task.args[0]
    elif task.key == DayContext.SELL_EVENT:
      if task.args[0] != INVALID_SELL_PRICE:
        diff = task.args[0] - context.price
        percent = diff / context.price
        self.lastSellPrice = task.args[0]
      else:
        diff = np.nan
        percent = np.nan
        self.lastSellPrice = None
      self.actionList.append((context.date, -1, context.price, diff, percent))
    
    # elif task.key == DayContext.SELL_ALWAYS_EVENT:
    #   if len(self.actionList) and self.actionList[-1][0] == context.date:
    #     pass
    #   else:
    #     self.actionList.append((context.date, context.price, 0))
  
  def DumpOne(self, one):
    return '日期：{}, 操作：{}, 价格：{}, 价差：{:.3}, 百分比：{:.2%}'.format(one[0], one[1], one[2], one[3], one[4])
  
  def ToDB(self):
    out = {}
    date = None
    if len(self.actionList):
      date = self.actionList[-1][0]
      out['_id'] = self.code
      out['name'] = self.name
      out['操作'] = self.actionList[-1][1]
      out['价格'] = self.actionList[-1][2]
      out['价差'] = self.actionList[-1][3]
      out['百分比'] = self.actionList[-1][4]
      out['策略'] = self.strategyName
    return date, out
  
  def Dump(self):
    print('####{}， {}#############################'.format(self.code, self.name))
    print('####{}， {}####'.format(self.lastBuyPrice, self.lastSellPrice))
    for one in self.actionList[-10:]:
      print(self.DumpOne(one))


#########################################################
def RunOne(code, beginMoney, name, save=False, check=False):
  stock = TradeUnit(code, name, beginMoney)
  stock.LoadQuotations()
  stock.LoadIndexs()
  stock.Merge()
  stock.CheckPrepare()
  
  print(stock.DV.dv2Index.checkPoint)
  print(stock.DV.dv2Index.dangerousPoint)
  for one in stock.DV.dv2Index.dividendPoint:
    print(one)
  
  stock.BackTest()
  stock.CloseAccount()
  if save:
    stock.Store2DB()
  
  if check:
    assert stock.CheckResult()
  # print(stock.checkPoint)
  # print(stock.dv2Index.dangerousPoint)
  # print(stock.dv2Index.dividendPoint)
  return stock


def RunOne2(code, beginMoney, name, args):
  stock = TradeUnit(code, name, beginMoney)
  stock.LoadQuotations()
  stock.LoadIndexs()
  stock.Merge()
  stock.CheckPrepare()
  
  print(stock.DV.dv2Index.checkPoint)
  print(stock.DV.dv2Index.dangerousPoint)
  for one in stock.DV.dv2Index.dividendPoint:
    print(one)
  
  if 'backtest' in args:
    stock.BackTest()
    stock.CloseAccount()
  
  if 'save' in args:
    stock.Store2DB()
  
  if 'check' in args:
    return stock.CheckResult()
  
  return stock


def TestAll(codes, save, check):
  # # 皖通高速
  # Test2('600012', 52105, save, check)
  # # 万华化学
  # Test2('600309', 146005, save, check)
  # # 北京银行
  # Test2('601169', 88305, save, check)
  # # 大秦铁路
  # # 2015年4月27日那天，预警价为14.33元，但收盘价只有14.32元，我们按照收盘价计算，
  # # 差一分钱才触发卖出规则。如果当时卖出，可收回现金14.32*12500+330=179330元。
  # # 错过这次卖出机会后不久，牛市见顶，股价狂泻，从14元多一直跌到5.98元。
  # # TODO 需要牛市清盘卖出策略辅助
  # Test2('601006', 84305, save, check)
  # # 南京银行
  # Test2('601009', 75005, save, check)
  for one in codes:
    RunOne(one['code'], one['money'], one['name'], save, check)


def Digest(collectionName, condition={}):
  # 从一个策略结果里面提取摘要
  client = MongoClient()
  db = client["stock_backtest"]
  collection = db[collectionName]
  cursor = collection.find(condition)
  out = []
  for c in cursor:
    out.append(c)
  
  for one in out:
    tmp = {}
    tmp['_id'] = one['_id']
    tmp['name'] = one['name']
    tmp['beginDate'] = one['beginDate']
    tmp['endDate'] = one['endDate']
    tmp['beginMoney'] = one['beginMoney']
    tmp['total'] = one['total']
    tmp['percent'] = one['percent']
    tmp['maxValue:value'] = one['maxValue:value']
    tmp['Retracement:value'] = one['Retracement:value']
    tmp['Retracement:days'] = one['Retracement:days']
    util.SaveMongoDB(tmp, 'stock_backtest', collectionName + "_digest")


def CompareOne(collectionName, code, name):
  client = MongoClient()
  db = client["stock_backtest"]
  collection = db[collectionName]
  cursor = collection.find({"_id": code})
  out = None
  for c in cursor:
    out = c
    break
  
  if out is not None:
    result = {'code': code, 'name': name}
    result['profit'] = out['result']['percent']
    result['hs300Profit'] = out['result']['hs300Profit']
    result['beforeProfit'] = out['beforeProfit']
    result['winHS300'] = result['profit'] - result['hs300Profit']
    result['hold'] = out['result']['status']
    try:
      result['winAlwaysHold'] = result['profit'] - result['beforeProfit']
    except Exception as e:
      result['winAlwaysHold'] = result['profit']
      result['beforeProfit'] = 0
    
    result['tradeCounter'] = out['tradeCounter']
    return result


def CompareAll(collectionName, codes):
  out = []
  TOTAL = len(codes)
  # 跑赢沪深300的数量
  winHS300Number = 0
  # 跑赢持续持股的数量
  winHoldAlwaysNumber = 0
  # 沪深300总收益
  allHS300Profit = 0
  # 策略总收益
  allProfit = 0
  # 持续持股总收益，选股，交易过样本
  holdAlwaysProfit = 0
  # 不选股，300总样本
  holdAlwaysAllProfit = 0
  # 沪深300持续持有收益
  holdAllTimeHS300 = 0
  # 发生过交易的股票数目
  tradeCounter = 0
  # 回测结束时候，是持币的股票。如果持币，亏损属于真实亏损
  realLossCounter = 0
  tempLossCounter = 0
  HOLD_ALL_HS300_PROFIT = 0.24
  for one in codes:
    tmp = CompareOne(collectionName, one['code'], one['name'])
    holdAlwaysAllProfit += tmp['beforeProfit']
    if tmp['tradeCounter'] > 0:
      tradeCounter += 1
      holdAllTimeHS300 += HOLD_ALL_HS300_PROFIT
      if tmp['winHS300'] > 0:
        winHS300Number += 1
      
      if tmp['winAlwaysHold'] > 0:
        winHoldAlwaysNumber += 1
      
      if tmp['profit'] < 0:
        if tmp['hold'] == 1:
          realLossCounter += 1
        else:
          tempLossCounter += 1
      
      allProfit += tmp['profit']
      holdAlwaysProfit += tmp['beforeProfit']
      allHS300Profit += tmp['hs300Profit']
    out.append(tmp)
  
  out.sort(key=lambda x: x['profit'])
  out.reverse()
  print(
    "总数：{}, 交易数：{}, 持币亏损数：{}, 持股亏损数：{}, 战胜沪深300：{}, 战胜始终持股：{}, 总收益：{}, 沪深300收益：{}, 持续收益：{}, 所有持续收益：{}, 持续沪深300收益：{}".format(
      TOTAL, tradeCounter, realLossCounter, tempLossCounter, winHS300Number, winHoldAlwaysNumber, allProfit,
      allHS300Profit, holdAlwaysProfit, holdAlwaysAllProfit, holdAllTimeHS300))
  for one in out:
    print(one)

#########################################################
def Compare(one, two, codes):
  #比较两个策略的percent
  df1 = util.LoadData('stock_backtest', one)
  condition = {}
  if codes is not None:
    condition = {'_id': {'$in': codes}}
  df2 = util.LoadData('stock_backtest', two, condition)
  df1Tmp = df1.loc[:, ['name', 'percent']]
  df2Tmp = df2.loc[:, ['percent']]
  dfMerge = df2Tmp.join(df1Tmp, how='left', lsuffix='_two', rsuffix='_one')
  # frame['panduan'] = frame.city.apply(lambda x: 1 if 'ing' in x else 0)
  def win(x):
    if x.percent_two > x.percent_one:
      return 1
    elif x.percent_two < x.percent_one:
      return -1
    else:
      return 0
    
  def diff(x):
    return x.percent_two - x.percent_one
    
  dfMerge['win'] = dfMerge.apply(win, axis=1)
  dfMerge['diff'] = dfMerge.apply(diff, axis=1)
  # dfMerge['win'] = dfMerge.apply((lambda x: True if x.percent_two > x.percent else False), axis=1)
  
  print(dfMerge)
  dfMerge.to_excel("c:/workspace/tmp/1226.xlsx")



# 对传入的标的，测算最后行情是否可以执行买卖操作
# 可以执行买卖操作存db，如果最后一天没有买卖信号，但一个自然月内有信号，也存入
def SignalDV(codes):
  # 要求行情是最新的
  buyList = []
  sellList = []
  toDBList = []
  now = datetime.now()
  collectionName = now.strftime('%Y-%m-%d')
  # 只有比anchor ge的数据才需要写入db
  anchor = pd.Timestamp(datetime(now.year, now.month, 1))
  
  for one in codes:
    re = RunOne(one['_id'], 100000, one['name'], False, False)
    if len(re.listen.actionList):
      if re.listen.actionList[-1][1] == 1:
        buyList.append((re.code, re.name, re.listen.DumpOne(re.listen.actionList[-1])))
      elif re.listen.actionList[-1][1] == -1:
        sellList.append((re.code, re.name, re.listen.DumpOne(re.listen.actionList[-1])))
      toDBList.append(re.listen.ToDB())
  print('buy list##################')
  for one in buyList:
    print(one)
  
  print('sell list##################')
  for one in sellList:
    print(one)
  
  for one in toDBList:
    # TODO 这个和月初的比较有问题，如果这一个月股价涨幅很大，那其实信息已经没意义了
    if one[0] is not None and one[0] >= anchor:
      util.SaveMongoDB(one[1], 'stock_signal', collectionName)


def CalcDV(codes):
  for one in codes:
    stock = RunOne2(one['_id'], 100000, one['name'], {})
    percent = np.nan
    if stock.DV.statisticsYears is not None and stock.DV.statisticsYears > 0:
      percent = stock.DV.dividendYears / stock.DV.statisticsYears
    util.SaveMongoDB({'_id': one['_id'], 'name': one['name'], '统计年数': stock.DV.statisticsYears,
                      '分红年数': stock.DV.dividendYears, '百分比': percent},
                     'stock_statistcs', 'dvYears')


def CalcQuarterSpeed(codes, year):
  for one in codes:
    stock = RunOne2(one['_id'], 100000, one['name'], {})
    out = {'_id': stock.code, 'name': stock.name}
    if year in stock.DV.dv2Index.checkPoint:
      counter = 0
      total = 0
      dictData = ['first', 'second', 'third', 'forth']
      for quarter in dictData:
        if quarter in stock.DV.dv2Index.checkPoint[year]:
          try:
            out[quarter] = stock.DV.dv2Index.checkPoint[year][quarter]['sjltz']
            counter += 1
            total += stock.DV.dv2Index.checkPoint[year][quarter]['sjltz']
          except Exception as e:
            pass
      
      if counter > 0:
        out['avg'] = total / counter
      else:
        out['avg'] = np.nan
    
    util.SaveMongoDB(out, 'stock_statistcs', 'quarterSpeed')


# 对于已经持仓的股票，看看现价和买点的差距
def HoldDV(codes):
  # 要求行情是最新的
  for one in codes:
    re = RunOne(one['_id'], 100000, one['name'], False, False)
    if re.A.isHoldStock():
      out = {'_id': one['_id'], 'name': one['name']}
      out['triggerPrice'] = re.A.oldPrice
      out['lastPrice'] = re.context.price
      out['diff'] = (re.context.price - re.A.oldPrice) / re.A.oldPrice
      out['date'] = re.context.date
      
      util.SaveMongoDB(out, 'stock_hold', 'dv1')


#########################################################
if __name__ == '__main__':
  pass
  # TODO 周末研究下怎么画图，把入出点放在图形上，更直观，hs300，股价，收益曲线