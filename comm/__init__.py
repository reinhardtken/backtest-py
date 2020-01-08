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
Message = const.Message

class Priority:
  def __init__(self, stage, priority):
    self.stage = stage
    self.priority = priority
    self.value = self.stage * 100000 + self.priority

  def __lt__(self, other):
    return self.value < other.value

class Task:
  def __init__(self, priority, key, util=None, *args, **kwargs):
    self.priority = priority
    self.key = key
    self.util = util
    self.args = args
    self.kwargs = kwargs
    #ugly
    #TODO 标识后续需要跳过的stage，此处唯一用途是cooldown的时候后续的买卖
    self.jump = None
  
  def __lt__(self, other):
    return self.priority < other.priority

#带优先级和阶段的的事件回调队列
#优先级，优先级高的事件会被先处理
#阶段，一个阶段处理完成后，loop会退出，再次进入才会处理第二个阶段
class Pump:
  def __init__(self, context):
    self.context = context
    self.taskPriorityQueue = PriorityQueue()
    
    self.handler = {}
    self.tmp = [] #临时存放task
    self.jump = None
    # self.currentStage = None
    # self.stagePriority = None
    pass
  
  
    
  def __getStageCallback(self, stage):
    if stage in self.stage:
      return self.stage[stage]
    else:
      return (None, None)
    
    
    
  def AddTask(self, task:Task):
    self.taskPriorityQueue.put_nowait(task)
  
  
  def AddHandler(self, key, handler):
    if isinstance(key, int):
      self.__AddHandlerInner(key, handler)
    elif isinstance(key, list):
      for one in key:
        self.__AddHandlerInner(one, handler)
  
  
  def __AddHandlerInner(self, key, handler):
    if key in self.handler:
      self.handler[key].append(handler)
    else:
      self.handler[key] = []
      self.handler[key].append(handler)


  # def LoopPrepare(self):
  #   self.stagePriority = PriorityQueue()
  #   for k, v in self.stage.items():
  #     self.stagePriority.put_nowait(k)
  
  
  def Loop(self, currentStage):
    # tmp = []
    if currentStage == Message.STAGE_STRATEGY:
      for one in self.tmp:
        if one.util is None:
          # del self.extra[k]
          pass
        elif self.context.date >= one.util:
          # del self.extra[k]
          pass
        else:
          self.taskPriorityQueue.put_nowait(one)
      self.tmp = []
    
    while self.taskPriorityQueue.qsize() != 0:
      task = self.taskPriorityQueue.get_nowait()
      if currentStage == task.priority.stage:
        self.tmp.append(task)
        if self.jump is not None and currentStage in self.jump:
          continue
          
        if task.key in self.handler:
          for handler in self.handler[task.key]:
            handler(self.context, task)
        
        if task.jump is not None:
          self.jump = task.jump
      else:
        self.taskPriorityQueue.put_nowait(task)
        break
    
    if currentStage == Message.STAGE_AFTER_TRADE:
      self.jump = None

#########################################################
#用于管理Pump，当所有的loop进出阶段的时候负责广播
class PumpManager:
  def __init__(self, loopSize, endDate):
    self.loopSize = loopSize
    self.endDate = endDate
    self.stageCallback = {}
    self.counter = {}
    
    
  # 添加阶段
  def AddStageCallback(self, stageValue, callback):
    if stageValue in self.stageCallback:
      pass
    else:
      self.stageCallback[stageValue] = []
    self.stageCallback[stageValue].append(callback)
    
    
  def Before(self, context):
    pass
    # 永久有效
    # context.AddTask(
    #   Task(
    #     Priority(
    #       Message.STAGE_STRATEGY, Message.PRIORITY_STAGE_STRATEGY_BEGIN),
    #     Message.STAGE_STRATEGY_BEGIN,
    #     pd.Timestamp(self.endDate)))
    # self.counter[Message.STAGE_STRATEGY_BEGIN] = 0
    # context.AddTask(
    #   Task(
    #     Priority(
    #       Message.STAGE_STRATEGY, Message.PRIORITY_STAGE_STRATEGY_END),
    #     Message.STAGE_STRATEGY_END,
    #     pd.Timestamp(self.endDate)))
    # self.counter[Message.STAGE_STRATEGY_END] = 0
    #
    # context.AddTask(
    #   Task(
    #     Priority(
    #       Message.STAGE_BEFORE_TRADE, Message.PRIORITY_STAGE_BEFORE_TRADE_BEGIN),
    #     Message.STAGE_BEFORE_TRADE_BEGIN,
    #     pd.Timestamp(self.endDate)))
    # self.counter[Message.STAGE_BEFORE_TRADE_BEGIN] = 0
    # context.AddTask(
    #   Task(
    #     Priority(
    #       Message.STAGE_BEFORE_TRADE, Message.PRIORITY_STAGE_BEFORE_TRADE_END),
    #     Message.STAGE_BEFORE_TRADE_END,
    #     pd.Timestamp(self.endDate)))
    # self.counter[Message.STAGE_BEFORE_TRADE_END] = 0
    # context.AddTask(
    #   Task(
    #     Priority(
    #       Message.STAGE_SELL_TRADE, Message.PRIORITY_STAGE_SELL_TRADE_BEGIN),
    #     Message.STAGE_SELL_TRADE_BEGIN,
    #     pd.Timestamp(self.endDate)))
    # self.counter[Message.STAGE_SELL_TRADE_BEGIN] = 0
    # context.AddTask(
    #   Task(
    #     Priority(
    #       Message.STAGE_SELL_TRADE, Message.PRIORITY_STAGE_SELL_TRADE_END),
    #     Message.STAGE_SELL_TRADE_END,
    #     pd.Timestamp(self.endDate)))
    # self.counter[Message.STAGE_SELL_TRADE_END] = 0
    # context.AddTask(
    #   Task(
    #     Priority(
    #       Message.STAGE_BUY_TRADE, Message.PRIORITY_STAGE_BUY_TRADE_BEGIN),
    #     Message.STAGE_BUY_TRADE_BEGIN,
    #     pd.Timestamp(self.endDate)))
    # self.counter[Message.STAGE_BUY_TRADE_BEGIN] = 0
    # context.AddTask(
    #   Task(
    #     Priority(
    #       Message.STAGE_BUY_TRADE, Message.PRIORITY_STAGE_BUY_TRADE_END),
    #     Message.STAGE_BUY_TRADE_END,
    #     pd.Timestamp(self.endDate)))
    # self.counter[Message.STAGE_BUY_TRADE_END] = 0
    # context.AddTask(
    #   Task(
    #     Priority(
    #       Message.STAGE_AFTER_TRADE, Message.PRIORITY_STAGE_AFTER_TRADE_BEGIN),
    #     Message.STAGE_AFTER_TRADE_BEGIN,
    #     pd.Timestamp(self.endDate)))
    # self.counter[Message.STAGE_AFTER_TRADE_BEGIN] = 0
    # context.AddTask(
    #   Task(
    #     Priority(
    #       Message.STAGE_AFTER_TRADE, Message.PRIORITY_STAGE_AFTER_TRADE_END),
    #     Message.STAGE_AFTER_TRADE_END,
    #     pd.Timestamp(self.endDate)))
    # self.counter[Message.STAGE_AFTER_TRADE_END] = 0
    
    
  def NotifyStageChange(self, stage, before):
    # if task.key in self.counter:
    #   self.counter[task.key] += 1
    #   if self.counter[task.key] == self.loopSize:
    #     self.counter[task.key] = 0
    if stage in self.stageCallback:
      for one in self.stageCallback[stage]:
        one(before)
          

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
            'hs300Profit': self.__hs300Profit, 'growth': growth, }
  
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
  
#########################################################
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
  


#最大值与最大回撤
class MaxAndRetracement:
  def __init__(self, v, date):
    self.M = MaxRecord()
    self.M.value = v
    self.M.date = date
    # 回撤相关
    self.R = Retracement()
  
  def Calc(self, newV, date):
    # 新高
    if newV > self.M.value:
      self.M.value = newV
      self.M.date = date
      old = self.R.Record(date)
      print('新高, 日期：{}, 净值：{}, 最近最大不创新高天数：{}'.format(date, self.M.value, old))


    # 回撤创新低
    if (self.M.value - newV) / self.M.value > self.R.current.value:
      self.R.current.value = (self.M.value - newV) / self.M.value
      if self.R.maxRetracementDaysLastCheck is None:
        self.R.maxRetracementDaysLastCheck = date
        self.R.current.begin = date
        # self.R.current.beginPrice = price
      else:
        diff = date - self.R.maxRetracementDaysLastCheck
        self.R.maxRetracementDaysLastCheck = date
        self.R.current.days += diff.days
      print('最大回撤, 日期：{}, 回撤：{}, 持续：{}'.format(date, self.R.current.value, self.R.current.days))
#########################################################
# 代表一次交易
class Move:
  def __init__(self, code, name, date, days, change, old, winLoss):
    self.code = code
    self.name = name
    self.date = date
    self.old = old
    self.change = change
    self.winLoss = winLoss
    self.days = int(days)
  
  def __str__(self):
    info = '### FundManager:Move {} {}, {} {}天, {:.2f}, {:.2f}, {:.2f}'.format(self.code, self.name,
                                                                               self.date, self.days, self.winLoss,
                                                                               self.old,
                                                                               self.change)
    return info
  
  
  def ToDict(self):
    return {'_id': self.code, 'name': self.name, 'date':self.date, 'days': self.days, 'winLoss':self.winLoss,
            'old': self.old, 'change': self.change}
  
  
  def __lt__(self, other):
    return self.winLoss < other.winLoss