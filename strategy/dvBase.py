# -*- coding: utf-8 -*-

# sys
from datetime import datetime
from dateutil import parser

# thirdpart
import pandas as pd
from pymongo import MongoClient
import numpy as np

import const
import util

# this project
if __name__ == '__main__':
  import sys

# https://www.cnblogs.com/nxf-rabbit75/p/11111825.html

VERSION = '1.0.0.17.5'


DIR_BUY = 1
DIR_NONE = 0
DIR_SELL = -1
HOLD_MONEY = 1
HOLD_STOCK = 2
BUY_PERCENT = 0.04
SELL_PERCENT = 0.03
INVALID_SELL_PRICE = 10000
INVALID_BUY_PRICE = -10000
YEAR_POSITION = 2
MIDYEAR_POSITION = 1


#交易日当天的信息
class DayInfo:
  def __init__(self):
    self.year = None
    self.date = None
    self.index = None
    self.price = None
    self.pb = None
    self.priceVec = []
    #5日平均线等等，都放在这里
    
#update DayInfo
# for buyChain
# for sellChain
# action
class TradeUnit:
  #沪深300，如果跑多个实例无需重复load数据
  DF_HS300 = None
  # 代表一个交易单元，比如10万本金
  def __init__(self, code, name, beginMoney):
    self.tradeList = []  # 交易记录
    self.status = HOLD_MONEY  # 持仓还是持币
    # self.money = 100000 #持币的时候，这个表示金额，持仓的的时候表示不够建仓的资金
    self.BEGIN_MONEY = beginMoney
    self.money = self.BEGIN_MONEY
    self.oldMoney = self.money
    self.costPrice = 0  # 持仓的时候，表示持仓成本
    self.number = 0  # 持仓的时候表示持仓数目(手)
    
    self.startYear = 2011  # 起始年份
    start = str(self.startYear) + '-01-01T00:00:00Z'
    self.startDate = parser.parse(start, ignoretz=True)
    self.endYear = 2011  # 结束年份
    end = str(self.endYear) + '-12-10T00:00:00Z'
    self.endDate = parser.parse(end, ignoretz=True)
    
    
    self.checkPoint = {}  # 所有的年报季报除权等影响买卖点的特殊时点
    self.code = code  # 代码
    self.name = name
    self.mongoClient = MongoClient()
    self.dangerousPoint = []  # 利润同比下滑超过10%的位置
    self.dividendPoint = []  # 除权的日期
    self.data = None  # 行情
    self.MAXEND = self.Quater2Date(2099, 'first')  # 默认的冻结开仓截止日期
    self.holdStockDate = 0  # 持股总交易天数
    self.holdStockNatureDate = 0  # 持股总自然天数
    # self.lastDate = None  # 最后回测的交易日
    # self.lastPrice = 0  # 最后回测的交易价格
    self.result = None  # 最后的交易结果
    self.sellPrice = None  # 卖出价格
    self.dividendAdjust = {}  # 除权日，调整买入卖出价格
    self.lastPriceVec = []  # 最后5天价格，用于调试
    self.index = None  # 沪深300指数
    self.indexProfit = 1  # 沪深300指数收益
    self.indexBuyPoint = None  # 股票开仓时候沪深300指数点位
    # self.lastIndex = None
    self.collectionName = 'dv1'  # 存盘表名
    self.beforeProfit = None  # 前复权行情，用于计算从头到尾持有股票的收益
    self.tradeCounter = 0  # 交易的次数，建仓次数
    self.current = DayInfo()  #当前循环的全部信息
    
    # 特殊年报时间
    self.ALL_SPECIAL_PAPER = {}
    self.specialPaper = {}
    
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
        0: pd.Timestamp(datetime(2015, 3, 25)),
        # 1: pd.Timestamp(datetime(2015, 8, 20))
      },
    }
    
    if self.code in self.ALL_SPECIAL_PAPER:
      self.specialPaper = self.ALL_SPECIAL_PAPER[self.code]
  
  def buyInner(self, price, money):
    # 计算多少钱买多少股，返回股数，钱数
    number = money // (price * 100)
    restMoney = money - number * 100 * price
    return (number, restMoney)
  
  def Buy(self, date, triggerPrice, sellPrice, price, indexPrice, where, reason=''):
    try:
      if self.status == HOLD_STOCK:
        self.holdStockDate += 1
        # 如果已经持股，需要根据历年股息调整卖出价格
        if sellPrice != INVALID_SELL_PRICE:
          self.sellPrice = sellPrice

      # 如果持币，以固定价格买入
      if price <= triggerPrice:
        if self.status == HOLD_MONEY:
          # 卖出的价格是在建仓的时候决定的
          self.sellPrice = sellPrice
          self.oldMoney = self.money
          number, money = self.buyInner(price, self.money)
          self.number = number
          self.money = money
          # self.number = self.money // (price * 100)
          # self.money = self.money - self.number*100*price
          self.costPrice = price
          self.status = HOLD_STOCK

          # 记录指数变化
          self.indexBuyPoint = indexPrice
          #交易次数+1
          self.tradeCounter += 1
          
          # 记录
          mark = TradeMark()
          mark.reason('买入').date(date).dir(DIR_BUY).total(self.number * 100 * price).number(self.number).price(
            price).where(where).extraInfo('{}'.format(reason)).Dump()
          self.tradeList.append(mark)
          # print("买入： 日期：{}, 触发价格：{}, 价格：{}, 数量：{}, 剩余资金：{}, 原因：{}".format(date, triggerPrice, price, self.number, self.money, reason))
        elif self.status == HOLD_STOCK:
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
  
  def Sell(self, date, sellPrice, price, indexPrice, where, reason=''):
    if self.status == HOLD_STOCK:
      if price >= self.sellPrice:
        self.SellNoCodition(date, price, indexPrice, reason)
      elif sellPrice == INVALID_SELL_PRICE and where.find('-year') != -1:
        # 在由年报决定买卖的情况下，如果卖出价格是INVALID_SELL_PRICE
        # 说明当年没有分红，那就必须无条件卖出了
        self.SellNoCodition(date, price, indexPrice, '年报未分红')
  
  def SellNoCodition(self, date, price, indexPrice, reason=''):
    if self.status == HOLD_STOCK:
      self.money = self.money + self.number * 100 * price
      winLoss = (self.money - self.oldMoney) / self.oldMoney
      self.status = HOLD_MONEY
      # 计算指数收益
      self.indexProfit *= indexPrice / self.indexBuyPoint
      
      # 记录
      mark = TradeMark()
      mark.reason('卖出').date(date).dir(DIR_SELL).total(self.number * 100 * price).number(self.number).price(
        price).extraInfo(
        '盈亏：{}, 原因：{}'.format(winLoss, reason)).Dump()
      self.tradeList.append(mark)
      # print(mark)
      # print("卖出： 日期：{}, 触发价格：{}, 价格：{}, 数量：{}, 盈亏：{}, 现金：{}, 原因：{}".format(date, triggerPrice, price,
      #                                                                      self.number, winLoss, self.money, reason))
  
  def CheckPrepare(self):
    # 准备判定条件
    now = datetime.now()
    stopYear = now.year
    for year in range(self.startYear - 1, stopYear + 1):
      if year not in self.checkPoint:
        self.checkPoint[year] = {}
      if year + 1 not in self.checkPoint:
        self.checkPoint[year + 1] = {}
      # 加载对应年的年报中报
      yearPaper = self.LoadYearPaper(year)
      self.checkPoint[year]['midYear'] = yearPaper[0]
      self.checkPoint[year]['year'] = yearPaper[1]

      # 加载对应的季报
      quarterPaper = self.LoadQuaterPaper(year)
      self.checkPoint[year]['first'] = quarterPaper[0]
      self.checkPoint[year]['second'] = quarterPaper[1]
      self.checkPoint[year]['third'] = quarterPaper[2]
      self.checkPoint[year]['forth'] = quarterPaper[3]

    # 对加载出来的数据做初步处理
    for k, v in self.checkPoint.items():
      try:
        # 计算分红
        v['midYear']['dividend'] = 0
        v['year']['dividend'] = 0
        self.CalcDividend(k, 'midYear')
        self.CalcDividend(k, 'year')
        v['year']['allDividend'] = v['midYear']['dividend'] + v['year']['dividend']

        # 根据分红计算全年和半年的买入卖出价格
        # TODO 如果发生了派股，买入和卖出价格需要根据派股做出调整
        # TODO 如果发生派股，派股的第二天买入卖出价格要变
        # allDividend影响下一年
        if v['year']['allDividend'] > 0:
          self.checkPoint[k + 1]['buyPrice'] = v['year']['allDividend'] / BUY_PERCENT
          self.checkPoint[k + 1]['sellPrice'] = v['year']['allDividend'] / SELL_PERCENT
        else:
          self.checkPoint[k + 1]['buyPrice'] = INVALID_BUY_PRICE
          self.checkPoint[k + 1]['sellPrice'] = INVALID_SELL_PRICE
        
        # midYear的dividend影响8月31号以后的当年
        if v['midYear']['dividend'] > 0:
          self.checkPoint[k]['buyPrice2'] = v['midYear']['dividend'] / BUY_PERCENT
          self.checkPoint[k]['sellPrice2'] = v['midYear']['dividend'] / SELL_PERCENT
        else:
          self.checkPoint[k]['buyPrice2'] = INVALID_BUY_PRICE
          self.checkPoint[k]['sellPrice2'] = INVALID_SELL_PRICE
        
        # 检查净利润同比下降10%以上的位置
        self.ProcessQuarterPaper(k, 'first')
        self.ProcessQuarterPaper(k, 'second')
        self.ProcessQuarterPaper(k, 'third')
        self.ProcessQuarterPaper(k, 'forth')
      except KeyError as e:
        print(e)

    # 针对dangerousPoint清理dividendPoint
    tmp = []
    have = set()
    for one in self.dividendPoint:
      for two in self.dangerousPoint:
        if two[0] <= one.date and two[1] >= one.date:
          print("清理除权 {} {} {}".format(one.date, two[0], two[1]))
          break
      else:
        if one.date not in have:
          tmp.append(one)
          have.add(one.date)
    self.dividendPoint = tmp
    
    # 对所有派股的情况，记录除权日，并调整买卖价格
    for one in self.dividendPoint:
      tmp = {}
      # 有派股
      if one.gift > 0:
        try:
          if one.position == 'midYear' and self.checkPoint[one.year]['buyPrice2'] != INVALID_BUY_PRICE:
            tmp['buyPriceX'] = self.checkPoint[one.year]['buyPrice2'] / (1 + one.gift)
            tmp['sellPriceX'] = self.checkPoint[one.year]['sellPrice2'] / (1 + one.gift)
            tmp['y'] = one.year
            tmp['p'] = 'midYear'
          elif one.position == 'year' and self.checkPoint[one.year + 1]['buyPrice'] != INVALID_BUY_PRICE:
            # 送股調整的時候，年报的调整需要囊括半年报的送股
            midYearGift = 0
            try:
              midYearGift = self.checkPoint[one.year]['midYear']['gift']
            except Exception as e:
              pass
            if midYearGift != 0:
              assert 0
            tmp['buyPriceX'] = self.checkPoint[one.year + 1]['buyPrice'] / ((1 + one.gift) * (1 + midYearGift))
            tmp['sellPriceX'] = self.checkPoint[one.year + 1]['sellPrice'] / ((1 + one.gift) * (1 + midYearGift))
            tmp['y'] = one.year + 1
            tmp['p'] = 'year'
          if len(tmp) > 0:
            # 有配股有分红才调整价格
            self.dividendAdjust[one.date] = tmp
        except Exception as e:
          print(e)
  
  def LoadYearPaper(self, y):
    # 加载年报，中报
    midYear = {}
    year = {}
    db = self.mongoClient["stock"]
    collection = db["gpfh-" + str(y) + "-06-30"]
    cursor = collection.find({"_id": self.code})
    for c in cursor:
      midYear[const.GPFH_KEYWORD.KEY_NAME['CQCXR']] = c[const.GPFH_KEYWORD.KEY_NAME['CQCXR']]
      midYear[const.GPFH_KEYWORD.KEY_NAME['AllocationPlan']] = c[const.GPFH_KEYWORD.KEY_NAME['AllocationPlan']]
      break
    
    collection = db["gpfh-" + str(y) + "-12-31"]
    cursor = collection.find({"_id": self.code})
    for c in cursor:
      year[const.GPFH_KEYWORD.KEY_NAME['CQCXR']] = c[const.GPFH_KEYWORD.KEY_NAME['CQCXR']]
      year[const.GPFH_KEYWORD.KEY_NAME['AllocationPlan']] = c[const.GPFH_KEYWORD.KEY_NAME['AllocationPlan']]
      break
    
    return (midYear, year)
  
  def LoadQuaterPaper(self, year):
    # 加载季报
    first = {}
    second = {}
    third = {}
    forth = {}
    db = self.mongoClient["stock"]
    collection = db["yjbg-" + self.code]
    strYear = str(year)
    # 一季度
    cursor = collection.find({"_id": strYear + "-03-31"})
    for c in cursor:
      first['sjltz'] = c['sjltz']
      break
    
    # 二季度
    cursor = collection.find({"_id": strYear + "-06-30"})
    for c in cursor:
      second['sjltz'] = c['sjltz']
      break
    
    # 三季度
    cursor = collection.find({"_id": strYear + "-09-30"})
    for c in cursor:
      third['sjltz'] = c['sjltz']
      break
    
    # 四季度
    cursor = collection.find({"_id": strYear + "-12-31"})
    for c in cursor:
      forth['sjltz'] = c['sjltz']
      break
    
    return (first, second, third, forth)
  
  def processSong(self, value):
    # 处理送股
    index = value.find('送')
    index2 = value.find('转')
    if index != -1:
      newValue = value[index + 1:]
      out = util.String2Number(newValue)
      return out
    elif index2 != -1:
      newValue = value[index2 + 1:]
      out = util.String2Number(newValue)
      return out
    else:
      return 0
  
  def processPai(self, value):
    # 处理派息
    index = value.find('派')
    if index != -1:
      newValue = value[index + 1:]
      out = util.String2Number(newValue)
      return out
    else:
      return 0
  
  def processPai2(self, value):
    # 处理派息，税后
    index = value.find('扣税后')
    if index != -1:
      newValue = value[index + 1:]
      out = util.String2Number(newValue)
      return out
    else:
      return 0
  
  def CalcDividend(self, year, position):
    # '10送3.00派4.00元(含税,扣税后3.30元)'
    if const.GPFH_KEYWORD.KEY_NAME['AllocationPlan'] in self.checkPoint[year][position]:
      value = self.checkPoint[year][position][const.GPFH_KEYWORD.KEY_NAME['AllocationPlan']]
      number = util.String2Number(value)
      profit = self.processPai(value)
      self.checkPoint[year][position]['dividend'] = profit / number
      profit2 = self.processPai2(value)
      self.checkPoint[year][position]['dividend_aftertax'] = profit2 / number
      gift = self.processSong(value)
      self.checkPoint[year][position]['gift'] = gift / number
      
      if const.GPFH_KEYWORD.KEY_NAME['CQCXR'] in self.checkPoint[year][position]:
        tmpDate = self.checkPoint[year][position][const.GPFH_KEYWORD.KEY_NAME['CQCXR']]
        # 农业银行，2010年
        if tmpDate == '-':
          if position == 'midYear':
            # 半年默认在当年年底除权
            tmpDate = pd.Timestamp(datetime(year, 12, 1))
          else:
            tmpDate = pd.Timestamp(datetime(year + 1, 6, 30))
        else:
          tmpDate = pd.to_datetime(np.datetime64(tmpDate))
        self.dividendPoint.append(DividendPoint(
          tmpDate,
          self.checkPoint[year][position]['dividend'],
          self.checkPoint[year][position]['dividend_aftertax'],
          self.checkPoint[year][position]['gift'],
          year, position))
  
  def Quater2Date(self, year, quarter):
    # 从某个季度，转换到具体日期
    if quarter == 'first':
      return pd.to_datetime(np.datetime64(str(year) + '-04-30T00:00:00Z'))
    elif quarter == 'second':
      return pd.to_datetime(np.datetime64(str(year) + '-08-31T00:00:00Z'))
    elif quarter == 'third':
      return pd.to_datetime(np.datetime64(str(year) + '-10-31T00:00:00Z'))
    elif quarter == 'forth':
      # 来年一季度,这里反正有问题，用29号变通下
      return pd.to_datetime(np.datetime64(str(year + 1) + '-04-29T00:00:00Z'))
  
  def ProcessQuarterPaper(self, year, position):
    if 'sjltz' in self.checkPoint[year][position]:
      speed = -11
      try:
        # 存在这个值为“-”的情形
        speed = float(self.checkPoint[year][position]['sjltz'])
      except Exception as e:
        pass
      
      if speed < -10:
        self.dangerousPoint.append((self.Quater2Date(year, position), self.MAXEND, year, position, speed))
      elif speed > 0:
        # 增速转正，如果之前有负的，要结合负的计算出冷冻区间（此区间不开仓）
        if len(self.dangerousPoint) > 0:
          # 找到所有没有填充终止的条目，全部填上当前时间点
          dirtyFlag = False
          for index in range(len(self.dangerousPoint)):
            if self.dangerousPoint[index][1] == self.MAXEND:
              dirtyFlag = True
              self.dangerousPoint[index] = (
              self.dangerousPoint[index][0], self.Quater2Date(year, position), self.dangerousPoint[index][2],
              self.dangerousPoint[index][3], self.dangerousPoint[index][4])

          # 清理在起始时间小于终止时间的区间（比如从2011年2季度开始，2,3,4季度都是负，2012年一季度转正）
          # 此时dangerousPoint里面有多个条目：（2011-2，2012-1），（2011-3，2012-1），（2011-4，2012-1）
          # 其中（2011-3，2012-1），（2011-4，2012-1）是没有意义的，需要删除
          if dirtyFlag:
            tmp = []
            have = set()
            for index in range(len(self.dangerousPoint)):
              if self.dangerousPoint[index][1] not in have:
                have.add(self.dangerousPoint[index][1])
                tmp.append(self.dangerousPoint[index])
              else:
                pass
            self.dangerousPoint = tmp

  def loadData(self, dbName, collectionName, condition):
    db = self.mongoClient[dbName]
    collection = db[collectionName]
    cursor = collection.find(condition)
    out = []
    for c in cursor:
      out.append(c)
  
    if len(out):
      df = pd.DataFrame(out)
      df.drop('date', axis=1, inplace=True)
      df.set_index('_id', inplace=True)
      return df
    
    return None


  def loadPB(self):
    db = self.mongoClient['stock2']
    collection = db['pb2']
    cursor = collection.find({'_id': self.code})
    out = None
    for c in cursor:
      out = c['data']
      break
  
    for one in out:
      one['date'] = pd.Timestamp(datetime.strptime(one['date'], '%Y-%m-%d'))
      
    if len(out):
      df = pd.DataFrame(out)
      df.drop('timestamp', axis=1, inplace=True)
      df.drop('price', axis=1, inplace=True)
      df.set_index('date', inplace=True)
      return df
  
    return None
  
  def LoadQuotations(self):
    self.data = self.loadData("stock_all_kdata_none", self.code, {"_id": {"$gte": self.startDate}})
    #加载前复权数据，计算区间股价涨幅
    try:
      pass
      # beforeData = self.loadData("stock_all_kdata", self.code, {"_id": {"$gte": datetime1}})
      # startPrice = beforeData.iloc[0]['close']
      # endPrice = beforeData.iloc[-1]['close']
      # self.beforeProfit = (endPrice - startPrice)/startPrice
    except Exception as e:
      print(e)

  
  def LoadIndexs(self):
    if TradeUnit.DF_HS300 is None:
      TradeUnit.DF_HS300 = self.loadData("stock_all_kdata_none", '000300', {"_id": {"$gte": self.startDate}})
    self.index = TradeUnit.DF_HS300

  
  
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
      self.mergeData.fillna(method='ffill', inplace=True)  # 用前面的值来填充
  
  
  def ProcessDividend(self, date, buyPrice, price, indexPrice, dividend):
    if self.status == HOLD_STOCK:
      if buyPrice >= price:
        oldMoney = self.money
        oldNumber = self.number
        dividendMoney = self.number * 100 * dividend.dividendAfterTax
        money = dividendMoney + self.money

        # 如果有转送股，所有的价格，股数需要变动
        if dividend.gift > 0:
          self.number *= 1 + dividend.gift
          oldSellPrice = self.sellPrice
          self.sellPrice /= 1 + dividend.gift
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
        # print(mark)
        # print("除权买入： 日期：{}, 除权日：{}, 触发价格：{}, 价格：{}, 数量：{}, 剩余资金：{}, 分红：{}".format(date, dividend[0], buyPrice, price, self.number, self.money,
        #                                                                   dividendMoney))
      else:
        oldMoney = self.money
        dividendMoney = self.number * 100 * dividend.dividendAfterTax
        self.money += dividendMoney
        
        # 如果有转送股，所有的价格，股数需要变动
        if dividend.gift > 0:
          self.number *= 1 + dividend.gift
          oldSellPrice = self.sellPrice
          self.sellPrice /= 1 + dividend.gift
          # price /= 1 + dividend.gift
          print('发生转送股2 {} {} {}'.format(self.number, oldSellPrice, self.sellPrice))
        
        # 记录
        mark = TradeMark()
        mark.reason('除权不买入').date(date).dir(DIR_NONE).total(dividendMoney).number(0).price(0).extraInfo(
          '分红金额：{}'.format(dividendMoney)).Dump()
        self.tradeList.append(mark)
        # print(mark)
        # print("除权不买入： 日期：{}, 除权日：{}, 触发价格：{}, 价格：{}, 数量：{}, 剩余资金：{}, 分红：{}".format(date, dividend[0], buyPrice, price, self.number, self.money,
        #                                                               dividendMoney))
  
  def MakeDecisionPrice(self, date):
    # 决定使用哪个年的checkpoit，返回对应的buy和sell
    
    anchor0 = pd.Timestamp(datetime(date.year, 4, 30))
    anchor1 = pd.Timestamp(datetime(date.year, 8, 31))
    # 特殊年报调整
    if date.year in self.specialPaper:
      if 0 in self.specialPaper[date.year]:
        anchor0 = self.specialPaper[date.year][0]
      if 1 in self.specialPaper[date.year]:
        anchor1 = self.specialPaper[date.year][1]
    
    where = None
    try:
      if date <= anchor0:
        # 在4月30日之前，只能使用去年的半年报，如果半年报没有，则无法交易
        where = str(date.year) + '-midYear'
        if self.checkPoint[date.year]['buyPrice2'] > 0:
          return True, self.checkPoint[date.year - 1]['buyPrice2'], self.checkPoint[date.year - 1]['sellPrice2'], where
      elif date <= anchor1:
        # 在8月31日之前，需要使用去年的年报
        where = str(date.year - 1) + '-year'
        if self.checkPoint[date.year]['buyPrice'] > 0:
          return True, self.checkPoint[date.year]['buyPrice'], self.checkPoint[date.year]['sellPrice'], where
      else:
        # 在8月31日之后，使用去年的allDividend和今年半年报中dividend中大的那个决定
        buy = self.checkPoint[date.year]['buyPrice']
        midBuy = self.checkPoint[date.year]['buyPrice2']
        if buy > midBuy and buy > 0:
          return True, buy, self.checkPoint[date.year]['sellPrice'], str(date.year - 1) + '-year2'
        else:
          if midBuy > 0:
            return True, midBuy, self.checkPoint[date.year]['sellPrice2'], str(date.year) + '-midYear2'
          else:
            return False, INVALID_BUY_PRICE, INVALID_SELL_PRICE, str(date.year - 1) + '-year3'
    except Exception as e:
      pass
    
    return False, INVALID_BUY_PRICE, INVALID_SELL_PRICE, where
  
  def ProcessDividendAdjust(self, data):
    if data['p'] == 'midYear':
      self.checkPoint[data['y']]['buyPrice2'] = data['buyPriceX']
      self.checkPoint[data['y']]['sellPrice2'] = data['sellPriceX']
    elif data['p'] == 'year':
      self.checkPoint[data['y']]['buyPrice'] = data['buyPriceX']
      self.checkPoint[data['y']]['sellPrice'] = data['sellPriceX']
  
  def BackTest(self):
    # 1 ver
    # self.backTestInner(self.data)
    # 2 ver
    self.backTestInner(self.mergeData)
  
  def backTestInner(self, backtestData):
    # 回测
    print('BackTest {} {}'.format(self.code, self.BEGIN_MONEY))
    cooldown = False
    cooldownEnd = None
    current = self.current
    for date, row in backtestData.iterrows():
      try:
        if cooldown:
          if date >= cooldownEnd:
            cooldown = False
            print('cooldownend {}'.format(cooldownEnd))
            cooldownEnd = None
          else:
            continue

        # midYear, year = self.MakeDecisionYear(date)
        current.year = date.year
        current.date = date
        current.price = row['close']
        current.index = row['close_index']
        # pb = row['pb']
        if len(current.priceVec) > 5:
          current.priceVec = current.priceVec[1:]
        current.priceVec.append((date, current.price, current.index))
        
        # 处理除权日时，需要调整买入卖出价格
        if date in self.dividendAdjust:
          self.ProcessDividendAdjust(self.dividendAdjust[date])

        # 根据具体日期，决定使用的年报位置
        action, buyPrice, sellPrice, where = self.MakeDecisionPrice(date)
        
        
        # 处理除权,
        # 除权日不可能不是交易日
        if len(self.dividendPoint) > 0:
          if date > self.dividendPoint[0].date:
            # 比如600900,在除权期间停牌。。。，对于这种目前简化为弹出这些除权，避免影响后续
            # TODO 如何处理停牌？
            while len(self.dividendPoint) > 0 and date > self.dividendPoint[0].date:
              print(' 弹出除权信息！！！ {}， {}'.format(self.lastPriceVec, self.dividendPoint[0]))
              self.dividendPoint = self.dividendPoint[1:]
          elif len(self.dividendPoint) > 0 and date == self.dividendPoint[0].date:
            self.ProcessDividend(date, buyPrice, current.price, current.index, self.dividendPoint[0])
            self.dividendPoint = self.dividendPoint[1:]

        
        if action:
          self.Buy(date, buyPrice, sellPrice, current.price, current.index, where, reason='低于买点')
        
        self.Sell(date, sellPrice, current.price, current.index, where, reason='高于卖点')
        
        
        
        # 处理季报，检查是否扣非-10%
        if len(self.dangerousPoint) > 0 and date >= self.dangerousPoint[0][0]:
          self.SellNoCodition(date, current.price, current.index, reason='扣非卖出: {}'.format(self.dangerousPoint[0][4]))
          # 记录因为扣非为负的区间，在区间内屏蔽开仓
          print('cooldownbegin {}'.format(self.dangerousPoint[0][0]))
          cooldown = True
          cooldownEnd = self.dangerousPoint[0][1]
          self.dangerousPoint = self.dangerousPoint[1:]
      
      
      
      except TypeError as e:
        print(e)
      except KeyError as e:
        print(e)
  
  def CloseAccount(self):
    money = 0
    if self.status == HOLD_STOCK:
      money = self.money + self.number * 100 * self.current.price
      # 计算指数收益
      self.indexProfit *= self.current.index / self.indexBuyPoint
    else:
      money = self.money
    
    one = TradeResult()
    one.beginDate(pd.Timestamp(self.startDate)).endDate(self.current.date).status(self.status).beginMoney(
      self.BEGIN_MONEY). \
      total(money).days(self.holdStockDate).hs300Profit(self.indexProfit - 1)
    one.Calc()
    self.result = one
    one.Dump()
    
  
  def Store2DB(self):
    # 保存交易记录到db，用于回测验证
    out = {"_id": self.code, 'ver': VERSION, 'name': self.name}
    out["beginMoney"] = self.BEGIN_MONEY
    tl = []
    for one in self.tradeList:
      tl.append(one.ToDB())
    out['tradeMarks'] = tl
    out.update(self.result.ToDB())
    out['beforeProfit'] = self.beforeProfit
    out['tradeCounter'] = self.tradeCounter
    util.SaveMongoDB(out, 'stock_backtest', self.collectionName)
  
  def ExistCheckResult(self):
    db = self.mongoClient["stock_backtest"]
    collection = db['dv1']
    cursor = collection.find({"_id": self.code})
    out = None
    for c in cursor:
      out = c
      break
    
    if out is not None:
      return True
    else:
      return False
  
  def CheckResult(self):
    db = self.mongoClient["stock_backtest"]
    collection = db['dv1']
    cursor = collection.find({"_id": self.code})
    out = None
    for c in cursor:
      out = c
      break
    
    where = 0
    if self.BEGIN_MONEY == out['beginMoney']:
      if len(self.tradeList) == len(out['tradeMarks']):
        if self.result == TradeResult.FromDB(out):
          for index in range(len(self.tradeList)):
            if self.tradeList[index] != TradeMark.FromDB(out['tradeMarks'][index]):
              where += 1
              return False
          else:
            return True
        else:
          where = 3
      else:
        where = 2
    else:
      where = 1
    return False


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
class DividendPoint:
  def __init__(self, date, dividend, dividendAfterTax, gift, year, postion):
    self.__date = date
    self.__dividend = dividend
    self.__dividendAfterTax = dividendAfterTax
    self.__gift = gift  # 送转股数量
    self.__year = year  # 影响哪个位置分红的买卖股价
    self.__position = postion  # year还是midYear
    # self.__profit = profit
  
  @property
  def date(self):
    return self.__date
  
  @property
  def dividend(self):
    return self.__dividend
  
  @property
  def dividendAfterTax(self):
    return self.__dividendAfterTax
  
  @property
  def gift(self):
    return self.__gift
  
  @property
  def year(self):
    return self.__year
  
  @property
  def position(self):
    return self.__position
  
  # @property
  # def profit(self):
  #   return self.__profit
  
  def __str__(self):
    return "日期：{}, 派息：{}, 派息税后：{}, 送股：{}, 年份：{}, 位置：{}, ".format(
      self.date, self.dividend, self.dividendAfterTax, self.gift, self.year, self.position)


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
    return {'beginDate': self.__beginDate, 'endDate': self.__endDate, 'status': self.__status,
            'beginMoney': self.__beginMoney, 'total': self.__total,
            'profit': self.__profit, 'percent': self.__percent, 'days': self.__days, 'hs300Profit': self.__hs300Profit}
  
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
def RunOne(code, beginMoney, name, save=False, check=False):
  stock = TradeUnit(code, name, beginMoney)
  stock.LoadQuotations()
  stock.LoadIndexs()
  stock.Merge()
  stock.CheckPrepare()
  
  print(stock.checkPoint)
  print(stock.dangerousPoint)
  for one in stock.dividendPoint:
    print(one)
  
  stock.BackTest()
  stock.CloseAccount()
  if save:
    stock.Store2DB()
  
  if check:
    assert stock.CheckResult()
  print(stock.checkPoint)
  print(stock.dangerousPoint)
  print(stock.dividendPoint)


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
  #跑赢沪深300的数量
  winHS300Number = 0
  #跑赢持续持股的数量
  winHoldAlwaysNumber = 0
  #沪深300总收益
  allHS300Profit = 0
  #策略总收益
  allProfit = 0
  #持续持股总收益，选股，交易过样本
  holdAlwaysProfit = 0
  #不选股，300总样本
  holdAlwaysAllProfit = 0
  #沪深300持续持有收益
  holdAllTimeHS300 = 0
  #发生过交易的股票数目
  tradeCounter = 0
  #回测结束时候，是持币的股票。如果持币，亏损属于真实亏损
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
  print("总数：{}, 交易数：{}, 持币亏损数：{}, 持股亏损数：{}, 战胜沪深300：{}, 战胜始终持股：{}, 总收益：{}, 沪深300收益：{}, 持续收益：{}, 所有持续收益：{}, 持续沪深300收益：{}".format(
    TOTAL, tradeCounter, realLossCounter, tempLossCounter, winHS300Number, winHoldAlwaysNumber, allProfit, allHS300Profit, holdAlwaysProfit, holdAlwaysAllProfit, holdAllTimeHS300))
  for one in out:
    print(one)


if __name__ == '__main__':
  CODE_AND_MONEY = [
    # # # 皖通高速
    #   {'code': '600012', 'money': 52105},
    # # # 万华化学
    #   {'code': '600309', 'money': 146005},
    # # # 北京银行
    #   {'code': '601169', 'money': 88305},
    # # # 大秦铁路
    # # # 2015年4月27日那天，预警价为14.33元，但收盘价只有14.32元，我们按照收盘价计算，
    # # # 差一分钱才触发卖出规则。如果当时卖出，可收回现金14.32*12500+330=179330元。
    # # # 错过这次卖出机会后不久，牛市见顶，股价狂泻，从14元多一直跌到5.98元。
    # # # TODO 需要牛市清盘卖出策略辅助
    #   {'code': '601006', 'money': 84305},
    # 南京银行
    # {'code': '601009', 'money': 75005},
    # 东风股份
    # {'code': '601515', 'money': 133705},
    # 福耀玻璃
    # {'code': '600660', 'money': 125905},
    # 光大银行
    # {'code': '601818', 'money': 29105},
    # 浦发银行
    # {'code': '600000', 'money': 74505},
    # 重庆水务
    # {'code': '601158', 'money': 58105},
    # 中国建筑
    # {'code': '601668', 'money': 29605},
    # 永新股份
    # {'code': '002014', 'money': 124305},
    # 万科
    # {'code': '000002', 'money': 72705},
    # 华域汽车
    # {'code': '600741', 'money': 92005},
    # 宇通客车
    # 再看历史PB情况，在2016年4月，宇通客车的PB位于3.84，中位值3.96，很显然，
    # 不具有很强的估值吸引力。也就是说，如果考虑估值因素，我们将不会买入。
    # TODO 买点看pb？
    # {'code': '600066', 'money': 203305},
    # 宝钢股份
    # TODO 买点看pb？
    # {'code': '600019', 'money': 70705},
    # 荣盛发展
    # {'code': '002146', 'money': 99205},
    # 厦门空港
    # {'code': '600897', 'money': 242805},
    # 金地集团
    # {'code': '600383', 'money': 103205},
    # 海螺水泥
    # {'code': '600585', 'money': 295905},
    # 长江电力
    # {'code': '600900', 'money': 63905},
    # 承德露露
    # 可以看到，虽然承德露露目前属于高息股票，但从2011到2016年，它的分红一直不达标
    # TODO 需要持续分红剔除没有做到持续分红的标的
    # {'code': '000848', 'money': 97405},
    # 联发股份
    # TODO 这是一个股息率算法的典型例子，因为它每年分红两次（中报、年报），且常有送转股
    # 缺少db.getCollection('gpfh-2015-12-31').find({"_id" : "002394"}) 数据
    # {'code': '002394', 'money': 149005},
    # 粤高速A
    # {'code': '000429', 'money': 75105},
    # 招商银行
    # {'code': '600036', 'money': 101505},
    # 鲁泰A
    # {'code': '000726', 'money': 93505},
  ]
  
  VERIFY_CODES = [
    # # # 皖通高速
    {'name': '皖通高速', 'code': '600012', 'money': 52105},
    # # # 万华化学
    {'name': '万华化学', 'code': '600309', 'money': 146005},
    # # # 北京银行
    {'name': '北京银行', 'code': '601169', 'money': 88305},
    # # # 大秦铁路
    # # # 2015年4月27日那天，预警价为14.33元，但收盘价只有14.32元，我们按照收盘价计算，
    # # # 差一分钱才触发卖出规则。如果当时卖出，可收回现金14.32*12500+330=179330元。
    # # # 错过这次卖出机会后不久，牛市见顶，股价狂泻，从14元多一直跌到5.98元。
    # # # TODO 需要牛市清盘卖出策略辅助
    {'name': '大秦铁路', 'code': '601006', 'money': 84305},
    # 南京银行
    {'name': '南京银行', 'code': '601009', 'money': 75005},
    # 东风股份
    {'name': '东风股份', 'code': '601515', 'money': 133705},
    # 福耀玻璃
    {'name': '福耀玻璃', 'code': '600660', 'money': 125905},
    # 光大银行
    {'name': '光大银行', 'code': '601818', 'money': 29105},
    # 浦发银行
    {'name': '浦发银行', 'code': '600000', 'money': 74505},
    # 重庆水务
    {'name': '重庆水务', 'code': '601158', 'money': 58105},
    # 中国建筑
    {'name': '中国建筑', 'code': '601668', 'money': 29605},
    # 永新股份
    {'name': '永新股份', 'code': '002014', 'money': 124305},
    # 万科
    {'name': '万科', 'code': '000002', 'money': 72705},
    # 华域汽车
    {'name': '华域汽车', 'code': '600741', 'money': 92005},
    # 宇通客车
    # 再看历史PB情况，在2016年4月，宇通客车的PB位于3.84，中位值3.96，很显然，
    # 不具有很强的估值吸引力。也就是说，如果考虑估值因素，我们将不会买入。
    # TODO 买点看pb？
    {'name': '宇通客车', 'code': '600066', 'money': 203305},
    # 宝钢股份
    # TODO 买点看pb？
    {'name': '宝钢股份', 'code': '600019', 'money': 70705},
    # 荣盛发展
    {'name': '荣盛发展', 'code': '002146', 'money': 99205},
    # 厦门空港
    {'name': '厦门空港', 'code': '600897', 'money': 242805},
    # 金地集团
    {'name': '金地集团', 'code': '600383', 'money': 103205},
    # 海螺水泥
    {'name': '海螺水泥', 'code': '600585', 'money': 295905},
    # 长江电力
    {'name': '长江电力', 'code': '600900', 'money': 63905},
    # 承德露露
    # 可以看到，虽然承德露露目前属于高息股票，但从2011到2016年，它的分红一直不达标
    # TODO 需要持续分红剔除没有做到持续分红的标的
    {'name': '承德露露', 'code': '000848', 'money': 97405},
    # 粤高速A
    {'name': '粤高速A', 'code': '000429', 'money': 75105},
    # 招商银行
    {'name': '招商银行', 'code': '600036', 'money': 101505},
    # 鲁泰A
    {'name': '鲁泰A', 'code': '000726', 'money': 93505},
    
    {'name': '中国石化', 'code': '600028', 'money': 74405},
    {'name': '双汇发展', 'code': '000895', 'money': 211205},
    {'name': '伟星股份', 'code': '002003', 'money': 80805},
    {'name': '兴业银行', 'code': '601166', 'money': 90205},
    {'name': '交通银行', 'code': '601328', 'money': 46905},
    {'name': '方大特钢', 'code': '600507', 'money': 167905},
    # TODO 增加pb指标
    {'name': '中国神华', 'code': '601088', 'money': 219705},
    {'name': '新城控股', 'code': '601155', 'money': 102805},
    {'name': '农业银行', 'code': '601288', 'money': 26405},
    # 2017年全年无分红，2018年出年报就该卖出
    {'name': '格力电器', 'code': '000651', 'money': 296305},
  
  ]
  
  # for one in VERIFY_CODES:
  #   stock = TradeUnit(one['code'], one['money'])
  #   if not stock.ExistCheckResult():
  #     print(stock.code)
  
  # Test2('000726', 93505, '鲁泰A', True, False)
  # test
  # TestAll(CODE_AND_MONEY, True, False)
  # save
  # TestAll(VERIFY_CODES, True, False)
  # check
  TestAll(VERIFY_CODES, False, True)
  # compare
  # CompareAll(VERIFY_CODES)
  # TODO 周末研究下怎么画图，把入出点放在图形上，更直观，hs300，股价，收益曲线