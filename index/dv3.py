# -*- coding: utf-8 -*-

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

GPFH_KEY = const.GPFH_KEYWORD.KEY_NAME

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
# 统计整个财务数据中，-10%的出现次数，出现越多，说明卖点越多
class DVIndex:
  def __init__(self, code, name, startYear, startDate, endDate):
    self.code = code
    self.name = name
    self.startYear = startYear
    self.startDate = startDate
    self.endDate = endDate
    self.checkPoint = {}  # 所有的年报季报除权等影响买卖点的特殊时点
    self.forecast = {} #业绩预告
    self.dangerousPoint = []  # 利润同比下滑超过10%的位置
    self.dividendPoint = []  # 除权的日期
    
    self.MAXEND = util.Quater2Date(2099, 'first')  # 默认的冻结开仓截止日期
    
    self.dividendAdjust = {}  # 除权日，调整买入卖出价格
    
    self.statisticsYears = None  # 参与统计的总年数
    self.dividendYears = 0  # 有过分红的年数
  
  def Run(self):
    
    # 准备判定条件
    now = datetime.now()
    stopYear = now.year
    for year in range(self.startYear - 1, stopYear + 1):
      self.forecast[year] = {}
      if year not in self.checkPoint:
        self.checkPoint[year] = {}
      if year + 1 not in self.checkPoint:
        self.checkPoint[year + 1] = {}
      # 加载对应年的年报中报
      yearPaper = util.LoadYearPaper(year, self.code)
      self.checkPoint[year]['midYear'] = yearPaper[0]
      self.checkPoint[year]['year'] = yearPaper[1]
      
      # 加载对应的季报
      quarterPaper = util.LoadQuaterPaper(year, self.code)
      self.checkPoint[year]['first'] = quarterPaper[0]
      self.checkPoint[year]['second'] = quarterPaper[1]
      self.checkPoint[year]['third'] = quarterPaper[2]
      self.checkPoint[year]['forth'] = quarterPaper[3]
      
      #加载业绩预告
      forecast = util.LoadForecast(year, self.code)
      self.forecast[year]['first'] = forecast[0]
      self.forecast[year]['second'] = forecast[1]
      self.forecast[year]['third'] = forecast[2]
      self.forecast[year]['forth'] = forecast[3]
      
      
    
    self.statisticsYears = len(self.checkPoint)
    # 对加载出来的数据做初步处理
    for k, v in self.checkPoint.items():
      try:
        if 'notExist' in v['year'] and 'notExist' in v['midYear']:
          # 没有查到年报数据的年份，可能是公司就没上市，需要扣减
          self.statisticsYears -= 1
        # 计算分红
        v['midYear']['dividend'] = 0
        v['year']['dividend'] = 0
        self.CalcDividend(k, 'midYear')
        self.CalcDividend(k, 'year')
        v['year']['allDividend'] = v['midYear']['dividend'] + v['year']['dividend']
        # 计算股利支付率，如果这个数字超过100%，则分红不能当真
        try:
          if GPFH_KEY['EarningsPerShare'] in v['year']:
            v['year']['dividendRatio'] = v['year']['allDividend'] / v['year'][GPFH_KEY['EarningsPerShare']]
          
          # 计算分红是不是大于每股收益，如果大于每股收益，显然不可持续
          # 002351 漫步者 买入："date" : ISODate("2011-06-17T00:00:00.000Z"),
          # 不可持续的分红，需要调整为实际每股收益的80%来测算
          # 2019/12/26此段逻辑很好的改善了回测中差标的集合的表现
          if GPFH_KEY['EarningsPerShare'] in v['midYear']:
            if 0.8 * v['midYear'][GPFH_KEY['EarningsPerShare']] < v['midYear']['dividend']:
              v['unsustainable'] = True
              v['midYear']['dividend'] = 0.8 * v['midYear'][GPFH_KEY['EarningsPerShare']]
              v['year']['allDividend'] = v['midYear']['dividend'] + v['year']['dividend']
          if GPFH_KEY['EarningsPerShare'] in v['year']:
            if 0.8 * v['year'][GPFH_KEY['EarningsPerShare']] < v['year']['dividend']:
              v['unsustainable'] = True
              v['year']['dividend'] = 0.8 * v['year'][GPFH_KEY['EarningsPerShare']]
              v['year']['allDividend'] = v['midYear']['dividend'] + v['year']['dividend']
        except Exception as e:
          if v['year'][GPFH_KEY['EarningsPerShare']] == '-':
            v['year'][GPFH_KEY['EarningsPerShare']] = 0
          util.PrintException(e)
        # 根据分红计算全年和半年的买入卖出价格
        # allDividend影响下一年
        if v['year']['allDividend'] > 0:
          # 统计分红过的年数
          self.dividendYears += 1
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
            tmp['date'] = one.date
            self.checkPoint[one.year]['midYear']['dividendAdjust'] = tmp
          elif one.position == 'year' and self.checkPoint[one.year + 1]['buyPrice'] != INVALID_BUY_PRICE:
            # 送股調整的時候，年报的调整需要囊括半年报的送股
            # TODO 年报调整为何要包括半年报？感觉此处有问题。。。
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
            tmp['date'] = one.date
            self.checkPoint[one.year + 1]['year']['dividendAdjust'] = tmp
          if len(tmp) > 0:
            # 有配股有分红才调整价格
            self.dividendAdjust[one.date] = tmp
        except Exception as e:
          print(e)
    
    # 清理所有截止日期早于startDate的cooldown
    # Test2('601288', 26405, '农业银行', False, True) ，有2010年的cooldown
    tmp = []
    for one in self.dangerousPoint:
      if one[1] < self.startDate:
        continue
      else:
        tmp.append(one)
    self.dangerousPoint = tmp
  
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
  
  def ProcessQuarterPaper(self, year, position):
    if 'sjltz' in self.checkPoint[year][position]:
      speed = -11
      try:
        # 存在这个值为“-”的情形
        speed = float(self.checkPoint[year][position]['sjltz'])
        self.checkPoint[year][position]['sjltz'] = speed
      except Exception as e:
        pass
      
      if speed < -10:
        self.dangerousPoint.append((util.Quater2Date(year, position), self.MAXEND, year, position, speed))
      elif speed > 0:
        # 增速转正，如果之前有负的，要结合负的计算出冷冻区间（此区间不开仓）
        if len(self.dangerousPoint) > 0:
          # 找到所有没有填充终止的条目，全部填上当前时间点
          dirtyFlag = False
          for index in range(len(self.dangerousPoint)):
            if self.dangerousPoint[index][1] == self.MAXEND:
              dirtyFlag = True
              self.dangerousPoint[index] = (
                self.dangerousPoint[index][0], util.Quater2Date(year, position), self.dangerousPoint[index][2],
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