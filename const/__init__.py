# -*- coding: utf-8 -*-


class Message:
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
  # 统计
  OTHER_WORK = 10
  
  SUGGEST_BUY_EVENT = 11
  
  NEW_DAY = 12
  
  PRICE_DIFF = 13
  
  
  # STAGE_STRATEGY_BEGIN = 1000
  # STAGE_STRATEGY_END = 1001
  # STAGE_BEFORE_TRADE_BEGIN = 1002
  # STAGE_BEFORE_TRADE_END = 1003
  # STAGE_SELL_TRADE_BEGIN = 1004
  # STAGE_SELL_TRADE_END = 1005
  # STAGE_FUND_MANAGE_BEGIN = 1006
  # STAGE_FUND_MANAGE_END = 1007
  # STAGE_BUY_TRADE_BEGIN = 1008
  # STAGE_BUY_TRADE_END = 1009
  # STAGE_AFTER_TRADE_BEGIN = 1010
  # STAGE_AFTER_TRADE_END = 1011
  #######################################################
  # priority##############################
  # 其实策略和账户响应根本不会在一起，没必要设置先后
  STAGE_STRATEGY = 1  # 策略在这里广播
  STAGE_BEFORE_TRADE = 2  # 除权调整价格
  STAGE_SELL_TRADE = 3  # 前交易阶段，所有的卖
  STAGE_FUND_MANAGE = 4  # 资金管理
  STAGE_BUY_TRADE = 5  # 所有的买
  STAGE_AFTER_TRADE = 6  # 所有交易后
  STAGE_INVALID = 7
  STAGE_VALUE = 100000  # 一个阶段可以包含的值
  #######################################################
  # 阶段
  # PRIORITY_STAGE_STRATEGY_BEGIN = 0
  # PRIORITY_STAGE_STRATEGY_END = STAGE_VALUE - 1
  # PRIORITY_STAGE_BEFORE_TRADE_BEGIN = 0
  # PRIORITY_STAGE_BEFORE_TRADE_END = STAGE_VALUE - 1
  # PRIORITY_STAGE_SELL_TRADE_BEGIN = 0
  # PRIORITY_STAGE_SELL_TRADE_END = STAGE_VALUE - 1
  # PRIORITY_STAGE_FUND_MANAGE_BEGIN = 0
  # PRIORITY_STAGE_FUND_MANAGE_END = STAGE_VALUE - 1
  # PRIORITY_STAGE_BUY_TRADE_BEGIN = 0
  # PRIORITY_STAGE_BUY_TRADE_END = STAGE_VALUE - 1
  # PRIORITY_STAGE_AFTER_TRADE_BEGIN = 0
  # PRIORITY_STAGE_AFTER_TRADE_END = STAGE_VALUE - 1
  
  # PRIORITY_JUMP = 1  # 跳过循环，这种必须有util字段
  PRIORITY_NEW_DAY = 1
  PRIORITY_DIVIDEND_ADJUST = 50  # 除权引发价格调整
  
  PRIORITY_MAKEDECISION = 900
  
  PRIORITY_BEFORE_DIVIDEND = 1100  # 发生在除权前
  PRIORITY_DIVIDEND = 1200  # 除权
  PRIORITY_COOLDOWN = 1300  # 扣非净利润-10%，卖出
  ########################
  # PRIORITY_STAGE_ONE = 1200  # 买卖前
  # PRIORITY_TRADE_TAG = 1300  #
  PRIORITY_SELL = 1400  #
  PRIORITY_SUGGEST_BUY = 1500
  PRIORITY_BUY = 1600  #
  PRIORITY_AFTER_TRADE = 1700  #


class DV2:
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



class GPFH_KEYWORD:
  ID_NAME = 'Code'
  DB_NAME = 'stock'
  COLLECTION_HEAD = 'gpfh-'
  KEY_NAME = {
    'Code': '代码',
    'Name': '名称',
    'XJFH': '现金分红',
    'GXL': '股息率',
    'EarningsPerShare': '每股收益(元)',
    'NetAssetsPerShare': '每股净资产(元)',
    'MGGJJ': '每股公积金(元)',
    'MGWFPLY': '每股未分配利润(元)',
    'JLYTBZZ': '净利润同比增长(%)',
    'TotalEquity': '总股本(亿）',
    'YAGGR': '预案公告日',
    'GQDJR': '股权登记日',
    'CQCXR': '除权除息日',
    'ProjectProgress': '方案进度',
    'NoticeDate': '最新公告日期',
    'SZZBL': '送转总比例',
    'SGBL': '送股比例',
    'ZGBL': '转股比例',
    'AllocationPlan': '分配方案',

  }
  
  
class HS300:
  KEY_NAME = {
    'code': '股票代码',
    'name': '股票名称',
    'date': '日期',
    'weight': '权重',
  }
  DB_NAME = 'stock'
  COLLECTION_NAME = 'hs300_stock_list'
  