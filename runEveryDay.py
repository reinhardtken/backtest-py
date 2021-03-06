#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from pymongo import MongoClient
import pandas as pd

import setting
import tools
from filter import ipoYear
from filter import dvYearAll
from filter import hs300All


def queryAllCode():
  from pymongo import MongoClient
  client = MongoClient()
  db = client['stock']
  collection = db['stock_list']
  out = []
  cursor = collection.find()
  for c in cursor:
    out.append(c["_id"])
  
  return out

def Run():
  import crawl.fake_spider.yjbg
  import crawl.fake_spider.yjyg
  import crawl.fake_spider.gpfh
  import crawl.fake_spider.tushare.kData
  import crawl.fake_spider.tushare.hs300
  import crawl.fake_spider.tushare.stockList
  
  # 更新沪深300的K线
  # crawl.fake_spider.tushare.kData.RunHS300IndexRecent()
  # #获取要更新股票的不复权K线
  # codes = queryAllCode()
  
  df = pd.read_excel(setting.CONFIG.EVERYDAY_STOCKLIST, dtype=str)
  codes = df.to_dict('records')
  # 更新k线数据
  # for code in codes:
  #   try:
  #     print('process {} {}############################################'.format(code['_id'], code['name']))
  #     re = crawl.fake_spider.tushare.kData.getKDataNoneRecent(code['_id'])
  #     crawl.fake_spider.tushare.kData.saveDB3(re, code['_id'])
  #   except Exception as e:
  #     print(e)
  
  # 更新最新回测结果
  filter = [
    dvYearAll.Filter,
    # dvYear.Filter,
    hs300All.Filter,
    # hs300.Filter,
    ipoYear.Filter,
  ]
  
  tools.DoBacktest(codes,
                   {'check': False, 'backtest': True, 'saveDB': 'all_dv3', 'saveFile': setting.CONFIG.SAVE_PATH,
                    'saveSignal': 'stock_signal_dv3'}, filter)


#########################################################
###这个文件每次运行爬取十五天k线，每周每天执行都行
if __name__ == '__main__':
  Run()




