#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from pymongo import MongoClient
import setting
import util




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

#########################################################
###这个文件新环境爬取一次，以后每个季度更新一次即可
if __name__ == '__main__':
  import crawl.fake_spider.yjbg
  import crawl.fake_spider.yjyg
  import crawl.fake_spider.gpfh
  import crawl.fake_spider.tushare.kData
  import crawl.fake_spider.tushare.hs300
  import crawl.fake_spider.tushare.stockList
  import tools
  
  #获取沪深300标的的基本信息
  crawl.fake_spider.tushare.hs300.saveDB(crawl.fake_spider.tushare.hs300.getHS300())
  #获取全部股票的基本信息
  crawl.fake_spider.tushare.stockList.saveDB(crawl.fake_spider.tushare.stockList.getBasics())
  #获取沪深300的K线
  crawl.fake_spider.tushare.kData.RunHS300Index()
  #获取全部股票的不复权K线
  codes = queryAllCode()
  # k线数据
  index = 0
  for code in codes:
    try:
      index += 1
      print('process {} ############################################'.format(code))
      re = crawl.fake_spider.tushare.kData.getKDataNone(code)
      crawl.fake_spider.tushare.kData.saveDB3(re, code)
    except Exception as e:
      print(e)

  #获取全部股票的季报增速
  try:
    crawl.fake_spider.yjbg.Handler.STOCK_LIST = codes
    crawl.fake_spider.yjbg.run()
  except Exception as e:
    print(e)

  #获取全部股票的年报分红
  try:
    crawl.fake_spider.gpfh.Handler.ALL = True
    crawl.fake_spider.gpfh.run()
  except Exception as e:
    print(e)

  # # 获取全部股票的业绩预告
  # try:
  #   crawl.fake_spider.yjyg.Handler.ALL = True
  #   crawl.fake_spider.yjyg.run()
  # except Exception as e:
  #   print(e)
    
    
  #计算全部股票的累计分红
  # tools.CalcDV({'_id': '603987', 'name': '康德莱'})
  tools.CalcDVAll()
  
  #历史所有沪深300股票入库
  # tools.AllHS300Code2DB(setting.PATH.ALLHS300_STOCKLIST)
  
  #跑全部股票
  out = util.QueryAllCode()
  tools.DoAction(out, util.BackTestFactory({'check': False, 'backtest': True, 'saveDB': 'all_dv3', }))
  
  



