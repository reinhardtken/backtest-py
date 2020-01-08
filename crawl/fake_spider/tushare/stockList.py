# -*- encoding: utf-8 -*-

# sys
import json
# thirdpart
import pandas as pd
import tushare as ts

# this project

##########################
import util.crawl as util
import const.crawl as const


#http://tushare.org/fundamental.html#id2
# code,代码
# name,名称
# industry,所属行业
# area,地区
# pe,市盈率
# outstanding,流通股本(亿)
# totals,总股本(亿)
# totalAssets,总资产(万)
# liquidAssets,流动资产
# fixedAssets,固定资产
# reserved,公积金
# reservedPerShare,每股公积金
# esp,每股收益
# bvps,每股净资
# pb,市净率
# timeToMarket,上市日期
# undp,未分利润
# perundp, 每股未分配
# rev,收入同比(%)
# profit,利润同比(%)
# gpr,毛利率(%)
# npr,净利润率(%)
# holders,股东人数
def getBasics():
  try:
    df = ts.get_stock_basics()
    df.rename(columns=const.BASICS.KEY_NAME, inplace=True)
    return df
  except Exception as e:
    print(e)




def saveDB(data: pd.DataFrame, handler=None):
  def callback(result):
    # handler.send_message(handler.project_name, result, self._date + '_' + result['_id'])
    pass

  re = util.updateMongoDB(data, util.genKeyCodeFunc(const.BASICS.KEY_NAME['code']),
                          const.BASICS.DB_NAME, const.BASICS.COLLECTION_NAME, True, callback)
  # util.everydayChange(re, 'gpfh')




if __name__ == '__main__':
  re = getBasics()
  saveDB(re)