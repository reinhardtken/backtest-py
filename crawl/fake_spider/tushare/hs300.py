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


#http://tushare.org/classifying.html#id8
# code :股票代码
# name :股票名称
# date :日期
# weight:权重



def getHS300():
  try:
    df = ts.get_hs300s()
    df.rename(columns=const.HS300.KEY_NAME, inplace=True)
    df.set_index(const.HS300.KEY_NAME['code'], inplace=True)
    return df
  except Exception as e:
    print(e)




def saveDB(data: pd.DataFrame, handler=None):
  def callback(result):
    # handler.send_message(handler.project_name, result, self._date + '_' + result['_id'])
    pass

  re = util.updateMongoDB(data, util.genKeyCodeFunc(const.HS300.KEY_NAME['code']), const.HS300.DB_NAME,
                          const.HS300.COLLECTION_NAME, True, callback)
  # util.everydayChange(re, 'gpfh')




if __name__ == '__main__':
  re = getHS300()
  saveDB(re)