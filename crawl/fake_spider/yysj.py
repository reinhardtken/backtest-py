#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-07-28 04:28:42
# Project: gpfh


# sys
import json
import random
import time
# thirdpart
import pandas as pd
from requests.models import RequestEncodingMixin

encode_params = RequestEncodingMixin._encode_params

# this project
if __name__ == '__main__':
  import sys

  sys.path.append('/home/ken/workspace/code/self/github/py-code/new_stock')
##########################
import util
import util.utils
import const
import const.TS as TS
import setting
from fake_spider import spider




MONGODB_ID = const.MONGODB_ID
DB_NAME = const.YYSJ_KEYWORD.DB_NAME
COLLECTION_HEAD = const.YYSJ_KEYWORD.COLLECTION_HEAD
ID_NAME = const.YYSJ_KEYWORD.ID_NAME
STOCK_LIST = setting.f_data_stocklist()
# STOCK_LIST = ['000636']




class Handler:
  crawl_config = {
  }

  def on_start(self):
    pass



  def saveDB(self, data: pd.DataFrame, key):
    def callback(result):
      self.send_message(self.project_name, result, key + '_' + result[MONGODB_ID])

    util.saveMongoDB(data, util.genEmptyFunc(), DB_NAME, COLLECTION_HEAD + key, callback)
    # util.updateMongoDB(data, util.genKeyIDFunc(key), DB_NAME, COLLECTION_NAME, False, callback)


  def run(self):
    for code in STOCK_LIST:
      try:
        url = 'http://data.eastmoney.com/bbsj/yysj/' + code +'.html'
        df = pd.read_html(url)[1]
        df.rename(columns={'截止日期': ID_NAME, }, inplace=True)
        df.set_index(ID_NAME, inplace=True)
        # df[MONGODB_ID] = df.index.values
        print(df)
        self.saveDB(df, code)
      except Exception as e:
        print(e)




  def saveDB(self, data: pd.DataFrame, key):
    util.saveMongoDB(data, util.genKeyDateFunc(MONGODB_ID), DB_NAME, COLLECTION_HEAD + key, None)


def run():
  gpfh = Handler()
  gpfh.on_start()
  gpfh.run()


if __name__ == '__main__':
  gpfh = Handler()
  gpfh.on_start()
  gpfh.run()
