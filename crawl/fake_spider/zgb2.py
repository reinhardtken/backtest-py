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
DB_NAME = TS.BASICS.DB_NAME
COLLECTION_NAME = TS.BASICS.COLLECTION_NAME

STOCK_LIST = setting.currentStockList()

jsonCallBack = 'jsonpCallback24417'

class Handler(spider.FakeSpider):
  crawl_config = {
  }

  def on_start(self):
    out = self.url()
    for one in out:
      save = {'key': one[1]}
      if one[1].startswith('6'):
        self.crawl(one[0], headers=self.shHeader(one[1]), callback=self.processFirstPage, save=save)
      else:
        self.crawl(one[0], headers=self.szHeader(one[1]), callback=self.processFirstPage, save=save)


  def szBaseURL(self):
    return 'http://www.szse.cn/api/report/index/companyGeneralization?'

  def shBaseURL(self):
    return 'http://query.sse.com.cn/security/stock/queryCompanyStockStruct.do?'

  def genSZParams(self, code):
    params = {
      'random': random.random(),
      'secCode': code,# = 000725
    }
    return params

  def genSHParams(self, code):
    params = {
      'jsonCallBack': jsonCallBack,
      'isPagination': 'false',
      'companyCode': code,
      '_': int(time.time()),
    }
    return params


  @property
  def stock_list(self):
    STOCK_LIST = setting.currentStockList()
    return STOCK_LIST

  def url(self):

    out = []
    for code in self.stock_list:
      base = self.shBaseURL() if code.startswith('6') else self.szBaseURL()
      params = self.genSHParams(code) if code.startswith('6') else self.genSZParams(code)
      url = encode_params(params)
      url = base + url
      out.append((url, code))

    return out

  def szHeader(self, code):
    headers = {
      'Host': 'www.szse.cn',
      'Referer': 'http://www.szse.cn/certificate/individual/index.html?code=' + code,
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
      'X-Request-Type': 'ajax',
      'X-Requested-With': 'XMLHttpRequest',
    }
    return headers

  def shHeader(self, code):
    headers = {
      'Host': 'query.sse.com.cn',
      'Referer': 'http://www.sse.com.cn/assortment/stock/list/info/capital/index.shtml?COMPANY_CODE=' + code,
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
      #'X-Request-Type': 'ajax',
      #'X-Requested-With': 'XMLHttpRequest',
    }
    return headers

  def saveDB(self, data: pd.DataFrame, key):
    def callback(result):
      self.send_message(self.project_name, result, key + '_' + result[MONGODB_ID])

    util.updateMongoDB(data, util.genKeyIDFunc(key), DB_NAME, COLLECTION_NAME, False, callback)

  def processFirstPage(self, response):
    if response.ok == False:
      return

    save = response.save
    data = response.content.decode('utf-8')

    if save['key'].startswith('6'):
      data = data[len(jsonCallBack)+1:-1]
      json_data = json.loads(data)
      result = self.parseSHPage([json_data['result'], ])
      self.saveDB(result, save['key'])
      pass
    else:
      json_data = json.loads(data)
      if json_data['code'] == '0':
        result = self.parseSZPage([json_data['data'], ])
        self.saveDB(result, save['key'])

  def parseSZPage(self, json):
    KEY_NAME = {
      'agzgb': 'A股总股本',
      'bgzgb': 'B股总股本',
      'zgb': TS.BASICS.KEY_NAME['zgb'],
    }
    NEED_TO_NUMBER = {
      'agzgb': 'A股总股本',
      'bgzgb': 'B股总股本',
    }
    NOTEAT = {
      'agzgb': None,
      'bgzgb': None,
    }
    DATA_SUB = {}
    try:
      tmp = []
      for item in json:
        one_stock = util.utils.dealwithData(item, util.utils.fourOP(DATA_SUB, NEED_TO_NUMBER,
                                                                    NOTEAT, KEY_NAME))

        one_stock[KEY_NAME['agzgb']] = one_stock[KEY_NAME['agzgb']] * 10000
        one_stock[KEY_NAME['bgzgb']] = one_stock[KEY_NAME['bgzgb']] * 10000
        re = {}
        re[KEY_NAME['zgb']] = one_stock[KEY_NAME['bgzgb']] + one_stock[KEY_NAME['agzgb']]
        series = pd.Series(re)
        tmp.append(series)

      df = pd.DataFrame(tmp)
      print(df)
      return df
    except Exception as e:
      print(e)


  def parseSHPage(self, json):
    KEY_NAME = {
      'totalShares': '总股本',
      'zgb': TS.BASICS.KEY_NAME['zgb'],
    }
    NEED_TO_NUMBER = {
      'totalShares': '总股本',
    }
    NOTEAT = {
      'totalShares': None,
    }
    DATA_SUB = {}
    try:
      tmp = []
      for item in json:
        one_stock = util.utils.dealwithData(item, util.utils.fourOP(DATA_SUB, NEED_TO_NUMBER,
                                                                    NOTEAT, KEY_NAME))
        re = {}
        re[KEY_NAME['zgb']] = one_stock[KEY_NAME['totalShares']] * 10000
        series = pd.Series(re)
        tmp.append(series)

      df = pd.DataFrame(tmp)
      print(df)
      return df
    except Exception as e:
      print(e)

  def on_message(self, project, msg):
    return msg



def run():
  gpfh = Handler()
  gpfh.on_start()
  gpfh.run()


if __name__ == '__main__':
  gpfh = Handler()
  gpfh.on_start()
  gpfh.run()
