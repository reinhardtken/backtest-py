#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-07-28 08:49:54
# Project: gpfh4

# sys
import json
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
import util.crawl as util
import const.crawl as const
from crawl.fake_spider import spider

MONGODB_ID = const.MONGODB_ID
ID_NAME = const.GPFH_KEYWORD.ID_NAME
DB_NAME = const.GPFH_KEYWORD.DB_NAME
COLLECTION_HEAD = const.GPFH_KEYWORD.COLLECTION_HEAD
KEY_NAME = const.GPFH_KEYWORD.KEY_NAME
NEED_TO_NUMBER = const.GPFH_KEYWORD.NEED_TO_NUMBER
DATA_SUB = const.GPFH_KEYWORD.DATA_SUB

KEY = 'var XbnsgnRv'
base_url = 'http://data.eastmoney.com/DataCenter_V3/yjfp/getlist.ashx?'
headers = {
  'Host': 'data.eastmoney.com',
  'Referer': 'http://data.eastmoney.com/yjfp/201712.html',
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
  # 'X-Requested-With': 'XMLHttpRequest',
}


#####################################################


class Handler(spider.FakeSpider):
  ALL = False
  crawl_config = {
  }

  def on_start(self):
    self.crawl(self.url(), headers=self.header(), callback=self.processFirstPage)

  class InnerTask():
    def __init__(self, date, getTotalNumber=False):
      self._date = date
      self.getTotalNumber = getTotalNumber

    def dump(self):
      return {'data': self._date, 'getTotalNumber': self.getTotalNumber}

    def load(dict):
      return Handler.InnerTask(dict['data'], dict['getTotalNumber'])

    def genParams(self, page, date):
      params = {
        'js': KEY,
        'pagesize': '50',
        'sr': '-1',
        'sortType': 'GQDJR',
        'mtk': u'全部股票'.encode('gb2312'),  # %C8%AB%B2%BF%B9%C9%C6%B1',
        'filter': '(ReportingPeriod=^' + date + '^)',  # '2017-12-31^)',
        'page': page,
        'rt': int(time.time()),
      }
      return params

    def genUrl(self, page):
      url = encode_params(self.genParams(page, self._date))
      return base_url + url


    def saveDB(self, data: pd.DataFrame, handler):
      def callback(result):
        handler.send_message(handler.project_name, result, self._date + '_' + result['_id'])

      re = util.saveMongoDB(data, util.genEmptyFunc(), DB_NAME, COLLECTION_HEAD + self._date, callback)
      util.everydayChange(re, 'gpfh')


  def url(self):
    return 'http://data.eastmoney.com/yjfp/201712.html'

  def header(self):
    headers = {
      'Host': 'data.eastmoney.com',
      'Referer': 'http://data.eastmoney.com/yjfp/201712.html',
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    }
    return headers

  def processFirstPage(self, response):
    if response.ok == False:
      return

    data_list = response.doc('#sel_bgq')
    # doc = pyquery.PyQuery(response)
    # data_list = doc('#sel_bgq')
    out = data_list.find('option')

    for one in out:
      print(one.text)
      year = float(one.text[:4])
      if not Handler.ALL and year > 2018:
        innerTask = Handler.InnerTask(one.text)
        save = innerTask.dump()
        self.crawl(innerTask.genUrl(1), headers=self.header(), callback=self.processSecondPage, save=save)
      elif Handler.ALL:
        innerTask = Handler.InnerTask(one.text)
        save = innerTask.dump()
        self.crawl(innerTask.genUrl(1), headers=self.header(), callback=self.processSecondPage, save=save)


  def processThirdPage(self, response):
    return self.processSecondPage(response)

  def processSecondPage(self, response):
    if response.ok == False:
      return

    content = response.content[13:]
    innerTask = Handler.InnerTask.load(response.save)
    try:
      data = content.decode('gb2312')
      json_data = json.loads(data)  # , encoding='GB2312')
      results = self.processDetailPage(json_data, innerTask)
      innerTask.saveDB(results, self)
    except UnicodeDecodeError as e:
      print(e)

  def processDetailPage(self, json, innerTask):
    if json:
      if json.get('success') != True:
        print('failed !!!!')
        return

      total = json.get('pages')
      if innerTask.getTotalNumber == False:
        innerTask.getTotalNumber = True
        if total >= 2:
          save = innerTask.dump()
          for i in range(2, total + 1):
            print(innerTask.genUrl(i))
            self.crawl(innerTask.genUrl(i), headers=self.header(), callback=self.processThirdPage, save=save)

      items = json.get('data')
      return self.parse_page(items)


  def parse_page(self, json):

    try:
      tmp = []
      for item in json:
        one_stock = util.dealwithData(item, util.threeOP(DATA_SUB, NEED_TO_NUMBER, KEY_NAME))
        one_stock[MONGODB_ID] = item.get(ID_NAME)
        series = pd.Series(one_stock)
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
