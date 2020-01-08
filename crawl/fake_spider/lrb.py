#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-07-30 05:28:16
# Project: yjyg_2018


# sys
import json
import time
# thirdpart
import pandas as pd
from requests.models import RequestEncodingMixin
encode_params = RequestEncodingMixin._encode_params
#from HTMLParser import HTMLParser

# this project
if __name__ == '__main__':
  import sys

  sys.path.append('/home/ken/workspace/code/self/github/py-code/new_stock')
##########################
import util
import util.utils
import const
from fake_spider import spider
import setting


MONGODB_ID = const.MONGODB_ID
ID_NAME = const.LRB_KEYWORD.ID_NAME
DB_NAME = const.LRB_KEYWORD.DB_NAME
COLLECTION_HEAD = const.LRB_KEYWORD.COLLECTION_HEAD
KEY_NAME = const.LRB_KEYWORD.KEY_NAME
NEED_TO_NUMBER = const.LRB_KEYWORD.NEED_TO_NUMBER
DATA_SUB = const.LRB_KEYWORD.DATA_SUB
NEED_TO_DECODE = const.LRB_KEYWORD.NEED_TO_DECODE
STOCK_LIST = setting.f_data_stocklist()
# STOCK_LIST = ['000636']

KEY = 'var XbnsgnRv'


base_url = 'http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get?'
headers = {
  'Host': 'dcfm.eastmoney.com',
  'Referer': 'http://data.eastmoney.com/bbsj/lrb/000636.html',
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
}



class Handler(spider.FakeSpider):
  crawl_config = {
  }

  def on_start(self):
    out = self.url()
    for one in out:
      save = {'key': one[1]}
      self.crawl(one[0], headers=self.header(), callback=self.processFirstPage, save=save)

  def genParams(self, code):
    # http: // dcfm.eastmoney.com // em_mutisvcexpandinterface / api / js / get?
    # type = CWBB_LRB20
    # & token = 70f12f2f4f091e459a279469fe49eca5
    # & filter = (scode=000636)
    # & st = reportdate
    # & sr = -1
    # & p = 2
    # & ps = 50
    # & js = var % 20
    # OSDpZdWf = {pages: (tp), data: % 20(x), font:(font)}
    # & rt = 51881549
    # var OSDpZdWf =

    params = {
      'type': 'CWBB_LRB20',
      'token': '70f12f2f4f091e459a279469fe49eca5',
      'st': 'reportdate',
      'sr': '-1',
      # 虽然可能存在p多于1页的情况，但是目前先不考虑这种情况
      'p': '1',
      'ps': '50',
      'js': 'var aUDOBatW={pages:(tp),data: (x),font:(font)}',
      'filter': '(scode=' + code + ')',
      'rt': int(time.time()),
    }
    return params



  def url(self):
    out = []
    for code in STOCK_LIST:
      url = encode_params(self.genParams(code))
      url = base_url + url
      out.append((url, code))

    return out


  def header(self):
    headers = {
      'Host': 'dcfm.eastmoney.com',
      'Referer': 'http://data.eastmoney.com/bbsj/yjbb/000636.html',
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    }
    return headers


  def saveDB(self, data: pd.DataFrame, key):
    def callback(result):
      self.send_message(self.project_name, result, key + '_' + result[MONGODB_ID])

    util.saveMongoDB(data, util.genEmptyFunc(), DB_NAME, COLLECTION_HEAD + key, callback)


  def processFirstPage(self, response):
    if response.ok == False:
      return

    content = response.content[13:]
    print(response.url)
    save = response.save
    code = save['key']
    try:
      data = content.decode('utf-8')
      print(data)
      # data2 = HTMLParser().unescape(data)
      data = data.replace('pages:', '"pages":', 1)
      data = data.replace('data:', '"data":', 1)
      data = data.replace('font:', '"font":', 1)
      json_data = json.loads(data)  # , encoding='GB2312')
      results = self.processDetailPage(json_data, code)
      self.saveDB(results, code)
    except UnicodeDecodeError as e:
      print(e)
    except Exception as e:
      print(e)



  def processDetailPage(self, json, code):
    if json:
      items = json.get('data')
      mapping = json.get('font')['FontMapping']
      return self.parse_page(items, mapping)



  def parse_page(self, json, mapping):

    try:
      tmp = []
      for item in json:
        for k, v in NEED_TO_DECODE.items():
          item[k] = util.utils.yjyg_unescape(mapping, item[k])



        one_stock = util.utils.dealwithData(item, util.utils.threeOP(DATA_SUB,
                                                                     NEED_TO_NUMBER, KEY_NAME))

        one_stock[MONGODB_ID] = one_stock[ID_NAME]


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
