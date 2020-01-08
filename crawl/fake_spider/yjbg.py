#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-07-30 05:28:16
# Project: yjyg_2018


# sys
import json
import time
# thirdpart
import pandas as pd
import numpy as np
from requests.models import RequestEncodingMixin
encode_params = RequestEncodingMixin._encode_params
#from HTMLParser import HTMLParser

# this project

##########################
import util.crawl as util
import const.crawl as const
from crawl.fake_spider import spider
try:
  import setting
except Exception as e:
  print(e)

MONGODB_ID = const.MONGODB_ID
ID_NAME = const.YJBG_KEYWORD.ID_NAME
DB_NAME = const.YJBG_KEYWORD.DB_NAME
COLLECTION_HEAD = const.YJBG_KEYWORD.COLLECTION_HEAD
KEY_NAME = const.YJBG_KEYWORD.KEY_NAME
NEED_TO_NUMBER = const.YJBG_KEYWORD.NEED_TO_NUMBER
DATA_SUB = const.YJBG_KEYWORD.DATA_SUB
# STOCK_LIST = setting.f_data_stocklist()
# STOCK_LIST = ['000636']

KEY = 'var XbnsgnRv'
"""
http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get?
type=YJBB21_YJBB
&token=70f12f2f4f091e459a279469fe49eca5
&filter=(scode=000636)
&st=reportdate
&sr=-1
&p=2
&ps=50
&js=var%20OSDpZdWf={pages:(tp),data:%20(x),font:(font)}
&rt=51881549
"""

base_url = 'http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get?'
headers = {
  'Host': 'dcfm.eastmoney.com',
  'Referer': 'http://data.eastmoney.com/bbsj/201812/yjyg.html',
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
  # 'X-Requested-With': 'XMLHttpRequest',
}



class Handler(spider.FakeSpider):
  crawl_config = {
  }
  STOCK_LIST = None
  def on_start(self):
    out = self.url()
    for one in out:
      save = {'key': one[1]}
      self.crawl(one[0], headers=self.header(), callback=self.processFirstPage, save=save)

  def genParams(self, code):
    # http: // dcfm.eastmoney.com // em_mutisvcexpandinterface / api / js / get?
    # type = YJBB21_YJBB
    # & token = 70
    # f12f2f4f091e459a279469fe49eca5
    # & filter = (scode=000636)
    # & st = reportdate
    # & sr = -1
    # & p = 2
    # & ps = 50
    # & js = var % 20
    # OSDpZdWf = {pages: (tp), data: % 20(x), font:(font)}
    # & rt = 51881549
    # var OSDpZdWf =
    # {pages: 2, data: [
    # {"scode": "000636",
    # "sname": "风华高科",
    # "securitytype": "A股",
    # "trademarket": "深交所主板",
    # "latestnoticedate": "2007-10-30T00:00:00",
    # "reportdate": "2006-09-30T00:00:00",
    # "publishname": "电子元件",
    # "securitytypecode": "058001001",
    # "trademarketcode": "069001002001",
    # "firstnoticedate": "2007-10-30T00:00:00",
    # "basiceps": "&#xE712;.&#xE712;&#xE4E5;&#xECD9;",
    # "cutbasiceps": "-",
    # "totaloperatereve": "&#xF275;&#xEBC0;&#xE268;&#xE268;&#xEBC0;&#xEBC0;&#xF2FF;&#xF3C3;&#xF4CD;&#xE0D4;.&#xF275;&#xF4CD;",
    # "ystz": "&#xE0D4;&#xE0D4;.&#xEBC0;&#xEBC0;&#xF2FF;&#xF2FF;",
    # "yshz": "&#xF3C3;.&#xE0D4;&#xF4CD;&#xE268;&#xF4CD;",
    # "parentnetprofit": "&#xF2FF;&#xE712;&#xF3C3;&#xF2FF;&#xF3C3;&#xE0D4;&#xE4E5;&#xECD9;.&#xE0D4;&#xF4CD;",
    # "sjltz": "&#xEBC0;&#xE4E5;&#xECD9;.&#xECD9;&#xE0D4;&#xEBC0;&#xE268;",
    # "sjlhz": "-&#xF3C3;&#xECD9;.&#xECD9;&#xE4E5;&#xE0D4;&#xF3C3;",
    # "roeweighted": "-",
    # "bps": "&#xECD9;.&#xE712;&#xE4E5;&#xECD9;",
    # "mgjyxjje": "&#xE712;.&#xF275;&#xF2FF;&#xECD9;&#xF4CD;&#xE712;&#xEBC0;",
    # "xsmll": "&#xF275;&#xF2FF;.&#xF4CD;&#xE268;&#xECD9;&#xE0D4;",
    # "assigndscrpt": "-",
    # "gxl": "-"}
    # ],
    #             font: {"WoffUrl": "http://data.eastmoney.com/font/2eb/2ebeaf102cde4374a146ca760dbd5e30.woff",
    #                    "EotUrl": "http://data.eastmoney.com/font/2eb/2ebeaf102cde4374a146ca760dbd5e30.eot",
    #                    "FontMapping": [{"code": "&#xF275;", "value": 1}, {"code": "&#xEBC0;", "value": 2},
    #                                    {"code": "&#xECD9;", "value": 3}, {"code": "&#xE0D4;", "value": 4},
    #                                    {"code": "&#xF2FF;", "value": 5}, {"code": "&#xF3C3;", "value": 6},
    #                                    {"code": "&#xE4E5;", "value": 7}, {"code": "&#xF4CD;", "value": 8},
    #                                    {"code": "&#xE268;", "value": 9}, {"code": "&#xE712;", "value": 0}]}}
    params = {
      'type': 'YJBB21_YJBB',
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
    STOCK_LIST = None
    if Handler.STOCK_LIST is not None:
      STOCK_LIST = Handler.STOCK_LIST
    else:
      STOCK_LIST = setting.f_data_stocklist()
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
        item['roeweighted'] = util.yjyg_unescape(mapping, item['roeweighted'])
        item['totaloperatereve'] = util.yjyg_unescape(mapping, item['totaloperatereve'])
        item['xsmll'] = util.yjyg_unescape(mapping, item['xsmll'])
        item['parentnetprofit'] = util.yjyg_unescape(mapping, item['parentnetprofit'])

        #以下字段目前未使用
        item['basiceps'] = util.yjyg_unescape(mapping, item['basiceps'])
        item['cutbasiceps'] = util.yjyg_unescape(mapping, item['cutbasiceps'])
        item['ystz'] = util.yjyg_unescape(mapping, item['ystz'])
        item['yshz'] = util.yjyg_unescape(mapping, item['yshz'])
        item['sjltz'] = util.yjyg_unescape(mapping, item['sjltz'])
        item['sjlhz'] = util.yjyg_unescape(mapping, item['sjlhz'])
        item['bps'] = util.yjyg_unescape(mapping, item['bps'])
        item['mgjyxjje'] = util.yjyg_unescape(mapping, item['mgjyxjje'])
        item['assigndscrpt'] = util.yjyg_unescape(mapping, item['assigndscrpt'])
        item['gxl'] = util.yjyg_unescape(mapping, item['gxl'])



        one_stock = util.dealwithData(item, util.threeOP(DATA_SUB,
                                                                     NEED_TO_NUMBER, KEY_NAME))

        one_stock[MONGODB_ID] = one_stock[ID_NAME]

        #下面两个都是百分数
        if isinstance(one_stock[KEY_NAME['roeweighted']], float):
          one_stock[KEY_NAME['roeweighted']] /= 100
        if isinstance(one_stock[KEY_NAME['xsmll']], float):
          one_stock[KEY_NAME['xsmll']] /= 100
        #计算销售净利率
        if isinstance(one_stock[KEY_NAME['parentnetprofit']], float) and isinstance(one_stock[KEY_NAME['totaloperatereve']], float):
          try:
            one_stock[KEY_NAME['xsjll']] = one_stock[KEY_NAME['parentnetprofit']] / one_stock[KEY_NAME['totaloperatereve']]
          except:
            one_stock[KEY_NAME['xsjll']] = np.nan

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
