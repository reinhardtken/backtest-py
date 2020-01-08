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
import util.crawl as util
import const.crawl as const
from crawl.fake_spider import spider

MONGODB_ID = const.MONGODB_ID
ID_NAME = const.YJYG_KEYWORD.ID_NAME
DB_NAME = const.YJYG_KEYWORD.DB_NAME
COLLECTION_HEAD = const.YJYG_KEYWORD.COLLECTION_HEAD
KEY_NAME = const.YJYG_KEYWORD.KEY_NAME
NEED_TO_NUMBER = const.YJYG_KEYWORD.NEED_TO_NUMBER
DATA_SUB = const.YJYG_KEYWORD.DATA_SUB


KEY = 'var XbnsgnRv'
'http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get?' \
'type=YJBB20_YJYG' \
'&token=70f12f2f4f091e459a279469fe49eca5' \
'&st=ndate&sr=-1&p=1&ps=30' \
'&js=var%20aUDOBatW={pages:(tp),data:%20(x)}' \
'&filter=(IsLatest=%27T%27)(enddate=^2018-09-30^)' \
'&rt=51098106'
#20190119
#http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get?
base_url = 'http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get'
headers = {
  'Host': 'dcfm.eastmoney.com',
  'Referer': 'http://data.eastmoney.com/bbsj/201812/yjyg.html',
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
  # 'X-Requested-With': 'XMLHttpRequest',
}



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
      #20190119
      # type=YJBB21_YJYG
      # &token=70f12f2f4f091e459a279469fe49eca5
      # &st=ndate
      # &sr=-1
      # &p=2&ps=30
      # &js=var%20thtXBqxv={pages:(tp),data:%20(x),font:(font)}
      # &filter=(IsLatest=%27T%27)(enddate=^2018-12-31^)
      # &rt=51596650
      # {"scode": "600862",
      #  "sname": "中航高科",
      #  "sclx": "上交所主板",
      #  "enddate": "2018-12-31T00:00:00",
      #  "forecasttype": "预增",
      #  "ndate": "2019-01-18T00:00:00",
      #  "hymc": "航天航空",
      #  "IsLatest": "T",
      #  "forecastl": "&#xF3C3;&#xE80C;&#xE793;&#xE793;&#xE793;&#xE80C;&#xF05A;&#xE80C;&#xE80C;",
      #  "forecastt": "&#xF3C3;&#xE80C;&#xE793;&#xE793;&#xE793;&#xE80C;&#xF05A;&#xE80C;&#xE80C;",
      #  "increasel": "&#xE7A3;&#xEA5D;&#xE80C;",
      #  "increaset": "&#xE7A3;&#xEA5D;&#xE80C;",
      #  "forecastcontent": "预计&#xE7A3;&#xE80C;&#xF2F8;&#xE793;年&#xF2F8;-&#xF2F8;&#xE7A3;月归属于上市公司股东的净利润盈利:&#xF3C3;&#xE80C;,&#xE793;&#xE793;&#xE793;.&#xE80C;&#xF05A;万元左右,同比上期增加&#xE7A3;&#xEA5D;&#xE80C;%左右。",
      #  "changereasondscrpt": "(一)报告期内,公司聚焦年度经营目标,复合材料业务收入呈现稳定增长态势,净利润同比增加约&#xF05A;,&#xF2F8;&#xE80C;&#xE80C;万元;机床业务同比减亏约&#xEBC0;,&#xE375;&#xE80C;&#xE80C;万元;房地产业务虽然收入规模收窄,受市场影响并加强管理提升,净利润同比增加约&#xF2F8;,&#xEBC0;&#xE80C;&#xE80C;万元。(二)报告期内,原重组时因构成业务的反向收购产生的合并口径评估增值摊销额同比减少约&#xEBC0;,&#xF3C3;&#xEA5D;&#xE80C;万元。(三)上年同期计提职工内退福利&#xE793;,&#xE793;&#xF05A;&#xF05A;万元,本期没有发生。",
      #  "yearearlier": "&#xE793;&#xF3C3;&#xF05A;&#xE375;&#xEA5D;&#xE375;&#xE80C;&#xE80C;",
      #  "zfpx": "&#xE7A3;&#xEA5D;&#xE80C;",
      #  "jlrpx": "&#xF3C3;&#xE80C;&#xE793;&#xE793;&#xE793;&#xE80C;&#xF05A;&#xE80C;&#xE80C;",
      #  "forecast": "increase"},
      params = {
        'type': 'YJBB21_YJYG',
        'token': '70f12f2f4f091e459a279469fe49eca5',
        'st': 'ndate',
        'sr': '-1',
        'p': page,
        'ps': '30',
        'js': 'var aUDOBatW={pages:(tp),data: (x),font:(font)}',
        'filter': '(IsLatest=\'T\')(enddate=^' + date + '^)',
        'rt': int(time.time()),
      }
      return params

    def genUrl(self, page):
      #url = encode_params()
      return base_url# + url


    def saveDB(self, data: pd.DataFrame, handler):
      def callback(result):
        handler.send_message(handler.project_name, result, self._date + '_' + result[KEY_NAME[ID_NAME]])

      re = util.saveMongoDB(data, util.genEmptyFunc(), DB_NAME, COLLECTION_HEAD + self._date, callback)
      util.everydayChange(re, 'yjyg')


  def url(self):
    return 'http://data.eastmoney.com/bbsj/201806/yjyg.html'

  def header(self):
    headers = {
      'Host': 'data.eastmoney.com',
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    }
    return headers

  def header2(self):
    headers = {
      'Host': 'dcfm.eastmoney.com',
      'Referer': 'http://data.eastmoney.com/bbsj/201806/yjyg.html',
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    }
    return headers

  def processFirstPage(self, response):
    if response.ok == False:
      return

    data_list = response.doc('#date_type')
    # doc = pyquery.PyQuery(response)
    # data_list = doc('#sel_bgq')
    out = data_list.find('option')

    for one in out:
      print(one.text)
      year = float(one.text[:4])
      if not Handler.ALL and year > 2018:
      # if one.text.startswith('2018'):
        innerTask = Handler.InnerTask(one.text)
        save = innerTask.dump()
        self.crawl(innerTask.genUrl(1), headers=self.header2(), param=innerTask.genParams(1, innerTask._date), callback=self.processSecondPage, save=save)
      elif Handler.ALL:
        innerTask = Handler.InnerTask(one.text)
        save = innerTask.dump()
        self.crawl(innerTask.genUrl(1), headers=self.header2(), param=innerTask.genParams(1, innerTask._date),
                   callback=self.processSecondPage, save=save)

  def processSecondPage(self, response):
    if response.ok == False:
      return

    content = response.content[13:]
    print(response.url)
    innerTask = Handler.InnerTask.load(response.save)
    try:
      data = content.decode('utf-8')
      print(data)
      #data2 = HTMLParser().unescape(data)
      data = data.replace('pages:', '"pages":', 1)
      data = data.replace('data:', '"data":', 1)
      data = data.replace('font:', '"font":', 1)
      json_data = json.loads(data)  # , encoding='GB2312')
      results = self.processDetailPage(json_data, innerTask)
      innerTask.saveDB(results, self)
    except UnicodeDecodeError as e:
      print(e)
    except Exception as e:
      print(e)

  def processDetailPage(self, json, innerTask):
    if json:

      total = json.get('pages')
      if innerTask.getTotalNumber == False:
        innerTask.getTotalNumber = True
        if total >= 2:
          save = innerTask.dump()
          for i in range(2, total + 1):
            self.crawl(innerTask.genUrl(i), headers=self.header2(), param=innerTask.genParams(i, innerTask._date), callback=self.processSecondPage,
                       save=save)

      items = json.get('data')
      mapping = json.get('font')['FontMapping']
      return self.parse_page(items, mapping)


  def parse_page(self, json, mapping):

    try:
      tmp = []
      for item in json:
        # 20190119数据格式变动
        item['forecastl'] = util.yjyg_unescape(mapping, item['forecastl'])
        item['forecastt'] = util.yjyg_unescape(mapping, item['forecastt'])
        item['increasel'] = util.yjyg_unescape(mapping, item['increasel'])
        item['increaset'] = util.yjyg_unescape(mapping, item['increaset'])
        item['forecastcontent'] = util.yjyg_unescape(mapping, item['forecastcontent'])
        item['changereasondscrpt'] = util.yjyg_unescape(mapping, item['changereasondscrpt'])
        item['yearearlier'] = util.yjyg_unescape(mapping, item['yearearlier'])
        item['zfpx'] = util.yjyg_unescape(mapping, item['zfpx'])
        item['jlrpx'] = util.yjyg_unescape(mapping, item['jlrpx'])

        one_stock = util.dealwithData(item, util.threeOP(DATA_SUB,
                                                                     NEED_TO_NUMBER, KEY_NAME))


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
