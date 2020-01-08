#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on 2018-07-30 08:54:31
# Project: m012

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
import util
import util.utils
import const
from fake_spider import spider

MONGODB_ID = const.MONGODB_ID
ID_NAME = const.M012_KEYWORD.ID_NAME
DB_NAME = const.M012_KEYWORD.DB_NAME
COLLECTION_HEAD = const.M012_KEYWORD.COLLECTION_HEAD
KEY = const.M012_KEYWORD.KEY
MX_TYPE = const.M012_KEYWORD.MX_TYPE
KEY_NAME = const.M012_KEYWORD.KEY_NAME



base_url = 'http://dc.xinhua08.com/?'
headers = {
  'Host': 'dc.xinhua08.com',
  'Referer': 'http://dc.xinhua08.com/9/',
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
  'X-Requested-With': 'XMLHttpRequest',
}



class Handler(spider.FakeSpider):
  crawl_config = {
  }

  def on_start(self):
    out = self.url()
    for one in out:
      save = {'key': one[1]}
      self.crawl(one[0], headers=self.header(), callback=self.processFirstPage, save=save)

  def genParams(self, type):
    params = {
      'action': 'json',
      'id': type,
      'jsoncallback': KEY,
      '_': int(time.time()),
    }
    return params

  def url(self):
    out = []
    for k, v in MX_TYPE.items():
      url = encode_params(self.genParams(v))
      url = base_url + url
      out.append((url, k))

    return out

  def header(self):
    headers = {
      'Host': 'dc.xinhua08.com',
      'Referer': 'http://dc.xinhua08.com/9/',
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    }
    return headers


  def saveDB(self, data: pd.DataFrame, key):
    def callback(result):
      self.send_message(self.project_name, result, key + '_' + result[ID_NAME])

    util.saveMongoDB(data, util.genEmptyFunc(), DB_NAME, COLLECTION_HEAD + key, callback)

  def processFirstPage(self, response):
    if response.ok == False:
      return

    data = response.content.decode('utf-8')
    # d = data[0:len(data)-2]
    # d2 = data[1:len(data) - 2]
    # d3 = data[2:len(data) - 2]
    # d4 = data[3:len(data) - 2]
    # d5 = data[4:len(data) - 2]
    # strange...
    data = data[len(KEY) + 2:len(data) - 2]
    print(data)
    json_data = json.loads(data)
    date = json_data['xAxis']['data']
    m_data = json_data['series'][0]['data']
    if len(date) == len(m_data):
      save = response.save
      self.saveDB(self.genData(date, m_data), save['key'])

  def genData(self, date, m_data):
    data = {MONGODB_ID: date, KEY_NAME[ID_NAME]: date, KEY_NAME['data']: m_data}
    df = pd.DataFrame(data=data)
    print(df)
    return df

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
