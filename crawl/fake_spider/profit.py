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

# encode_params = RequestEncodingMixin._encode_params

from pymongo import MongoClient
from pymongo import errors

# this project
if __name__ == '__main__':
  import sys
  
  sys.path.append(r'C:\workspace\code\self\github\py-code\new_stock')
##########################
import util
import const
import query
from fake_spider import spider2
from fake_spider import spider


#####################################################
class MyNumbers:
  def __init__(self, **kwargs):
    self.codes = query.query_stock_list.queryAllCode()
    self.kwargs = kwargs
    self.counter = 0
  
  def __iter__(self):
    return self
  
  def __next__(self):
    if self.counter < len(self.codes):
      code = self.codes[self.counter]
      head = "sh"
      if code[0] == '0' or code[0] == '3':
        head = 'sz'
      url = "https://eniu.com/chart/profita/{0}{1}".format(
        head, code)
      
      if self.counter % 50 == 0:
        print("now code............................................................... {0}".format(self.counter))
      self.counter += 1
      return (url, self.kwargs)
    else:
      raise StopIteration


class Handler(spider2.FakeSpider):
  crawl_config = {
  }
  
  def __init__(self):
    spider2.FakeSpider.__init__(self)
    self.notOK = 0
  
  def on_start(self):
    myclass = MyNumbers(headers=self.header(), callback=self.processFirstPage)
    myiter = iter(myclass)
    self.crawl(myiter)
    # self.crawl(self.url(), headers=self.header(), callback=self.processFirstPage)
  
  def url(self):
    url = "https://eniu.com/chart/profita/sh{0}".format("601398")
    out = []
    out.append(url)
    return out
  
  def header(self):
    headers = {
      "Accept": "application/json, text/plain, */*",
      "Accept-Encoding": "gzip, deflate, br",
      "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
      "Connection": "keep-alive",
      "Cookie": "PHPSESSID=0v43ul88556id679l4b9rojp00; Hm_lvt_45d0f23af3186fc1292d2629c2cbacb6=1574876344; __gads=Test; Hm_lpvt_45d0f23af3186fc1292d2629c2cbacb6=1574908733",
      "Host": "eniu.com",
      "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
      "Referer": "ttps://eniu.com/gu/sh601398/roe",
      "X-Requested-With": "XMLHttpRequest",
    }
    return headers
  
  def processFirstPage(self, response):
    if response.ok == False:
      self.notOK += 1
      if self.notOK > 20:
        print(response.url)
      return
    
    try:
      data1 = response.content[1:-1]
      data2 = data1.decode("ascii")
      data3 = data2.replace("\\", "")
      jsonData = json.loads(data3)  # , encoding='GB2312')
      urls = response.url.split('/')
      jsonData["_id"] = urls[-1]
      util.saveMongoDB2(jsonData, "stock2", "profit")
    except UnicodeDecodeError as e:
      print(e)
    except Exception as e:
      print(e)


def run():
  gpfh = Handler()
  gpfh.on_start()
  gpfh.run()


if __name__ == '__main__':
  # ProcessRank2()
  run()
