# -*- encoding: utf-8 -*-

# sys
import json
import datetime
# thirdpart
import pandas as pd
import tushare as ts
from pymongo import MongoClient


# this project

##########################
import util.crawl as util
import const.crawl as const

#http://tushare.org/classifying.html#id8
# code :股票代码
# name :股票名称
# date :日期
# weight:权重



def getLastK(code):
  end = util.today().strftime('%Y-%m-%d')
  start = util.weekAgo().strftime('%Y-%m-%d')
  try:
    df = ts.get_k_data(code, start=start, end=end)
    df.loc[:, 'date'] = pd.to_datetime(df.loc[:, 'date'])
    df.set_index('date', inplace=True)
    df.drop('code', axis=1, inplace=True)
    return df
  except Exception as e:
    print(e)



def getKData(code, starts='2001-01-01'):
  try:
    df = ts.get_k_data(code, start=starts,  index=False)
    df.loc[:, 'date'] = pd.to_datetime(df.loc[:, 'date'])
    df.set_index('date', inplace=True)
    df.drop('code', axis=1, inplace=True)
    return df
  except Exception as e:
    print(e)



def getKDataRecent(code):
  try:
    now = datetime.datetime.now()
    starts = now - datetime.timedelta(days=15)
    starts = starts.strftime('%Y-%m-%d')
    df = ts.get_k_data(code, start=starts,  index=False)
    df.loc[:, 'date'] = pd.to_datetime(df.loc[:, 'date'])
    df.set_index('date', inplace=True)
    df.drop('code', axis=1, inplace=True)
    return df
  except Exception as e:
    print(e)




def getKDataNone(code, starts='2001-01-01', index=False):
  try:
    df = ts.get_k_data(code, start=starts, autype=None,  index=index)
    df.loc[:, 'date'] = pd.to_datetime(df.loc[:, 'date'])
    df.set_index('date', inplace=True)
    df.drop('code', axis=1, inplace=True)
    return df
  except Exception as e:
    print(e)


def saveDB(data: pd.DataFrame, code, handler=None):
  def callback(result):
    # handler.send_message(handler.project_name, result, self._date + '_' + result['_id'])
    pass

  re = util.updateMongoDB(data, util.genKeyCodeFunc('date'), const.KData.DB_NAME,
                          const.KData.COLLECTION_D_HEAD + code, True, callback)
  # util.everydayChange(re, 'gpfh')


def run():
  STOCK_LIST = setting.currentStockList()
  for one in STOCK_LIST:
    re = getKDataRecent(one)
    saveDB(re, one)
    

def runAll():
  STOCK_LIST = setting.currentStockList()
  for one in STOCK_LIST:
    re = getKData(one)
    saveDB(re, one)
    

#这个是前复权
def RunOne(code, force=False):
  #dblist = MongoClient.list_database_names()
  client = MongoClient()
  db = client['stock_all_kdata']
  collectionLIst = db.list_collection_names()
  if not force and code in collectionLIst:
    print("exist {}".format(code))
  else:
    #如果强制更新，删除已有数据
    if force and code in collectionLIst:
      db.drop_collection(code)
    re = getKData(code)
    saveDB2(re, code)
   
   
def saveDB2(data: pd.DataFrame, code, handler=None):
  def callback(result):
    pass

  util.updateMongoDB(data, util.genKeyCodeFunc('date'), "stock_all_kdata",
                     const.KData.COLLECTION_D_HEAD + code, True, callback)

#这个是不复权
def RunOneNone(code):
  client = MongoClient()
  db = client['stock_all_kdata_none']
  collectionList = db.list_collection_names()
  if code in collectionList:
    print("exist {}".format(code))
  else:
    re = getKDataNone(code)
    saveDB3(re, code)
    
  
#最近一个月的数据
def RunOneNoneRecent(code):
  now = datetime.datetime.now()
  starts = now - datetime.timedelta(days=31)
  #starts = datetime.datetime(now.year, now.month, 1)
  starts = starts.strftime('%Y-%m-%d')
  re = getKDataNone(code, starts)
  saveDB3(re, code)


def RunHS300IndexRecent():
  now = datetime.datetime.now()
  starts = now - datetime.timedelta(days=31)
  # starts = datetime.datetime(now.year, now.month, 1)
  starts = starts.strftime('%Y-%m-%d')
  re = getKDataNone('000300', starts, index=True)
  saveDB3(re, '000300')



def RunHS300Index():
  re = getKDataNone('000300', starts='2001-01-01', index=True)
  saveDB3(re, '000300')



def saveDB3(data: pd.DataFrame, code, handler=None):
  def callback(result):
    pass
  
  util.updateMongoDB(data, util.genKeyCodeFunc('date'), "stock_all_kdata_none",
                     const.KData.COLLECTION_D_HEAD + code, True,
                     callback)


