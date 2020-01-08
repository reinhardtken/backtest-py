# -*- coding: utf-8 -*-

# sys
import datetime
import re
import traceback

# thirdpart
import pandas as pd
import pymongo
from pymongo import MongoClient
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.dates import DayLocator, DateFormatter
from pymongo import MongoClient
from pymongo import errors

#this project
import util
import const

def getWeekofYear():
  iso = datetime.datetime.now().isocalendar()
  return int(iso[0]) * 100 + int(iso[1])


def String2Number(s):
  out = np.nan
  try:
    out = float(re.findall('([-+]?\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?', s)[0][0])
  except Exception as e:
    pass

  return out


def SaveMongoDB(data, dbName, collectionName):
  client = MongoClient()
  db = client[dbName]
  collection = db[collectionName]
  result = data
  
  try:
    update_result = collection.update_one({'_id': result['_id']},
                                          {'$set': result})  # , upsert=True)
    
    if update_result.matched_count > 0:
      print('upate to Mongo: %s : %s' % (dbName, collectionName))
      if update_result.modified_count > 0:
        # detail[k] = result
        pass
    
    if update_result.matched_count == 0:
      try:
        if collection.insert_one(result):
          print('insert to Mongo: %s : %s' % (dbName, collectionName))
          # detail[k] = result
      except errors.DuplicateKeyError as e:
        print('faild to Mongo!!!!: %s : %s' % (dbName, collectionName))
        pass
  
  except Exception as e:
    print(e)
    



def SaveMongoDB_DF(data: pd.DataFrame, dbName, collectionName, insert=True):
  print('enter SaveMongoDBDF')
  client = MongoClient()
  db = client[dbName]
  collection = db[collectionName]


  for k, v in data.iterrows():
    result = v.to_dict()

    try:
      update_result = collection.update_one({'_id': result['_id']},
                                          {'$set': result}, upsert=insert)

      if update_result.matched_count > 0 and update_result.modified_count > 0:
        print('update to Mongo: %s : %s'%(dbName, collectionName))
      elif update_result.upserted_id is not None:
        print('insert to Mongo: %s : %s : %s' % (dbName, collectionName, update_result.upserted_id))

    except errors.DuplicateKeyError as e:
      print('DuplicateKeyError to Mongo!!!: %s : %s : %s' % (dbName, collectionName, result['_id']))
    except Exception as e:
      print(e)

  print('leave SaveMongoDBDF')



def SaveMongoDBDict(data: dict, dbName, collectionName, insert=True):
  print('enter SaveMongoDBDict')
  client = MongoClient()
  db = client[dbName]
  collection = db[collectionName]


  for k, v in data.items():
    try:
      update_result = collection.update_one({'_id': k},
                                          {'$set': v}, upsert=insert)

      if update_result.matched_count > 0 and update_result.modified_count > 0:
        print('update to Mongo: %s : %s'%(dbName, collectionName))
      elif update_result.upserted_id is not None:
        print('insert to Mongo: %s : %s : %s' % (dbName, collectionName, update_result.upserted_id))

    except errors.DuplicateKeyError as e:
      print('DuplicateKeyError to Mongo!!!: %s : %s : %s' % (dbName, collectionName, k))
    except Exception as e:
      print(e)

  print('leave SaveMongoDBDict')

  
def QueryHS300All():
  client = MongoClient()
  db = client['stock']
  collection = db['hs300_stock_list']

  out = []

  cursor = collection.find()
  for c in cursor:
    out.append(c)

  if len(out):
    df = pd.DataFrame(out)
    df.set_index(const.HS300.KEY_NAME['code'], inplace=True)
    return df
  else:
    return None
  
  

def QueryCodeList():
  client = MongoClient()
  db = client['stock']
  collection = db['hs300_stock_list']

  out = []

  cursor = collection.find()
  index = 0
  for c in cursor:
    out.append(c[const.HS300.KEY_NAME['code']])

  return out



def QueryAll():
  client = MongoClient()
  db = client['stock']
  collection = db['stock_list']

  out = []

  cursor = collection.find()
  for c in cursor:
    out.append(c)

  if len(out):
    df = pd.DataFrame(out)
    df.set_index("_id", inplace=True)
    return df
  else:
    return None
  
  
def PrintException(e):
  msg = traceback.format_exc()
  print(msg)


#加载所有记录
def LoadData(dbName, collectionName, condition={}, sort=[('_id', 1)], limit=None):
  client = MongoClient()
  db = client[dbName]
  collection = db[collectionName]
  cursor = collection.find(condition).sort(sort)
  if limit is not None:
    cursor = cursor.limit(limit)
  out = []
  for c in cursor:
    out.append(c)

  if len(out):
    df = pd.DataFrame(out)
    # df.drop('date', axis=1, inplace=True)
    df.set_index('_id', inplace=True)
    return df

  return None


#每个code加载一条记录
def LoadData2(dbName, collectionName, codes):
  client = MongoClient()
  db = client[dbName]
  collection = db[collectionName]
  out = []
  for code in codes:
    cursor = collection.find({'_id': code})
    for c in cursor:
      out.append(c)
      break

  if len(out):
    df = pd.DataFrame(out)
    try:
      df.drop('name', axis=1, inplace=True)
    except Exception as e:
      pass
    df.set_index('_id', inplace=True)
    return df


def LastPriceNone(codes):
  client = MongoClient()
  
  out = []
  for code in codes:
    db = client['stock_all_kdata_none']
    collection = db[code]
    cursor = collection.find().sort([('_id', -1)]).limit(1)
    for c in cursor:
      out.append({'_id': code, 'price': c['close']})
      break

  if len(out):
    df = pd.DataFrame(out)
    df.set_index('_id', inplace=True)
    return df


  return None



def LoadQuaterPaper(year, code):
  # 加载季报
  first = {}
  second = {}
  third = {}
  forth = {}
  client = MongoClient()
  db = client["stock"]
  collection = db["yjbg-" + code]
  strYear = str(year)
  # 一季度
  cursor = collection.find({"_id": strYear + "-03-31"})
  for c in cursor:
    first['sjltz'] = c['sjltz']
    break

  # 二季度
  cursor = collection.find({"_id": strYear + "-06-30"})
  for c in cursor:
    second['sjltz'] = c['sjltz']
    break

  # 三季度
  cursor = collection.find({"_id": strYear + "-09-30"})
  for c in cursor:
    third['sjltz'] = c['sjltz']
    break

  # 四季度
  cursor = collection.find({"_id": strYear + "-12-31"})
  for c in cursor:
    forth['sjltz'] = c['sjltz']
    break

  return (first, second, third, forth)



def LoadForecast(year, code):
  # 加载季报
  first = {}
  second = {}
  third = {}
  forth = {}
  client = MongoClient()
  db = client["stock"]
  strYear = str(year)
  
  # 一季度
  collection = db["yjyg-" + str(strYear) + "-03-31"]
  cursor = collection.find({"_id": code})
  for c in cursor:
    first['date'] = c['公告日期']
    first['forecast'] = c['预告类型']
    break

  # 二季度
  collection = db["yjyg-" + str(strYear) + "-06-30"]
  cursor = collection.find({"_id": code})
  for c in cursor:
    second['date'] = c['公告日期']
    second['forecast'] = c['预告类型']
    break

  # 三季度
  collection = db["yjyg-" + str(strYear) + "-09-30"]
  cursor = collection.find({"_id": code})
  for c in cursor:
    third['date'] = c['公告日期']
    third['forecast'] = c['预告类型']
    break

  # 四季度
  collection = db["yjyg-" + str(strYear) + "-12-31"]
  cursor = collection.find({"_id": code})
  for c in cursor:
    forth['date'] = c['公告日期']
    forth['forecast'] = c['预告类型']
    break

  return (first, second, third, forth)



def LoadYearPaper(y, code):
  # 加载年报，中报
  midYear = {}
  year = {}
  client = MongoClient()
  db = client["stock"]
  collection = db["gpfh-" + str(y) + "-06-30"]
  cursor = collection.find({"_id": code})
  for c in cursor:
    midYear[const.GPFH_KEYWORD.KEY_NAME['CQCXR']] = c[const.GPFH_KEYWORD.KEY_NAME['CQCXR']]
    midYear[const.GPFH_KEYWORD.KEY_NAME['AllocationPlan']] = c[const.GPFH_KEYWORD.KEY_NAME['AllocationPlan']]
    midYear[const.GPFH_KEYWORD.KEY_NAME['EarningsPerShare']] = c[const.GPFH_KEYWORD.KEY_NAME['EarningsPerShare']]
    break
  else:
    midYear.update({'notExist': 1})

  collection = db["gpfh-" + str(y) + "-12-31"]
  cursor = collection.find({"_id": code})
  for c in cursor:
    year[const.GPFH_KEYWORD.KEY_NAME['CQCXR']] = c[const.GPFH_KEYWORD.KEY_NAME['CQCXR']]
    year[const.GPFH_KEYWORD.KEY_NAME['AllocationPlan']] = c[const.GPFH_KEYWORD.KEY_NAME['AllocationPlan']]
    year[const.GPFH_KEYWORD.KEY_NAME['EarningsPerShare']] = c[const.GPFH_KEYWORD.KEY_NAME['EarningsPerShare']]
    break
  else:
    year.update({'notExist': 1})

  return (midYear, year)


#########################################################
def Quater2Date(year, quarter):
  # 从某个季度，转换到具体日期
  if quarter == 'first':
    return pd.to_datetime(np.datetime64(str(year) + '-04-30T00:00:00Z'))
  elif quarter == 'second':
    return pd.to_datetime(np.datetime64(str(year) + '-08-31T00:00:00Z'))
  elif quarter == 'third':
    return pd.to_datetime(np.datetime64(str(year) + '-10-31T00:00:00Z'))
  elif quarter == 'forth':
    # 来年一季度,这里反正有问题，用29号变通下
    return pd.to_datetime(np.datetime64(str(year + 1) + '-04-29T00:00:00Z'))
  
  
def ForecastString2Int(info):
  infos = {
    "首亏": -100,
    "略增": 1,
    "略减": -1,
    "扭亏": -10,
    "增亏": -50,
    "减亏": -20,
    "续亏": -30,
    "续盈": 1,
    "预增": 1,
    "预减": -1,
  }
  if info in infos:
    return infos[info]
  else:
    return -5