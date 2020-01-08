#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# sys
import datetime
# thirdpart
import pymongo
from pymongo import MongoClient
from pymongo import errors
import pandas as pd
import numpy as np

# this project
##########################

import const.crawl as const


def isnan(x):
  if isinstance(x, str) and x == '无':
    return True
  return np.isnan(x)


FirstQuarter = datetime.datetime.strptime('03-31', '%m-%d')
SecondQuarter = datetime.datetime.strptime('06-30', '%m-%d')
ThirdQuarter = datetime.datetime.strptime('09-30', '%m-%d')
FourthQuarter = datetime.datetime.strptime('12-31', '%m-%d')


def today():
  now = datetime.datetime.now()
  return now.replace(hour=0, minute=0, second=0, microsecond=0)


def weekAgo():
  t = today()
  diff = datetime.timedelta(days=7)
  return t - diff


def getYear(date: datetime.datetime):
  return date.year


def getQuarter(date: datetime.datetime):
  return date.month


def isSameQuarter(d1: datetime.datetime, d2: datetime.datetime):
  return d1.month == d2.month


def priorYear(date):
  return date.replace(year=date.year - 1)


def nextYear(date):
  return date.replace(year=date.year + 1)


def priorQuarter(date):
  if isSameQuarter(date, FourthQuarter):
    return getThirdQuarter(date)
  elif isSameQuarter(date, ThirdQuarter):
    return getSecondQuarter(date)
  elif isSameQuarter(date, SecondQuarter):
    return getFirstQuarter(date)
  elif isSameQuarter(date, FirstQuarter):
    newOne = priorYear(date)
    return getFourthQuarter(newOne)
  
  return None


def nextQuarter(date):
  if isSameQuarter(date, FirstQuarter):
    return getSecondQuarter(date)
  elif isSameQuarter(date, SecondQuarter):
    return getThirdQuarter(date)
  elif isSameQuarter(date, ThirdQuarter):
    return getFourthQuarter(date)
  elif isSameQuarter(date, FourthQuarter):
    newOne = nextYear(date)
    return getFirstQuarter(newOne)
  
  return None


def changeQuarter(date, des):
  return date.replace(month=des.month, day=des.day)


def getFirstQuarter(date):
  return changeQuarter(date, FirstQuarter)


def getSecondQuarter(date):
  return changeQuarter(date, SecondQuarter)


def getThirdQuarter(date):
  return changeQuarter(date, ThirdQuarter)


def getFourthQuarter(date):
  return changeQuarter(date, FourthQuarter)


def priorXQuarter(date, x):
  date = priorQuarter(date)
  for n in range(1, x):
    if date != None:
      date = priorQuarter(date)
  
  return date


def nextXQuarter(date, x):
  date = nextQuarter(date)
  for n in range(1, x):
    if date != None:
      date = nextQuarter(date)
  
  return date


def nowQuarter():
  now = datetime.datetime.now()
  if now.month >= 1 and now.month <= 3:
    return now.replace(month=3, day=31, hour=0, minute=0, second=0, microsecond=0)
  elif now.month >= 4 and now.month <= 6:
    return now.replace(month=6, day=30, hour=0, minute=0, second=0, microsecond=0)
  elif now.month >= 7 and now.month <= 9:
    return now.replace(month=9, day=30, hour=0, minute=0, second=0, microsecond=0)
  else:
    return now.replace(month=12, day=31, hour=0, minute=0, second=0, microsecond=0)


def performancePreviewRange():
  # 11月1日-4月30：上一年4季
  # 如果是11月15号，有今年四季度的每股收益，则当季是明年一季度。否则，是今年四季度
  # 如果是2月15号，有去年四季度每股收益，则当季是今年一季度，否则是去年四季度
  # 5月1-8月30：二季
  # 9月1-10月30：三季
  
  now = datetime.datetime.now()
  nowDay = now.replace(hour=0, minute=0, second=0, microsecond=0)
  first = now.replace(month=4, day=30, hour=0, minute=0, second=0, microsecond=0)
  second = now.replace(month=8, day=31, hour=0, minute=0, second=0, microsecond=0)
  third = now.replace(month=10, day=31, hour=0, minute=0, second=0, microsecond=0)
  if (now - first).total_seconds() < 0:
    return [priorQuarter(getFirstQuarter(nowDay)), getFirstQuarter(nowDay)]
  elif (now - second).total_seconds() < 0:
    return [getSecondQuarter(nowDay), getThirdQuarter(nowDay)]
  elif (now - third).total_seconds() < 0:
    return [getThirdQuarter(nowDay), getFourthQuarter(nowDay)]
  else:
    return [getFourthQuarter(nowDay), getFirstQuarter(nextYear(nowDay))]


def saveMongoDB(data: pd.DataFrame, keyFunc, dbName, collectionName, callback=None):
  client = MongoClient()
  db = client[dbName]
  collection = db[collectionName]
  
  out = {'db': dbName, 'collection': collectionName}
  detail = {}
  
  for k, v in data.iterrows():
    result = v.to_dict()
    # print(dir(k))
    result.update(keyFunc(k, result))
    
    try:
      if callback:
        callback(result)
    except Exception as e:
      print(e)
    try:
      update_result = collection.update_one({'_id': result['_id']},
                                            {'$set': result})  # , upsert=True)
      
      if update_result.matched_count > 0:
        print('upate to Mongo: %s : %s' % (dbName, collectionName))
        if update_result.modified_count > 0:
          detail[k] = result
      
      if update_result.matched_count == 0:
        try:
          if collection.insert_one(result):
            print('insert to Mongo: %s : %s' % (dbName, collectionName))
            detail[k] = result
        except errors.DuplicateKeyError as e:
          print('faild to Mongo!!!!: %s : %s' % (dbName, collectionName))
          pass
    
    except Exception as e:
      print(e)
  
  out['detail'] = detail
  print('leave saveMongoDB')
  return out


def everydayChange(result, crawl):
  if len(result['detail']) > 0:
    client = MongoClient()
    db = client['stock-everyday']
    collection = db[datetime.datetime.now().strftime('%Y-%m-%d')]
    
    for k, v in result['detail'].items():
      v['index'] = k
      v['crawl'] = crawl
      if '_id' in v:
        v.pop('_id')
      
      try:
        if collection.insert_one(v):
          pass
      except errors.DuplicateKeyError as e:
        print('faild to Mongo!!!!')
        pass
  
  else:
    pass


def genKeyDateFunc(k):
  def keyDateFunc(v, d):
    if isinstance(v, str):
      out = {k: v}
      if const.COMMON_ID not in d:
        out[const.COMMON_ID] = v
    else:
      out = {k: v.strftime('%Y-%m-%d')}
      if const.COMMON_ID not in d:
        out[const.COMMON_ID] = v.strftime('%Y-%m-%d')
    
    return out
  
  return keyDateFunc


def genEmptyFunc():
  def emptyFunc(v, d):
    return {}
  
  return emptyFunc


def genKeyCodeFunc(k):
  return lambda v, d: {const.MONGODB_ID: v, k: v}


def genKeyIDFunc(k):
  return lambda v, d: {const.MONGODB_ID: k}


def genKeyMONGODB_IDFunc():
  return lambda v, d: {const.MONGODB_ID: v}


def updateMongoDB(data: pd.DataFrame, keyFunc, dbName, collectionName, insert=True, callback=None):
  print('enter updateMongoDB')
  client = MongoClient()
  db = client[dbName]
  collection = db[collectionName]
  
  out = {'db': dbName, 'collection': collectionName}
  detail = {}
  
  for k, v in data.iterrows():
    result = v.to_dict()
    # print(dir(k))
    result.update(keyFunc(k, result))
    
    try:
      if callback:
        callback(result)
    except Exception as e:
      print(e)
    try:
      update_result = collection.update_one({'_id': result['_id']},
                                            {'$set': result}, upsert=insert)
      
      if update_result.matched_count > 0 and update_result.modified_count > 0:
        print('update to Mongo: %s : %s' % (dbName, collectionName))
        result['dbop'] = 'update'
        detail[k] = result
      elif update_result.upserted_id is not None:
        print('insert to Mongo: %s : %s : %s' % (dbName, collectionName, update_result.upserted_id))
        result['dbop'] = 'insert'
        detail[k] = result
    
    except errors.DuplicateKeyError as e:
      print('DuplicateKeyError to Mongo!!!: %s : %s : %s' % (dbName, collectionName, result['_id']))
    except Exception as e:
      print(e)
  
  out['detail'] = detail
  print('leave updateMongoDB')
  return out


def saveMongoDB2(data, dbName, collectionName):
  client = MongoClient()
  db = client[dbName]
  collection = db[collectionName]
  result = data
  
  try:
    update_result = collection.update_one({'_id': result['_id']},
                                          {'$set': result})  # , upsert=True)
    
    if update_result.matched_count > 0:
      print('upate to Mongo2: %s : %s' % (dbName, collectionName))
      if update_result.modified_count > 0:
        # detail[k] = result
        pass
    
    if update_result.matched_count == 0:
      try:
        if collection.insert_one(result):
          print('insert to Mongo2: %s : %s' % (dbName, collectionName))
          # detail[k] = result
      except errors.DuplicateKeyError as e:
        print('faild to Mongo2!!!!: %s : %s' % (dbName, collectionName))
        pass
  
  except Exception as e:
    print(e)
  
  print('leave saveMongoDB2')


###########################
def addSysPath(path):
  import sys
  for one in sys.path:
    if one == path:
      return
  sys.path.append(path)


if __name__ == '__main__':
  nowQuarter()
  pass
#########################################################
#!/usr/bin/env python
# -*- encoding: utf-8 -*-


import re
from datetime import datetime



def TimeString2TimeStamp(date):
  a = datetime.strptime(date, '%Y-%m-%d')
  return a.timestamp()



def genchangeKeyFunc(newKey):
  def changeKey(k, v):
    if k in newKey:
      return newKey[k], v
    return k, v

  return changeKey


def genCutDateFunc(key):
  def cutDate(k, v):
    if k in key:
      return k, v[:10]
    return k, v

  return cutDate


def genString2NumberFunc(key):
  def tryFloat(v):
    try:
      return True, float(v)
    except ValueError as e:
      return False, v

  def toNumber(k, v):
    if k in key:
      succ, v = tryFloat(v)
      if succ == True:
        return k, v
      newV = v.replace(',', '')
      succ, v = tryFloat(newV)
      if succ == True:
        return k, v
      # if v.find('万亿') != -1:
      #   print(v)
      if v[-2:] == '万亿':  # 2.36万亿
        _, v = tryFloat(v[:-2])
        v *= 100000000 * 10000
      elif v[-1] == '亿':  # 938亿
        _, v = tryFloat(v[:-1])
        v *= 100000000


    return k, v

  return toNumber


def genEatFunc(key):
  def eat(k, v):
    if k in key:
      return k, v
    else:
      return None, None

  return eat


def threeOP(k1, k2, k3):
  return [genCutDateFunc(k1), genString2NumberFunc(k2), genchangeKeyFunc(k3)]


def fourOP(k1, k2, k3, k4):
  return [genCutDateFunc(k1), genString2NumberFunc(k2), genEatFunc(k3), genchangeKeyFunc(k4)]


def dealwithData(data, itemList):
  out = {}
  for k, v in data.items():
    for op in itemList:
      try:
        k, v = op(k, v)
      except ValueError as e:
        print(e)

    if k is not None:
      out[k] = v

  return out


def yjyg_unescape(mapping, s):
  # [{"code": "&#xE426;", "value": 1}, {"code": "&#xECD9;", "value": 2}, {"code": "&#xE891;", "value": 3},
  #  {"code": "&#xECE9;", "value": 4}, {"code": "&#xEBED;", "value": 5}, {"code": "&#xE7A3;", "value": 6},
  #  {"code": "&#xE73F;", "value": 7}, {"code": "&#xF78F;", "value": 8}, {"code": "&#xE375;", "value": 9},
  #  {"code": "&#xF2F8;", "value": 0}]
  mapped = {}
  for one in mapping:
    mapped[one['code']] = one['value']

  #'nideposit': 1880107000000.0
  #工商银行的这个字段没有加密。。。
  if isinstance(s, float):
    return s

  if '&' not in s:
    return s

  def replaceEntities(s):
    s = s.groups()[0]
    try:
      if s[0] == "&" and s[1] == '#' and s[2] in ['x', 'X'] and s[-1] == ';':
        if s in mapped:
          return str(mapped[s])
    except Exception as e:
      print(e)
      return s


  return re.sub(r"(&#?[xX]?(?:[0-9a-fA-F]+|\w{1,8});)", replaceEntities, s)

# def readList():
#   import xlrd
#
#   workbook = xlrd.open_workbook('/home/ken/workspace/tmp/in.xlsx')
#
#   sheet = workbook.sheet_by_name('股票池')
#   '''
#   sheet.nrows　　　　sheet的行数
#   sheet.row_values(index)　　　　返回某一行的值列表
# 　　sheet.row(index)　　　　返回一个row对象，可以通过row[index]来获取这行里的单元格cell对象'''
#   nrows = sheet.nrows
#   out = []
#   for index in range(1, nrows):
#     print(nrows)
#     row = sheet.row(index)
#     out.append(row[0].value)
#
#   for one in out:
#     print('"' + str(one) + '",')
