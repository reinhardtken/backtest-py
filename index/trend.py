# -*- coding: utf-8 -*-

from datetime import datetime
from datetime import timedelta
from dateutil import parser
import traceback
from queue import PriorityQueue

# thirdpart
import pandas as pd
from pymongo import MongoClient
import numpy as np

import const
import util



class ProfitMarginTrend:
  # db.getCollection('yjbg-002269').find({})
  # 中的sjltz，为负或减前值为负，则单趋势为 - 1，否则为 + 1
  # 前单趋势为 - 1，本单趋势为 - 1，则本连续趋势为 - 2
  # 计算单趋势和连续趋势
  
  def Run(codes):
    #计算全财报的，计算2010年作为开始年份的
    for one in codes:
      try:
        out = []
        # out2010 = []
        beforeSJLTZ = 0
        beforeContinuityTrend = 0
        # beforeSJLTZ2010 = 0
        beforeContinuityTrend2010 = 0
        df = util.LoadData('stock', 'yjbg-'+one, condition={}, sort=[('_id', 1)])
        for quarter, row in df.iterrows():
          id = datetime.strptime(quarter, '%Y-%m-%d')
          value = util.String2Number(row['sjltz'])
          if not np.isnan(value):
            nowTrend = None
            #这个条件不过分，一个公司即使业务完全停止增长，考虑到通胀也该在数值上是增长的
            if value < 0:
              nowTrend = -1
              # nowTrend2010 = -1
            elif value - beforeSJLTZ < 0:
              nowTrend = -0.5
              # nowTrend2010 = -0.5
            else:
              nowTrend = 1
              # nowTrend2010 = 1
            beforeSJLTZ = value
            nowContinuityTrend = beforeContinuityTrend + nowTrend
            beforeContinuityTrend = nowContinuityTrend
            out.append({'_id': id, 'nowPMT': nowTrend, 'continuityPMT': nowContinuityTrend})
            if id.year >= 2010:
              nowContinuityTrend2010 = beforeContinuityTrend2010 + nowTrend
              beforeContinuityTrend2010 = nowContinuityTrend2010
              out[-1].update({'continuityPMTFrom2010': nowContinuityTrend2010})
  
        dfOut = pd.DataFrame(out)
        util.SaveMongoDB_DF(dfOut, 'stock_statistics', one)
      except Exception as e:
        print(e)
  
  def Show(codes):
    out = []
    for one in codes:
      try:
        df = util.LoadData('stock_statistics', one['_id'], condition={}, sort=[('_id', -1)], limit=1)
        df['_id'] = one['_id']
        df['name'] = one['name']
        df['date'] = df.index
        df.set_index('_id', inplace=True)
        out.append(df)
      except Exception as e:
        print(e)

    dfAll = out[0]
    for index in range(1, len(out)):
      dfAll = dfAll.append(out[index])
    print(dfAll)