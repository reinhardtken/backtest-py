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

#统计整个财务数据中，-10%的出现次数，出现越多，说明卖点越多
class DangerousQuarterRatio:
  def Run(codes):
    out = []
    for one in codes:
      try:
        baseCounter = 0
        hitCounter = 0
        firstQuarter = None
        lastQuarter = None
        baseCounter2010 = 0
        hitCounter2010 = 0
        firstQuarter2010 = None
        lastQuarter2010 = None
        df = util.LoadData('stock', 'yjbg-' + one['_id'], condition={}, sort=[('_id', 1)])
        for quarter, row in df.iterrows():
          id = datetime.strptime(quarter, '%Y-%m-%d')
          value = util.String2Number(row['sjltz'])
          if not np.isnan(value):
            if firstQuarter is None:
              firstQuarter = id
            lastQuarter = id
            baseCounter += 1
            if id.year >= 2010:
              if firstQuarter2010 is None:
                firstQuarter2010 = id
              lastQuarter2010 = id
              baseCounter2010 += 1
            if value < -10:
              hitCounter += 1
              if id.year >= 2010:
                hitCounter2010 += 1
        
        percent = 0
        percent2010 = 0
        if baseCounter > 0:
          percent = hitCounter/baseCounter
        if baseCounter2010 > 0:
          percent2010 = hitCounter2010/baseCounter2010
        out.append({'_id': one['_id'],
                    'begin': firstQuarter, 'end': lastQuarter, 'base': baseCounter, 'hit': hitCounter, 'percent': percent,
                    'begin2010': firstQuarter2010, 'end2010': lastQuarter2010, 'base2010': baseCounter2010, 'hit2010': hitCounter2010,
                    'percent2010': percent2010})
      except Exception as e:
        util.PrintException(e)
    dfOut = pd.DataFrame(out)
    util.SaveMongoDB_DF(dfOut, 'stock_statistics2', 'dangerousQuarterRatio')
  
  def Show(codes):
    out = []
    for one in codes:
      try:
        df = util.LoadData('stock_statistics2', 'dangerousQuarterRatio', condition={'_id': one['_id']})
        df['name'] = one['name']
        out.append(df)
      except Exception as e:
        print(e)

    dfAll = out[0]
    for index in range(1, len(out)):
      dfAll = dfAll.append(out[index])
    print(dfAll)