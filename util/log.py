# -*- coding: utf-8 -*-

import logging



import util
import const

"""
import logging

logger= logging.getLogger( "module_name" )
logger_a = logger.getLogger( "module_name.function_a" )
logger_b = logger.getLogger( "module_name.function_b" )

def function_a( ... ):
    logger_a.debug( "a message" )

def functio_b( ... ):
    logger_b.debug( "another message" )

if __name__ == "__main__":
    logging.basicConfig( stream=sys.stderr, level=logging.DEBUG )
    logger_a.setLevel( logging.DEBUG )
    logger_b.setLevel( logging.WARN )
"""
#=======================================================
DEBUG = logging.DEBUG
INFO = logging.INFO
WARN = logging.WARNING
ERROR = logging.ERROR
FATAL = logging.CRITICAL

def Init(processName, propFile):
  fileDir = r'c:\ctp-py-log'

  if propFile != None:
    content = util.ReadJsonProp(propFile)
    fileDir = content["log_path"]

  midDir = util.TodayString() + '-' + processName
  fileName = util.NowTimeString() + '.log'
  util.CreateDir(fileDir + '/' + midDir)
  fileName = fileDir + '/' + midDir + '/' + fileName
  print("the log file process and name " + processName + " " + fileName)

  # logging.basicConfig(filename = fileName, level = logging.DEBUG)

  format =  logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
  fh = logging.FileHandler(fileName)
  fh.setLevel(logging.INFO)
  fh.setFormatter(format)

  ch = logging.StreamHandler()
  ch.setLevel(logging.DEBUG)
  ch.setFormatter(format)

  __currentLogger = logging.getLogger()
  __currentLogger.setLevel(logging.DEBUG)
  __currentLogger.addHandler(ch)
  __currentLogger.addHandler(fh)

  __currentLogger.info("the py version: " + const.VERSION)
  __currentLogger.debug("test debug output")
  __currentLogger.warning("test warn output %d", 123)
  __currentLogger.error("test error output  %d: %s" % (123, "456"))
  __currentLogger.critical("test critical output")

  #AddFilter("not allow debug")


__currentLogger = None

def GetLogger(name):
  __currentLogger = logging.getLogger(name)
  return __currentLogger


 #很多工具类模块，不关心当前的log是什么，只要用。所以
def current():
  if __currentLogger != None:
    return __currentLogger
  else:
    return GetLogger('Anonymous')


def SetLevel(l):
  logging.setLevel(l)
