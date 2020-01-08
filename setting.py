#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import sys


def currentOS():
  if sys.platform == 'linux':
    return 'linux'
  else:
    return 'win'

CHROME_DRIVER_PATH = r'/home/ken/prog/chromedriver_linux64/chromedriver' if sys.platform == 'linux' else \
  ''
PHANTOMJS_PATH = r'/home/ken/prog/phantomjs-2.1.1-linux-x86_64/bin/phantomjs'  if sys.platform == 'linux' else \
  ''