#!/usr/bin/env python
# -*- encoding: utf-8 -*-

MONGODB_ID = '_id'
COMMON_ID = '季度'


class CWSJ_KEYWORD:
  ID_NAME = 'date'
  DB_NAME = 'stock'
  COLLECTION_HEAD = 'cwsj-'
  KEY_NAME = {
    "date": "季度",
    "jbmgsy": "基本每股收益(元)",
    "kfmgsy": "扣非每股收益(元)",
    "xsmgsy": "稀释每股收益(元)",
    "mgjzc": "每股净资产(元)",
    "mggjj": "每股公积金(元)",
    "mgwfply": "每股未分配利润(元)",
    "mgjyxjl": "每股经营现金流(元)",
    "yyzsr": "营业总收入(元)",
    "mlr": "毛利润(元)",
    "gsjlr": "归属净利润(元)",
    "kfjlr": "扣非净利润(元)",
    "yyzsrtbzz": "营业总收入同比增长(%)",
    "gsjlrtbzz": "归属净利润同比增长(%)",
    "kfjlrtbzz": "扣非净利润同比增长(%)",
    "yyzsrgdhbzz": "营业总收入滚动环比增长(%)",
    "gsjlrgdhbzz": "归属净利润滚动环比增长(%)",
    "kfjlrgdhbzz": "扣非净利润滚动环比增长(%)",
    "jqjzcsyl": "加权净资产收益率(%)",
    "tbjzcsyl": "摊薄净资产收益率(%)",
    "tbzzcsyl": "摊薄总资产收益率(%)",
    "mll": "毛利率(%)",
    "jll": "净利率(%)",
    "sjsl": "实际税率(%)",
    "yskyysr": "预收款/营业收入",
    "xsxjlyysr": "销售现金流/营业收入",
    "jyxjlyysr": "经营现金流/营业收入",
    "zzczzy": "总资产周转率(次)",
    "yszkzzts": "应收账款周转天数(天)",
    "chzzts": "存货周转天数(天)",
    "zcfzl": "资产负债率(%)",
    "ldzczfz": "流动负债/总负债(%)",
    "ldbl": "流动比率",
    "sdbl": "速动比率",

    'zgb': '总股本',
  }

  DATA_SUB = {}

  NEED_TO_NUMBER = {

    "jbmgsy": "0.2170",
    "kfmgsy": "0.1910",
    "xsmgsy": "0.2170",
    "mgjzc": "2.4372",
    "mggjj": "1.1088",
    "mgwfply": "0.2985",
    "mgjyxjl": "0.7548",
    "yyzsr": "938亿",
    "mlr": "228亿",
    "gsjlr": "75.7亿",
    "kfjlr": "66.8亿",
    "yyzsrtbzz": "36.15",
    "gsjlrtbzz": "301.99",
    "kfjlrtbzz": "53185.01",
    "yyzsrgdhbzz": "1.45",
    "gsjlrgdhbzz": "-7.91",
    "kfjlrgdhbzz": "-13.59",
    "jqjzcsyl": "9.25",
    "tbjzcsyl": "8.92",
    "tbzzcsyl": "3.41",
    "mll": "25.07",
    "jll": "8.38",
    "sjsl": "19.31",
    "yskyysr": "0.01",
    "xsxjlyysr": "1.10",
    "jyxjlyysr": "0.28",
    "zzczzy": "0.41",
    "yszkzzts": "60.84",
    "chzzts": "43.00",
    "zcfzl": "59.28",
    "ldzczfz": "32.76",
    "ldbl": "2.01",
    "sdbl": "1.83"
  }

  ADJUST_NAME = {
    '_id': '_id',
    "date": "季度",
    'QuarterProfit': '当季每股收益',
    'QuarterProfitRatio': '较一季度比例',
    'HalfYearProfitRatio': '较上半年比例',
    'ThreeQuarterProfitRatio': '较前三季度比例',

    'ForecastProfit': '预告基本每股收益',
    'ForecastQuarterProfit': '预告季度每股收益',
    'LastYearProfit': '上一年每股收益',
    'ForecastGrowthRate': '预测增长率',
    'ForecastMidGrowthRate': '预测中间增长率',
    'ForecastFinalGrowthRate': '预测最终增长率',
    'ForecastPerShareProfit': '每股收益预测',
    'PEMin': 'PE下限',
    'PEMax': 'PE上限',
    'ValueMin': '估值下限',
    'ValueMax': '估值上限',
    'LastYearROE': '上年净资产收益率(%)',
    'MarketValue': '市值',
    'ForcastPE': '目前PE(预)',
    'ForecastNow': '业绩预告（本期）',
    'ForecastNext': '业绩预告（下期）',
    'DistanceMin': '下限距离',
    'DistanceMax': '上限距离',

    #for output may not be right here
    'zgb': '总股本',
    'industry': '所属行业',
    'lastPrice': '目前价格',
    'code': '代码',
    'name': '名称',

    #for test,may not be right here
    'forecastl': '预计净利润下限',
    'djmgsy_jy': '当季每股收益-检验',
    'jyjdbl_jy': '较一季度比例-检验',
    'jsbnbl_jy': '较上半年比例-检验',
    'jqsjdbl_jy': '较前三季度比例-检验',

  }




class YJYG_KEYWORD:
  ID_NAME = 'scode'
  DB_NAME = 'stock'
  COLLECTION_HEAD = 'yjyg-'
  KEY_NAME = {
    "date": "季度",
    'scode': '代码',
    'sname': '名称',
    'hymc': '板块',
    'enddate': '截止日期',
    'forecastl': '预计净利润下限',
    'forecastt': '预计净利润上限',
    'increasel': '业绩变动幅度下限',
    'increaset': '业绩变动幅度上限',
    'forecastcontent': '业绩变动',
    'changereasondscrpt': '业绩变动原因',
    'forecasttype': '预告类型',
    'yearearlier': '上年同期净利润',
    'ndate': '公告日期',
    'sclx': '市场',
  }

  NEED_TO_NUMBER = {

    'forecastl': '预计净利润下限',
    'forecastt': '预计净利润上限',
    'increasel': '业绩变动幅度下限',
    'increaset': '业绩变动幅度上限',
    'yearearlier': '上年同期净利润',

  }

  DATA_SUB = {

    'enddate': '截止日期',
    'ndate': '公告日期',
  }
#########################################################
class YJBG_KEYWORD:
  ID_NAME = '季度'
  DB_NAME = 'stock'
  COLLECTION_HEAD = 'yjbg-'
  KEY_NAME = {
    "reportdate": "季度",
    'scode': '代码',
    'sname': '名称',
    'roeweighted': '净资产收益率',
    'totaloperatereve': '营业收入(元)',
    'xsmll': '销售毛利率',
    'parentnetprofit': '净利润(元)',
    'xsjll': '销售净利率',

  }

  NEED_TO_NUMBER = {
    'roeweighted': '净资产收益率',
    'totaloperatereve': '营业收入(元)',
    'xsmll': '销售毛利率',
    'parentnetprofit': '净利润(元)',
  }

  DATA_SUB = {
    "reportdate": "季度",
  }

#########################################################
class ZCFZ_KEYWORD:
  ID_NAME = '季度'
  DB_NAME = 'stock'
  COLLECTION_HEAD = 'zcfz-'
  KEY_NAME = {
    "reportdate": "季度",
    'scode': '代码',
    'sname': '名称',
    'zcfzl': '资产负债率',
    'inventory': '存货(元)',
    'accountrec': '应收账款(元)',
    'sumasset': '总资产(元)',
    'accountrec_sumasset': '应收账款/总资产',
    'inventory_sumasset': '存货/总资产',
    'accountrec_tb': '应收账款增长率',
    'inventory_tb': '存货增长率',
  }

  NEED_TO_NUMBER = {
    'zcfzl': '资产负债率',
    'inventory': '存货(元)',
    'accountrec': '应收账款(元)',
    'sumasset': '总资产(元)',
    'accountrec_tb': '应收账款增长率',
    'inventory_tb': '存货增长率',

  }

  DATA_SUB = {
    "reportdate": "季度",
  }

  NEED_TO_DECODE = {
    #"scode":"000636",
# "hycode":"016022",
# "companycode":"10000969",
# "sname":"风华高科",
# "publishname":"电子元件",
# "mkt":"szzb",
# "reporttimetypecode":"003",
# "combinetypecode":"001",
# "dataajusttype":"2",
# "noticedate":"2019-04-25T00:00:00",
# "reportdate":"2019-03-31T00:00:00",
# "eutime":"2019-04-24T20:35:43",
"sumasset":"&#xECD9;&#xECE9;&#xF137;&#xECE9;&#xE375;&#xE0D4;&#xF05A;&#xE0D4;&#xECEA;&#xEE3A;.&#xECEA;&#xE375;",
"fixedasset":"&#xECEA;&#xF05A;&#xF78F;&#xECD9;&#xF78F;&#xF05A;&#xECD9;&#xF05A;&#xEE3A;&#xECD9;.&#xE0D4;&#xE375;",
"monetaryfund":"&#xF05A;&#xECEA;&#xF05A;&#xF05A;&#xE0D4;&#xF137;&#xE375;&#xF137;&#xECD9;&#xF05A;.&#xECE9;&#xE0D4;",
"monetaryfund_tb":"&#xF05A;.&#xF05A;&#xE0D4;&#xE793;&#xE375;&#xE793;&#xE375;&#xF137;&#xECEA;&#xECE9;&#xF78F;&#xF05A;&#xF78F;&#xF05A;&#xF05A;",
"accountrec":"&#xECD9;&#xE375;&#xF05A;&#xEE3A;&#xECD9;&#xF78F;&#xEE3A;&#xF137;&#xF78F;.&#xF05A;&#xE793;",
"accountrec_tb":"-&#xF78F;.&#xF05A;&#xEE3A;&#xECEA;&#xE0D4;&#xF137;&#xECE9;&#xF78F;&#xECD9;&#xF137;&#xECEA;&#xF137;&#xECE9;&#xF05A;",
"inventory":"&#xF137;&#xF137;&#xE375;&#xECD9;&#xF05A;&#xEE3A;&#xE793;&#xE0D4;&#xF05A;.&#xF05A;&#xE375;",
"inventory_tb":"-&#xF78F;.&#xECEA;&#xF78F;&#xE375;&#xF137;&#xE793;&#xE375;&#xECD9;&#xE793;&#xE375;&#xECEA;&#xECEA;&#xF78F;&#xEE3A;&#xE793;&#xECEA;",
"sumliab":"&#xF05A;&#xF05A;&#xF05A;&#xE793;&#xE375;&#xECD9;&#xE375;&#xF78F;&#xE793;&#xE375;.&#xECE9;&#xE0D4;",
"accountpay":"-",
"accountpay_tb":"-",
"advancereceive":"&#xECEA;&#xECEA;&#xE375;&#xEE3A;&#xECD9;&#xECE9;&#xEE3A;&#xE0D4;.&#xF05A;&#xECD9;",
"advancereceive_tb":"-&#xF78F;.&#xECEA;&#xF05A;&#xEE3A;&#xF78F;&#xF137;&#xECD9;&#xF78F;&#xECE9;&#xE375;&#xEE3A;&#xF78F;&#xEE3A;&#xE793;&#xEE3A;&#xECE9;",
"sumshequity":"&#xEE3A;&#xE375;&#xE793;&#xEE3A;&#xF78F;&#xECEA;&#xF137;&#xECE9;&#xECE9;&#xE375;.&#xE793;&#xECE9;",
"sumshequity_tb":"&#xF78F;.&#xECEA;&#xECEA;&#xECEA;&#xECD9;&#xE0D4;&#xE793;&#xE0D4;&#xECEA;&#xEE3A;&#xE793;&#xECE9;&#xECE9;&#xF05A;&#xE375;&#xF05A;",
"tsatz":"&#xF78F;.&#xF05A;&#xECEA;&#xF78F;&#xE793;&#xE0D4;&#xE793;",
"tdetz":"-&#xF78F;.&#xECEA;&#xF05A;&#xECD9;&#xE0D4;&#xECD9;",
"ld":"&#xF78F;.&#xF78F;&#xE793;&#xE793;&#xEE3A;&#xECEA;&#xECD9;&#xF78F;&#xE375;",
"zcfzl":"&#xF78F;.&#xF05A;&#xECD9;&#xECEA;&#xECD9;&#xECEA;&#xECEA;&#xF137;&#xF78F;&#xECE9;&#xEE3A;",
"cashanddepositcbank":"-",
"cashanddepositcbank_tb":"-",
"loanadvances":"-",
"loanadvances_tb":"-",
"saleablefasset":"-",
"saleablefasset_tb":"-",
"borrowfromcbank":"-",
"borrowfromcbank_tb":"-",
"acceptdeposit":"-",
"acceptdeposit_tb":"-",
"sellbuybackfasset":"-",
"sellbuybackfasset_tb":"-",
"settlementprovision":"-",
"settlementprovision_tb":"-",
"borrowfund":"-",
"borrowfund_tb":"-",
"agenttradesecurity":"-",
"agenttradesecurity_tb":"-",
"premiumrec":"-",
"premiumrec_tb":"-",
"stborrow":"-",
"stborrow_tb":"-",
"premiumadvance":"-",
"premiumadvance_tb":"-"
                    }


#########################################################
class LRB_KEYWORD:
  ID_NAME = '季度'
  DB_NAME = 'stock'
  COLLECTION_HEAD = 'lrb-'
  KEY_NAME = {
    "reportdate": "季度",
    'scode': '代码',
    'sname': '名称',
    'sjlktz': '归属母公司股东的净利润同比增长率(扣除非经常性损益)',
    'tystz': '营业收入增长率',

  }

  NEED_TO_NUMBER = {
    'sjlktz': '归属母公司股东的净利润同比增长率(扣除非经常性损益)',
    'tystz': '营业收入增长率',

  }

  DATA_SUB = {
    "reportdate": "季度",
  }

  NEED_TO_DECODE = {

# "scode":"000636",
# "hycode":"016022",
# "companycode":"10000969",
# "sname":"风华高科",
# "publishname":"电子元件",
# "reporttimetypecode":"003",
# "combinetypecode":"001",
# "dataajusttype":"2",
# "mkt":"szzb",
# "noticedate":"2019-04-25T00:00:00",
# "reportdate":"2019-03-31T00:00:00",
# "eutime":"2019/4/24 20:35:49",
"parentnetprofit":"&#xEBED;&#xF275;&#xECEA;&#xEBED;&#xEBED;&#xE375;&#xE375;&#xF275;&#xF275;.&#xECE9;&#xF275;",
"totaloperatereve":"&#xECE9;&#xF275;&#xE793;&#xE793;&#xF275;&#xECEA;&#xEBC0;&#xE7A3;&#xEBC0;.&#xF275;&#xEBED;",
"totaloperateexp":"&#xF2FF;&#xECE9;&#xE793;&#xE793;&#xE268;&#xE793;&#xE7A3;&#xEBC0;&#xEBC0;.&#xE268;&#xE268;",
"totaloperateexp_tb":"-&#xE375;.&#xE375;&#xE7A3;&#xE268;&#xE793;&#xECEA;&#xECE9;&#xE375;&#xECEA;&#xECE9;&#xEBED;&#xEBC0;&#xE375;&#xE7A3;&#xECE9;&#xEBED;&#xE7A3;",
"operateexp":"&#xF275;&#xECE9;&#xF275;&#xEBC0;&#xE375;&#xEBC0;&#xE375;&#xECEA;&#xE7A3;.&#xF2FF;&#xF2FF;",
"operateexp_tb":"-&#xE375;.&#xE375;&#xE375;&#xEBED;&#xECEA;&#xE793;&#xE375;&#xE7A3;&#xEBC0;&#xF2FF;&#xE793;&#xEBC0;&#xE375;&#xECE9;&#xE268;&#xECE9;&#xE268;",
"saleexp":"&#xEBED;&#xF2FF;&#xECEA;&#xECEA;&#xECEA;&#xE375;&#xE375;&#xE268;.&#xE375;&#xF2FF;",
"manageexp":"&#xF275;&#xE793;&#xEBC0;&#xE268;&#xE793;&#xF2FF;&#xEBED;&#xF275;.&#xEBED;&#xECEA;",
"financeexp":"&#xE793;&#xECEA;&#xF275;&#xECEA;&#xE268;&#xE268;&#xEBED;.&#xE793;&#xEBC0;",
"operateprofit":"&#xEBED;&#xECE9;&#xECE9;&#xEBED;&#xF275;&#xEBED;&#xECEA;&#xECEA;&#xEBC0;.&#xE793;&#xF275;",
"sumprofit":"&#xEBED;&#xECE9;&#xECE9;&#xEBED;&#xECE9;&#xF275;&#xE793;&#xF275;&#xE375;.&#xEBED;&#xF275;",
"incometax":"&#xE793;&#xE268;&#xECE9;&#xF275;&#xE7A3;&#xECEA;&#xE375;&#xE793;.&#xECEA;&#xEBED;",
"operatereve":"-",
"intnreve":"-",
"intnreve_tb":"-",
"commnreve":"-",
"commnreve_tb":"-",
"operatetax":"&#xECE9;&#xEBED;&#xE7A3;&#xEBC0;&#xECEA;&#xE375;&#xF275;.&#xE268;&#xEBC0;",
"operatemanageexp":"-",
"commreve_commexp":"-",
"intreve_intexp":"-",
"premiumearned":"-",
"premiumearned_tb":"-",
"investincome":"-",
"surrenderpremium":"-",
"indemnityexp":"-",
"tystz":"&#xE375;.&#xE375;&#xEBED;&#xEBED;&#xE268;&#xEBC0;&#xF275;","yltz":"&#xE375;.&#xE7A3;&#xE7A3;&#xECE9;&#xEBED;&#xECEA;&#xE7A3;",
"sjltz":"&#xE375;.&#xE7A3;&#xF275;&#xEBED;&#xE375;&#xECEA;",
"kcfjcxsyjlr":"&#xEBED;&#xEBC0;&#xEBED;&#xF275;&#xE793;&#xF275;&#xECE9;&#xECEA;&#xEBC0;.&#xECEA;&#xECE9;",
"sjlktz":"&#xE375;.&#xE793;&#xE268;&#xF2FF;&#xE268;&#xF275;&#xEBED;&#xE793;&#xE7A3;&#xE793;&#xEBC0;",
"yyzc":"&#xF275;&#xECE9;&#xF275;&#xEBC0;&#xE375;&#xEBC0;&#xE375;&#xECEA;&#xE7A3;.&#xF2FF;&#xF2FF;"
  }


#########################################################
class XJLL_KEYWORD:
  ID_NAME = '季度'
  DB_NAME = 'stock'
  COLLECTION_HEAD = 'xjll-'
  KEY_NAME = {
    "reportdate": "季度",
    'scode': '代码',
    'sname': '名称',
    'netoperatecashflow': '经营性现金流量净额',


  }

  NEED_TO_NUMBER = {
    'netoperatecashflow': '经营性现金流量净额',

  }

  DATA_SUB = {
    "reportdate": "季度",
  }

  NEED_TO_DECODE = {
    # "scode":"000636",
# "hycode":"016022",
# "companycode":"10000969",
# "sname":"风华高科",
# "publishname":"电子元件",
#     "reporttimetypecode":"003",
# "combinetypecode":"001",
# "dataajusttype":"2",
# "mkt":"szzb",
# "noticedate":"2019-04-25T00:00:00",
# "reportdate":"2019-03-31T00:00:00",
# "eutime":"2019/4/24 20:36:09",
"nideposit":"-",
"nideposit_zb":"-",
"netoperatecashflow":"&#xE712;&#xECD9;&#xE5C1;&#xE8BC;&#xEA5D;&#xF78F;&#xF05A;&#xF275;&#xE7A3;.&#xE5C1;&#xE712;",
"netoperatecashflow_zb":"&#xE8BC;.&#xE7A3;&#xE5C1;&#xE8BC;&#xE7A3;&#xE712;&#xF275;&#xF137;&#xE8BC;&#xF275;&#xF137;&#xE7A3;&#xF78F;&#xEA5D;&#xF05A;",
"salegoodsservicerec":"&#xF05A;&#xE712;&#xF275;&#xE8BC;&#xE712;&#xE712;&#xE5C1;&#xF05A;&#xE8BC;.&#xECD9;&#xF275;",
"salegoodsservicerec_zb":"&#xECD9;&#xE712;.&#xF05A;&#xF275;&#xF137;&#xECD9;&#xF78F;&#xE5C1;&#xF05A;&#xE5C1;&#xF05A;&#xE8BC;&#xF275;&#xEA5D;&#xE8BC;",
"employeepay":"&#xE5C1;&#xECD9;&#xE5C1;&#xF05A;&#xF78F;&#xEA5D;&#xF05A;&#xE8BC;&#xE5C1;.&#xE7A3;&#xE5C1;",
"employeepay_zb":"&#xE712;&#xE712;.&#xE712;&#xE712;&#xE712;&#xE712;&#xECD9;&#xEA5D;&#xF05A;&#xE712;&#xE5C1;&#xF275;&#xF137;&#xF78F;&#xEA5D;",
"netinvcashflow":"-&#xE8BC;&#xECD9;&#xF275;&#xE8BC;&#xE712;&#xF05A;&#xF78F;&#xEA5D;.&#xE7A3;&#xE8BC;",
"netinvcashflow_zb":"-&#xE5C1;.&#xF05A;&#xE8BC;&#xE5C1;&#xF137;&#xECD9;&#xE712;&#xE712;&#xE5C1;&#xF275;&#xF137;&#xEA5D;&#xE5C1;&#xE7A3;&#xF137;",
"invincomerec":"&#xEA5D;&#xE7A3;&#xF275;&#xEA5D;&#xF137;&#xF05A;&#xE5C1;.&#xF05A;&#xE5C1;",
"invincomerec_zb":"&#xF78F;.&#xF137;&#xF05A;&#xE5C1;&#xF137;&#xF05A;&#xF137;&#xE712;&#xF78F;&#xE712;&#xECD9;&#xE5C1;&#xF78F;&#xECD9;&#xF78F;&#xE8BC;",
"buyfilassetpay":"&#xF275;&#xEA5D;&#xF275;&#xF78F;&#xF137;&#xE712;&#xE8BC;&#xF137;.&#xEA5D;&#xECD9;",
"buyfilassetpay_zb":"&#xF137;.&#xE8BC;&#xF78F;&#xF78F;&#xF78F;&#xECD9;&#xE5C1;&#xECD9;&#xF137;&#xE8BC;&#xF275;&#xE712;&#xF78F;&#xF137;&#xEA5D;",
"netfinacashflow":"-&#xE7A3;&#xE5C1;&#xEA5D;&#xF05A;&#xECD9;&#xF137;&#xF275;&#xF78F;.&#xE5C1;&#xF05A;",
"netfinacashflow_zb":"-&#xE5C1;.&#xECD9;&#xE712;&#xF05A;&#xECD9;&#xF05A;&#xE7A3;&#xEA5D;&#xECD9;&#xE5C1;&#xE7A3;&#xE7A3;&#xEA5D;&#xECD9;",
"nicashequi":"&#xE5C1;&#xE712;&#xEA5D;&#xE8BC;&#xE712;&#xF275;&#xF137;&#xE5C1;.&#xE5C1;&#xE7A3;",
"nicashequi_tb":"-&#xF78F;.&#xE8BC;&#xE8BC;&#xE8BC;&#xF78F;&#xE8BC;&#xE712;&#xE7A3;&#xE7A3;&#xF275;&#xF05A;&#xF275;&#xE712;&#xF05A;&#xE5C1;&#xF05A;",
"niclientdeposit":"-",
"niclientdeposit_zb":"-",
"niloanadvances":"-",
"niloanadvances_zb":"-",
"intandcommrec":"-",
"intandcommrec_zb":"-",
"agentuwsecurityrec":"-",
"invpay":"&#xE7A3;&#xE712;&#xECD9;&#xECD9;&#xF05A;&#xF137;&#xECD9;&#xE712;&#xF78F;.&#xE7A3;&#xE5C1;",
"invpay_zb":"&#xE5C1;&#xF137;.&#xE7A3;&#xF137;&#xF137;&#xF05A;&#xF275;&#xE5C1;&#xF275;&#xE5C1;&#xE8BC;&#xE5C1;&#xECD9;&#xE8BC;&#xF137;",
"cashequibeginning":"&#xE712;&#xE712;&#xE7A3;&#xF05A;&#xE5C1;&#xE712;&#xF05A;&#xF275;&#xF78F;&#xE712;.&#xEA5D;&#xF275;",
"cashequibeginning_zb":"&#xE7A3;&#xF137;.&#xF78F;&#xE5C1;&#xE7A3;&#xF78F;&#xE8BC;&#xE712;&#xF275;&#xE5C1;&#xF78F;&#xF137;&#xECD9;&#xF05A;&#xF137;",
"cashequiending":"&#xE712;&#xE712;&#xEA5D;&#xE712;&#xF78F;&#xEA5D;&#xE712;&#xECD9;&#xF137;&#xECD9;.&#xE712;&#xE5C1;",
"cashequiending_zb":"&#xE7A3;&#xECD9;.&#xF78F;&#xE5C1;&#xE7A3;&#xF78F;&#xE8BC;&#xE712;&#xF275;&#xE5C1;&#xF78F;&#xF137;&#xECD9;&#xF05A;&#xF137;",
"premiumrec":"-",
"premiumrec_zb":"-",
"indemnitypay":"-",
"indemnitypay_zb":"-"}


#########################################################
#由原始数据生成的数据放在这里
class EXTRA_KEYWORD:
  ID_NAME = '季度'
  DB_NAME = 'stock'
  COLLECTION_HEAD = 'extra-'
  KEY_NAME = {
    "reportdate": "季度",
    'yszkzzl_srzzl': '应收账款增长率/收入增长率',
    'chzzl_srzzl': '存货增长率/收入增长率',
    'jyhdcsdxjllje_yysr': '经营活动产生的现金流量净额/营业收入',
    'jyhdcsdxjllje_zgb': '每股经营现金流量',
  }

  NEED_TO_NUMBER = {


  }

  DATA_SUB = {
    "reportdate": "季度",
  }

  NEED_TO_DECODE = {
    }

#########################################################
class YYSJ_KEYWORD:
  ID_NAME = '季度'
  DB_NAME = 'stock'
  COLLECTION_HEAD = 'yysj-'
  KEY_NAME = {
    "reportdate": "季度",
  }

  NEED_TO_NUMBER = {


  }

  DATA_SUB = {
    "reportdate": "季度",
  }

  NEED_TO_DECODE = {
    }
#########################################################

class GPFH_KEYWORD:
  ID_NAME = 'Code'
  DB_NAME = 'stock'
  COLLECTION_HEAD = 'gpfh-'
  KEY_NAME = {
    'Code': '代码',
    'Name': '名称',
    'XJFH': '现金分红',
    'GXL': '股息率',
    'EarningsPerShare': '每股收益(元)',
    'NetAssetsPerShare': '每股净资产(元)',
    'MGGJJ': '每股公积金(元)',
    'MGWFPLY': '每股未分配利润(元)',
    'JLYTBZZ': '净利润同比增长(%)',
    'TotalEquity': '总股本(亿）',
    'YAGGR': '预案公告日',
    'GQDJR': '股权登记日',
    'CQCXR': '除权除息日',
    'ProjectProgress': '方案进度',
    'NoticeDate': '最新公告日期',
    'SZZBL': '送转总比例',
    'SGBL': '送股比例',
    'ZGBL': '转股比例',
    'AllocationPlan': '分配方案',

  }

  NEED_TO_NUMBER = {

    'XJFH': True,
    'GXL': True,
    'EarningsPerShare': True,
    'NetAssetsPerShare': True,
    'MGGJJ': True,
    'MGWFPLY': True,
    'JLYTBZZ': True,
    'TotalEquity': True,
    'SZZBL': True,
    'SGBL': True,
    'ZGBL': True,
  }

  DATA_SUB = {
    'YAGGR': 1,
    'GQDJR': 1,
    'CQCXR': 1,
    'ReportingPeriod': 1,
    'ResultsbyDate': 1,
    'NoticeDate': 1,
  }


class M012_KEYWORD:
  ID_NAME = '_id'
  DB_NAME = 'stock'
  COLLECTION_HEAD = 'macro-'

  KEY = 'abc'
  MX_TYPE = {

    'M0': 11,
    'M1': 10,
    'M2': 9,
  }

  KEY_NAME = {
    '_id': '月份',
    'data': '同比增幅',
  }



STOCK_LIST = {
  '000725',
  "000651",
  "002508",
  "600566",
  "600487",
  "300298",
  "300642",
  "603595",
  "603156",
  "603868",
  "002517",
  "603387",
  "600690",
  "300628",
  "002626",
  "002294",
  "002372",
  "002415",
  "603516",
  "002901",
  "000848",
  "002032",
  "603833",
  "603160",
  "002304",
  "600519",
  "300741",
  "603288",
  '000786',
  '603260',
  '600516',
}

TEST_STOCK_LIST = ['603516',
  # '000725',
  # "000651",
  # "002508",
                   ]



########################################################
class BASICS:
  KEY_NAME = {
    'code': '代码',
    'name': '名称',
    'industry': '所属行业',
    'area': '地区',
    'pe': '市盈率',
    'outstanding': '流通股本(亿)',
    'totals': '总股本(亿)',
    'totalAssets': '总资产(万)',
    'liquidAssets': '流动资产',
    'fixedAssets': '固定资产',
    'reserved': '公积金',
    'reservedPerShare': '每股公积金',
    'esp': '每股收益',
    'bvps': '每股净资',
    'pb': '市净率',
    'timeToMarket': '上市日期',
    'undp': '未分利润',
    'perundp': '每股未分配',
    'rev': '收入同比(%)',
    'profit': '利润同比(%)',
    'gpr': '毛利率(%)',
    'npr': '净利润率(%)',
    'holders': '股东人数',
  
    # http://www.szse.cn/api/report/index/companyGeneralization?
    # http://query.sse.com.cn/security/stock/queryCompanyStockStruct.do?
    'zgb': '总股本(股)'
  }
  
  DB_NAME = 'stock'
  COLLECTION_NAME = 'stock_list'


class HS300:
  KEY_NAME = {
    'code': '股票代码',
    'name': '股票名称',
    'date': '日期',
    'weight': '权重',
  }
  DB_NAME = 'stock'
  COLLECTION_NAME = 'hs300_stock_list'


class KData:
  DB_NAME = 'stock_kdata'
  COLLECTION_D_HEAD = ''
  COLLECTION_W_HEAD = 'w_'
  COLLECTION_M_HEAD = 'm_'


class HData:
  DB_NAME = 'stock_hdata'
