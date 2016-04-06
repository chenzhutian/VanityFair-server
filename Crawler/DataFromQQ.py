#!/usr/bin/env python
# -*- coding: utf-8 -*-
import gevent.monkey
gevent.monkey.patch_all()
import requests
import gevent
import re
import json
from bs4 import BeautifulSoup as bs
from datetime import timedelta, date, datetime
from UrlGen import *

# 日期 代码 名称 上榜原因 收盘价 涨跌幅 成交量 成交额 买入s：[代码 日期 单位名称,买入额，卖出额] 卖出s:[代码 日期
# 单位名称,买入额，卖出额]
requestHeaders = {'user-agent':"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"}
LOGFILE = "crawlerLogfile.log"
LogToFile = open(LOGFILE,'a')
RETRIESNUMLIMIT = 5

def QQOverviewDataUrlGen(startDate,endDate):
    return "http://stock.finance.qq.com/cgi-bin/sstock/q_lhb_js?t=2&b=" + startDate.strftime("%Y%m%d") + "&e=" + endDate.strftime("%Y%m%d") + "&ol=6"

def QQDetailDataUrlGen(date,stockCode,detailCode):
    if type(date) is str:
        date = "".join(date.split("-"))
    else:
        date = date.strftime("%Y%m%d")
    return "http://stock.finance.qq.com/cgi-bin/sstock/q_lhb_xx_js?c=" + stockCode + "&b=" + date + "&l=" + detailCode

def GetOverviewDataFromQQ(startDate,endDate, collection):
    r = requests.get(QQOverviewDataUrlGen(startDate,endDate),headers = requestHeaders)
    retriesNum = 1
    while r.status_code != requests.codes.ok:
        if retriesNum < RETRIESNUMLIMIT:
            r = requests.get(QQOverviewDataUrlGen(startDate,endDate),headers = requestHeaders)
            retriesNum += 1
        else:
            json.dump({"date":startDate.strftime("%Y-%m-%d"),"statusCode":r.status_code,"time":datetime.now()},LogToFile)
            return False

    result = re.sub('([{,])([^{:\s"]*):', lambda m: '%s"%s":' % (m.group(1),m.group(2)),r.text[14:-2])
    try:
        result = json.loads(result)
    except:
        json.dump({"date":startDate.strftime("%Y-%m-%d"),"statusCode":r.status_code,"time":datetime.now()},LogToFile)
        return False
    
    heads = ["日期","代码","名称","上榜原因","detailCode","收盘价","涨跌幅"]
    docs = {}
    stockDetailCodes = set()
    for data in result['_datas']:
        doc = {}
        for i in range(len(data)):
            if i == 5: # head[i] == "收盘价"
                continue
            doc[heads[i]] = data[i]
        docs[doc["代码"] + "_" + doc["detailCode"]] = doc
        stockDetailCodes.add(doc["代码"] + "_" + doc["detailCode"])
        
    threads = []
    for stockDetailCode in stockDetailCodes:
        stockCode = stockDetailCode.split("_")[0]
        detailCode = stockDetailCode.split("_")[1]
        threads.append(gevent.spawn(AsyncMergeDetailDataToOverviewDataFromQQ,docs[stockDetailCode],docs[stockDetailCode]["日期"],stockCode,detailCode,collection))
            
    gevent.joinall(threads)
    print(startDate.strftime("%Y-%m-%d"),endDate.strftime("%Y-%m-%d"))
    return True

def AsyncMergeDetailDataToOverviewDataFromQQ(doc,date,stockCode,detailCode,collection):
    detailDoc = GetDetailDataFromQQ(date,stockCode,detailCode)
    if detailDoc is not None:
        for key in detailDoc:
            doc[key] = detailDoc[key]
        del doc["detailCode"]
        collection.insert_one(doc)

def GetDetailDataFromQQ(date,stockCode,detailCode):
    
    r = requests.get(QQDetailDataUrlGen(date,stockCode,detailCode),headers = requestHeaders)
    retriesNum = 1
    while(r.status_code != requests.codes.ok):
        if retriesNum < RETRIESNUMLIMIT:
            r = requests.get(QQDetailDataUrlGen(date,stockCode,detailCode),headers = requestHeaders)
            retriesNum += 1
        else:
            json.dump({"date":date.strftime("%Y-%m-%d"),"stockCode":stockCode,"detailCode":detailCode,"statusCode":r.status_code,"time":datetime.now()},LogToFile)
            return None

    result = re.sub('([{,])([^{:\s"]*):', lambda m: '%s"%s":' % (m.group(1),m.group(2)),r.text[16:-2])
    try:
        result = json.loads(result)
    except:
        json.dump({"date":date.strftime("%Y-%m-%d"),"statusCode":r.status_code,"time":datetime.now()},LogToFile)
        return False

    heads = ["代码","名称","BorS","nouse","日期","单位名称","买入额","卖出额"]
    try:
        _cje = round(float(result["_cje"]) / 10000,2)
    except ValueError:
        _cje = "--" 
    detailDoc = {"成交额":_cje,"成交量":result["_cjl"]}
    buyers = []
    sellers = []
    filterCols = {0,1,3,4}
    for data in result["_datas"]:
        someone = {}
        for i in range(len(data)):
            if i in filterCols: #if(heads[i] == "名称" or heads[i] == "nouse"):
                continue
            if i == 6 or i == 7:
                try:
                    someone[heads[i]] = round(float(data[i]) / 10000,2)
                except ValueError:
                    someone[heads[i]] = "--"
            else:
                someone[heads[i]] = data[i]
        if someone["BorS"] == "B":
            buyers.append(someone)
        else:
            sellers.append(someone)
        del someone["BorS"]
    detailDoc["buyers"] = buyers
    detailDoc["sellers"] = sellers

    return detailDoc

def CrawlBillBoardData(start,end,collection):
    current = start
    nextCurrent = start
    span = 5
    while current > end:
        nextCurrent -= timedelta(span)
        GetOverviewDataFromQQ(nextCurrent,current,collection)
        current = nextCurrent - timedelta(1)
    LogToFile.close()
   