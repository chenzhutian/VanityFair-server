#!/usr/bin/env python
# -*- coding: utf-8 -*-
import gevent.monkey
gevent.monkey.patch_all()
import requests
import gevent
import re
import json
from pymongo import MongoClient
from bs4 import BeautifulSoup as bs
from datetime import timedelta, date, datetime

requestHeaders = {'user-agent':"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) \
                                Chrome/41.0.2228.0 Safari/537.36"}
LOGFILE = "crawlerLogfile.log"
LogToFile = open(LOGFILE, 'a')
RETRIESNUMLIMIT = 5


def EMOverviewDataUrlGen(date):
    return "http://data.eastmoney.com/stock/lhb/" + date.strftime("%Y-%m-%d") + ".html"

def EMDetailDataUrlGen(date, stockCode):
    return "http://data.eastmoney.com/stock/lhb," + date.strftime("%Y-%m-%d") + "," + stockCode + \
            ".html"

def GetOverviewDataFromEM(date, collection):
    r = requests.get(EMOverviewDataUrlGen(date), headers = requestHeaders)
    retriesNum = 1
    while r.status_code != requests.codes.ok:
        if retriesNum < RETRIESNUMLIMIT:
            r = requests.get(EMOverviewDataUrlGen(date), headers = requestHeaders)
            retriesNum += 1
        else:
            json.dump({"date":date.strftime("%Y-%m-%d"),"statusCode":r.status_code,
                       "time":datetime.now()}, LogToFile)
            return False
            
    soup = bs(r.text,"lxml")
    # get table data
    table = soup.find(id="dt_1")
    heads = ["序号","代码","名称","相关链接","涨跌幅","龙虎榜成交额","买入额","占总成交比例","卖出额",
             "占总成交比例","上榜原因"]
    # get table body
    tBody = table.find("tbody")
    rowSpanData = []
    rowSpanCount = 0
    docs = {}
    stockCodes = set()
    for row in tBody.find_all("tr",class_="all"):
        cols = row.find_all('td')
        if len(cols) == 1 : break
        doc = {}
        shouldGetDetail = False

        ## whether it is rowspan
        if rowSpanCount > 0 :
            for idx,val in enumerate(rowSpanData):
                if idx == 0 or idx == 3:
                    continue
                doc[heads[idx]] = val
        else:
            rowSpanData = []
            rowSpanCount = 0
            shouldGetDetail = True

        ## get data from each columns
        idxOffset = len(rowSpanData)
        filterCols = {0,3,5,6,7,8,9}
        for idx,val in enumerate(cols):
            # set row Span
            if val.has_attr("rowspan"): 
                rowSpanCount = int(val["rowspan"])
                rowSpanData.append("".join(val.text.split()))
                
            if  idx + idxOffset in filterCols:    
                continue
            doc[heads[idx + idxOffset]] = "".join(val.text.split())
        doc["日期"] = date.strftime("%Y-%m-%d")
        stockCodes.add(doc["代码"])
        if doc["代码"] not in docs: 
            docs[doc["代码"]] = []
        docs[doc["代码"]].append(doc)

        if(rowSpanCount > 0): 
            rowSpanCount -= 1

    threads = [gevent.spawn(AsyncMergeDetailDataToOverviewDataFromEM,docs[stockCode],date,stockCode,
                            collection) for stockCode in stockCodes]

    gevent.joinall(threads)
    return True

def AsyncMergeDetailDataToOverviewDataFromEM(docs,date,stockCode,collection):
    detailDocs = GetDetailDataFromEM(date,stockCode)
    if detailDocs is not None:
        for i in range(len(detailDocs)):
            detailDoc = detailDocs[i]
            targetDoc = None
            for doc in docs:
                if detailDoc["上榜原因"] == doc["上榜原因"]:
                    targetDoc = doc
            for key in detailDoc:
                targetDoc[key] = detailDoc[key]
            targetDoc["涨跌幅"] = targetDoc["涨跌幅"][:-1]
            if "收盘价" in targetDoc: del targetDoc["收盘价"]
            collection.insert_one(targetDoc)

def GetDetailDataFromEM(date,stockCode):

    r = requests.get(EMDetailDataUrlGen(date,stockCode),headers = requestHeaders)
    retriesNum = 1
    while r.status_code != requests.codes.ok:
        if retriesNum < RETRIESNUMLIMIT:
            r = requests.get(EMDetailDataUrlGen(date,stockCode),headers = requestHeaders)
            retriesNum += 1
        else:
            json.dump({"date":date.strftime("%Y-%m-%d"),"stockCode":stockCode,
                       "statusCode":r.status_code,"time":datetime.now()}, LogToFile)
            return None
    
    docs = []
    soup = bs(r.text,"lxml") 
    # get table data
    targetDiv = soup.find(id="cont1")
    tables = targetDiv.find_all("table")
    divtips = targetDiv.find_all("div",class_="divtips")
    for i  in range(len(divtips)):
        doc = {}
        ## get tips data
        tiplis = divtips[i].find_all("li")
        tipli = tiplis[0].text.replace("：",":").split(":")
        doc["类型" if tipli[0] == "上榜原因" else tipli[0]] = tipli[1] 

        if len(tiplis) > 1:
            for item in tiplis[1].text.replace("：",":").split():
                item = item.split(":")
                if item[0] == "成交金额":
                    doc["成交额"] = item[1][:-2]
                elif item[0] == "成交量":
                    doc[item[0]] = item[1][:-2]
                        
        ## get table head
        table = tables[i * 2]
        heads = ["序号","单位名称","买入额","占总成交比例","卖出额","占总成交比例","净额"]
        ## get buyer table body
        tBody = table.find("tbody")
        buyers = []
        filterCols = {0,3,5,6}
        for index,row in tBody.find_all("tr"):
            cols = row.find_all('td')
            if(len(cols) == 1): break
                
            oneBuyer = {}
            for idx,val in enumerate(cols):
                if idx in filterCols :
                    continue
                oneBuyer[heads[idx]] = "".join(val.text.split())
            buyers.append(oneBuyer)
        doc["buyers"] = buyers

        ## get seller table body
        tBody = tables[i * 2 + 1].find("tbody")
        sellers = []
        filterCols = {0,3,5,6}
        for row in tBody.find_all("tr"):
            cols = row.find_all('td')
            if(len(cols) == 1): break
                
            oneSeller = {}
            for idx,val in enumerate(cols):
                if idx in filterCols :
                    continue
                oneSeller[heads[idx]] = "".join(val.text.split())
            sellers.append(oneSeller)
        doc["sellers"] = sellers
        docs.append(doc)
    return docs
