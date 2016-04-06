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
from datetime import timedelta, date,datetime

requestHeaders = {'user-agent':"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"}
LOGFILE = "crawlerLogfile.log"
LogToFile = open(LOGFILE,'a')
RETRIESNUMLIMIT = 5


def EMOverviewDataUrlGen(date):
    return "http://data.eastmoney.com/stock/lhb/" + date.strftime("%Y-%m-%d") + ".html"

def EMDetailDataUrlGen(date,stockCode):
    return "http://data.eastmoney.com/stock/lhb," + date.strftime("%Y-%m-%d") + "," + stockCode + ".html"

def GetOverviewDataFromEM(date,collection):
    r = requests.get(EMOverviewDataUrlGen(date), headers = requestHeaders)
    retriesNum = 1
    while(r.status_code != requests.codes.ok):
        if retriesNum < RETRIESNUMLIMIT:
            r = requests.get(EMOverviewDataUrlGen(date), headers = requestHeaders)
            retriesNum += 1
        else:
            json.dump({"date":date.strftime("%Y-%m-%d"),"statusCode":r.status_code,"time":datetime.now()},LogToFile)
            return False
            
    soup = bs(r.text,"lxml")
    # get table data
    table = soup.find(id="dt_1")
    heads = ["????","????","????","????????","?ǵ???","???????ɽ??????","?????????","ռ?ܳɽ?????","?????????","ռ?ܳɽ?????","?ϰ?ԭ??"]
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
                if idx == 0 or idx == 3: #if(heads[idx] == "????" or heads[idx] == "????????"):
                    continue
                doc[heads[idx]] = val
        else:
            rowSpanData = []
            rowSpanCount = 0
            shouldGetDetail = True

        ## get data from each columns
        idxOffset = len(rowSpanData)
        filterCols = {0,3,5,6,7,8,9}    # "????"??"????????","ռ?ܳɽ?????"
        for idx,val in enumerate(cols):
            # set row Span
            if val.has_attr("rowspan"): 
                rowSpanCount = int(val["rowspan"])
                rowSpanData.append("".join(val.text.split()))
                
            if  idx + idxOffset in filterCols:    
                continue
            doc[heads[idx + idxOffset]] = "".join(val.text.split())
        doc["????"] = date.strftime("%Y-%m-%d")
        stockCodes.add(doc["????"])
        if doc["????"] not in docs: 
            docs[doc["????"]] = []
        docs[doc["????"]].append(doc)

        if(rowSpanCount > 0): 
            rowSpanCount -= 1

    threads = [gevent.spawn(AsyncMergeDetailDataToOverviewDataFromEM,docs[stockCode],date,stockCode,collection) for stockCode in stockCodes]
    # FOR TEST
    #threads = [gevent.spawn(AsyncMergeDetailDataToOverviewDataFromEM,docs[targetStockCode],date,targetStockCode,collection)]
    gevent.joinall(threads)
    return True

def AsyncMergeDetailDataToOverviewDataFromEM(docs,date,stockCode,collection):
    detailDocs = GetDetailDataFromEM(date,stockCode)
    if detailDocs is not None:
        for i in range(len(detailDocs)):
            detailDoc = detailDocs[i]
            targetDoc = None
            for doc in docs:
                if detailDoc["?ϰ?ԭ??"] == doc["?ϰ?ԭ??"]:
                    targetDoc = doc
            for key in detailDoc:
                targetDoc[key] = detailDoc[key]
            targetDoc["?ǵ???"] = targetDoc["?ǵ???"][:-1]
            if "???̼?" in targetDoc: del targetDoc["???̼?"]
            collection.insert_one(targetDoc)

def GetDetailDataFromEM(date,stockCode):

    r = requests.get(EMDetailDataUrlGen(date,stockCode),headers = requestHeaders)
    retriesNum = 1
    while r.status_code != requests.codes.ok:
        if retriesNum < RETRIESNUMLIMIT:
            r = requests.get(EMDetailDataUrlGen(date,stockCode),headers = requestHeaders)
            retriesNum += 1
        else:
            json.dump({"date":date.strftime("%Y-%m-%d"),"stockCode":stockCode,"statusCode":r.status_code,"time":datetime.now()},LogToFile)
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
        tipli = tiplis[0].text.replace("??",":").split(":")
        doc["?ϰ?ԭ??" if tipli[0] == "????" else tipli[0]] = tipli[1] 

        if len(tiplis) > 1:
            for item in tiplis[1].text.replace("??",":").split():
                item = item.split(":")
                if item[0] == "?ɽ?????":
                    doc["?ɽ???"] = item[1][:-2]
                elif item[0] == "?ɽ???":
                    doc[item[0]] = item[1][:-2]
                        
        ## get table head
        table = tables[i * 2]
        heads = ["????","??λ????","??????","ռ?ܳɽ?????","??????","ռ?ܳɽ?????","????"]
        ## get buyer table body
        tBody = table.find("tbody")
        buyers = []
        filterCols = {0,3,5,6}
        for index,row in tBody.find_all("tr"):
            cols = row.find_all('td')
            if(len(cols) == 1): break
                
            oneBuyer = {}   #{"????":date.strftime("%Y-%m-%d"), "????":stockCode}
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
                
            oneSeller = {}  #{"????":date.strftime("%Y-%m-%d"), "????":stockCode}
            for idx,val in enumerate(cols):
                if idx in filterCols :
                    continue
                oneSeller[heads[idx]] = "".join(val.text.split())
            sellers.append(oneSeller)
        doc["sellers"] = sellers
        docs.append(doc)
    return docs
