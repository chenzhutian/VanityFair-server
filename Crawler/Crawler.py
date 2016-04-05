#!/usr/bin/env python
# -*- coding: utf-8 -*-
import gevent.monkey
gevent.monkey.patch_all()

from pymongo import MongoClient
from bs4 import BeautifulSoup as bs
from datetime import timedelta, date
from UrlGen import *
import requests
import gevent
import re
import json
# 日期 代码 名称 上榜原因 收盘价 涨跌幅 成交量 成交额 买入s：[代码 日期 单位名称,买入额，卖出额]  卖出s:[代码 日期 单位名称,买入额，卖出额]
requestHeaders = {'user-agent':"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"}

def GetOverviewDataFromQQ(date,collection):
    r = requests.get(QQOverviewDataUrlGen(date),headers = requestHeaders)
    while(r.status_code != requests.codes.ok):
        r = requests.get(QQOverviewDataUrlGen(date),headers = requestHeaders)

    if r.status_code == requests.codes.ok:
        result = re.sub('([{,])([^{:\s"]*):', lambda m: '%s"%s":'%(m.group(1),m.group(2)),r.text[14:-2])
        result = json.loads(result)
        heads = ["日期","代码","名称","上榜原因","detailCode","收盘价","涨跌幅"]
        docs = {}
        stockDetailCodes = set()
        for data in result['_datas']:
            doc = {}
            for i in range(len(data)):
                doc[heads[i]] = data[i]
            docs[doc["代码"]+"_"+doc["detailCode"]] = doc
            stockDetailCodes.add(doc["代码"]+"_"+doc["detailCode"])
        
        threads = []
        for stockDetailCode in stockDetailCodes:
            stockCode = stockDetailCode.split("_")[0]
            detailCode = stockDetailCode.split("_")[1]
            threads.append(gevent.spawn(AsyncMergeDetailDataToOverviewDataFromQQ,docs[stockDetailCode],date,stockCode,detailCode,collection))
            break
        gevent.joinall(threads)
        #print(GetDetailDataFromQQ(date,"000019","070003"))
    else:
        print(r.status_code)

def AsyncMergeDetailDataToOverviewDataFromQQ(doc,date,stockCode,detailCode,collection):
    detailDoc = GetDetailDataFromQQ(date,stockCode,detailCode)
    for key in detailDoc:
        doc[key] = detailDoc[key]
    del doc[detailCode]
    collection.insert_one(doc)

def GetDetailDataFromQQ(date,stockCode,detailCode):
    r = requests.get(QQDetailDataUrlGen(date,stockCode,detailCode),headers = requestHeaders)
    while(r.status_code != requests.codes.ok):
        r = requests.get(QQDetailDataUrlGen(date,stockCode,detailCode),headers = requestHeaders)
    
    if r.status_code == requests.codes.ok:
        result = re.sub('([{,])([^{:\s"]*):', lambda m: '%s"%s":'%(m.group(1),m.group(2)),r.text[16:-2])
        result = json.loads(result)
        heads = ["代码","名称","BorS","nouse","日期","单位名称","买入额","卖出额"]
        detailDoc = {"成交额":result["_cje"],"成交量":result["_cjl"]}
        buyers = []
        sellers = []
        for data in result["_datas"]:
            someone = {}
            for i in range(len(data)):
                if(heads[i] == "名称" or
                   heads[i] == "nouse"):
                    continue
                someone[heads[i]] = data[i]
            if someone["BorS"] == "B":
                buyers.append(someone)
            else:
                sellers.append(someone)
            del someone["BorS"]
        detailDoc["buyers"] = buyers
        detailDoc["sellers"] = sellers

        return detailDoc

def CrawlBillBoardData(start,end):
    # connect to mongodb
    client = MongoClient()
    db = client.BillBoard
    collection = db.BillBoardEveryDay
    current = start
    span = 10
    while current > end:
        threads = []
        i = 0
        while i < span:
            current -= timedelta(1)
            print(current.strftime("%Y-%m-%d"))
            if current.weekday() < 5:
                threads.append(gevent.spawn(GetOverviewDataFromEM,current,collection))
            i += 1
        gevent.joinall(threads)
         
def GetOverviewDataFromEM(date,collection):
    r = requests.get(EMOverviewDataUrlGen(date), headers = requestHeaders)
    while(r.status_code != requests.codes.ok):
        r = requests.get(EMOverviewDataUrlGen(date), headers = requestHeaders)

    if r.status_code == requests.codes.ok:
        #print(r.text)
        soup = bs(r.text,"lxml")
        # get table data
        table = soup.find(id="dt_1")
        # get table heads
        tHead = table.find("thead")
        headRow = tHead.find("tr")
        headNames = headRow.find_all("th")
        heads = ["".join(ele.text.split()) for ele in headNames]
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
                    if(heads[idx] == "序号" or heads[idx] == "相关链接"):
                        continue
                    doc[heads[idx]] = val
            else:
                rowSpanData = []
                rowSpanCount = 0
                shouldGetDetail = True

            ## get data from each columns
            idxOffset = len(rowSpanData)
            for idx,val in enumerate(cols):
                # set row Span
                if val.has_attr("rowspan"): 
                    rowSpanCount = int(val["rowspan"])
                    rowSpanData.append("".join(val.text.split()))
                if(heads[idx + idxOffset] == "序号" or heads[idx + idxOffset] == "相关链接" or heads[idx + idxOffset] == "占总成交比例"):
                    continue
                doc[heads[idx + idxOffset]] = "".join(val.text.split())
            doc["日期"] = date.strftime("%Y-%m-%d")
            stockCodes.add(doc["代码"])
            if doc["代码"] not in docs: 
                docs[doc["代码"]] = []
            docs[doc["代码"]].append(doc)

            if(rowSpanCount > 0): 
                rowSpanCount -= 1

        threads = [gevent.spawn(AsyncMergeDetailDataToOverviewDataFromEM,docs[stockCode],date,stockCode,collection) for stockCode in stockCodes]
        gevent.joinall(threads)

    else:
        print(r.status_code)
 
def AsyncMergeDetailDataToOverviewDataFromEM(docs,date,stockCode,collection):
    detailDocs = GetDetailDataFromEM(date,stockCode)
    for i in range(len(detailDocs)):
        detailDoc = detailDocs[i]
        targetDoc = None
        for doc in docs:
            if detailDoc["上榜原因"] == doc["上榜原因"]:
                targetDoc = doc
        for key in detailDoc:
            targetDoc[key] = detailDoc[key]
        collection.insert_one(targetDoc)

def GetDetailDataFromEM(date,stockCode):
    r = requests.get(EMDetailDataUrlGen(date,stockCode),headers = requestHeaders)
    while r.status_code != requests.codes.ok:
        r = requests.get(EMDetailDataUrlGen(date,stockCode),headers = requestHeaders)
    docs = []
    if(r.status_code == requests.codes.ok):
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
            doc["上榜原因" if tipli[0] == "类型" else tipli[0]] = tipli[1] 

            if len(tiplis) > 1:
                for item in tiplis[1].text.replace("：",":").split():
                    item = item.split(":")
                    doc[item[0]] = item[1]
  
            ## get table head
            table = tables[i * 2]
            headRow = table.find("thead").find_all("tr")[1]
            headNames = headRow.find_all("th")
            heads = ["".join(ele.text.split()) for ele in headNames]
            ## get buyer table body
            tBody = table.find("tbody")
            buyers = {}
            for index,row in enumerate(tBody.find_all("tr")):
                cols = row.find_all('td')
                if(len(cols) == 1): break
                
                oneBuyer = {}
                for idx,val in enumerate(cols):
                    # set row Span
                    if(heads[idx] == "序号"):continue
                    oneBuyer[heads[idx]] = "".join(val.text.split())
                buyers["b" + str(index)] = oneBuyer
            doc["buyers"] = buyers

            ## get seller table body
            tBody = tables[i * 2 + 1].find("tbody")
            sellers = {}
            rows = tBody.find_all("tr")
            rowsCount = len(rows)
            for index in range(rowsCount - 1):
                row = rows[index]
                cols = row.find_all('td')
                if(len(cols) == 1): break
                
                oneSeller = {}
                for idx,val in enumerate(cols):
                    # set row Span
                    if(heads[idx] == "序号"):continue
                    oneSeller[heads[idx]] = "".join(val.text.split())
                sellers["s" + str(index)] = oneSeller
            doc["sellers"] = sellers

            ## get statistics data from last row
            #for idx,val in enumerate(rows[rowsCount - 1].find_all('td')):
            #    if(idx == 0):continue
            #    doc[heads[idx + 1]] = "".join(val.text.split())
            doc['日期'] = date.strftime("%Y-%m-%d")
            doc['代码'] = stockCode
            docs.append(doc)
    else:
        print(r.status_code)
    
    return docs

if __name__ == "__main__":
    #start = date(2016,4,4)
    #end = date(2013,1,1)
    #CrawlBillBoardData(start,end)
    GetOverviewDataFromQQ(date(2015,11,10),None)