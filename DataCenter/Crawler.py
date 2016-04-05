import gevent.monkey
gevent.monkey.patch_all()

from bs4 import BeautifulSoup as bs
import requests
import gevent
from pymongo import MongoClient

requestHeaders = {'user-agent':"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"}
# connect to mongodb
client = MongoClient()
db = client.BillBoard
collection = db.BillBoardEveryDay

def CrawlBillBoardData(start,end):
    pass

def EMOverviewDataUrlGen(date):
    return "http://data.eastmoney.com/stock/lhb/" + date + ".html"

def GetBillBoardDataFromEM(date):
    r = requests.get(EMOverviewDataUrlGen(date), headers = requestHeaders)
    while(r.status_code != requests.codes.ok):
        r = requests.get(EMOverviewDataUrlGen(date), headers = requestHeaders)

    if(r.status_code == requests.codes.ok):
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
            if(len(cols) == 1): break
            doc = {}
            shouldGetDetail = False

            ## whether it is rowspan
            if(rowSpanCount > 0):
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
                if(heads[idx + idxOffset] == "序号" or 
                   heads[idx + idxOffset] == "相关链接" or
                   heads[idx + idxOffset] == "占总成交比例"):
                    continue
                doc[heads[idx + idxOffset]] = "".join(val.text.split())
            doc["日期"] = date
            stockCodes.add(doc["代码"])
            if doc["代码"] not in docs: 
                docs[doc["代码"]] = []
            docs[doc["代码"]].append(doc)

            if(rowSpanCount > 0): 
                rowSpanCount -= 1

        threads = [gevent.spawn(AsyncMergeBillBoardDetailDataToOverviewDataFromEM,docs[stockCode],date,stockCode) for stockCode in stockCodes]
        gevent.joinall(threads)

    else:
        print(r.status_code)
 
def AsyncMergeBillBoardDetailDataToOverviewDataFromEM(docs,date,stockCode):
    print("get"+stockCode)
    detailDocs = GetBillBoardDetailDataFromEM(date,stockCode)
    print("get finish")
    for i in range(len(detailDocs)):
        detailDoc = detailDocs[i]
        targetDoc = None
        for doc in docs:
            if detailDoc["上榜原因"] == doc["上榜原因"]:
                targetDoc = doc
        for key in detailDoc:
            targetDoc[key] = detailDoc[key]
        collection.insert_one(targetDoc)
             
def EMDetailviewDataUrlGen(date,stockCode):
    return "http://data.eastmoney.com/stock/lhb," + date + "," + stockCode + ".html"

def GetBillBoardDetailDataFromEM(date,stockCode):
    r = requests.get(EMDetailviewDataUrlGen(date,stockCode),headers = requestHeaders)
    while r.status_code != requests.codes.ok:
        r = requests.get(EMDetailviewDataUrlGen(date,stockCode),headers = requestHeaders)
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
            tips = divtips[i].find_all("li")
            tip1 = tips[0].text.replace("：",":").split(":")
            doc["上榜原因" if tip1[0] == "类型" else tip1[0]] = tip1[1] 

            for item in tips[1].text.replace("：",":").split():
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
            doc['日期'] = date
            doc['代码'] = stockCode
            docs.append(doc)
    else:
        print(r.status_code)
    
    return docs