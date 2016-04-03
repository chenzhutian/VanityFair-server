from bs4 import BeautifulSoup as bs
import requests
from pymongo import MongoClient

def THSDetailDataUrlGen(stockCode,date):
    return "http://data.10jqka.com.cn/market/lhbcjmx/code/"+stockCode+"/date/"+date+"/ajax/1"

def GetBillBoardDetailDataFromTHS(stockCode, date):
    pass

def THSOverviewDataUrlGen(date,page = 1):
    return "http://data.10jqka.com.cn/market/longhu/date/"+date+"/ajax/1/page/"+ str(page)

def GetBillBoardDataFromTHSOn(date = None):
    # connect to mongodb
    client = MongoClient()
    db = client.BillBoard
    collection = db.BillBoardEveryDay

    date = "2015-11-15"
    #date = "2016-04-01"
    r = requests.get(THSOverviewDataUrlGen(date))

    heads = []

    if(r.status_code):
        #print(r.text)
        soup = bs(r.text,"lxml")
        
        # get page count
        pageCount = soup.find(id="lhdata").find(id="m_page")
        if pageCount: 
            pageCount = int(pageCount.find("span",class_="page_info").text.split('/')[-1])
        else:
            pageCount = 1
        print(pageCount)

        # get table data
        table = soup.find(id="lhdata").find(id="maintable")
        # get table heads
        tHead = table.find("thead")
        headRow = tHead.find("tr")
        headNames = headRow.find_all("th")
        heads = [ele.text.strip() for ele in headNames]
        # get table body
        tBody = table.find("tbody")
        for row in tBody.find_all("tr"):
            cols = row.find_all('td')
            if(len(cols) == 1): break
            doc = {};
            for idx,val in enumerate(cols):
                if(heads[idx] == "序号" or heads[idx] == "相关链接"):continue
                doc[heads[idx]] = val.text.strip()
            doc["日期"] = date
            collection.insert_one(doc)
        
        currentPage = 2
        while currentPage <= pageCount:
            r = requests.get(THSOverviewDataUrlGen(date,currentPage))
            if(r.status_code):
                soup = bs(r.text,"lxml")
                # get table data
                table = soup.find(id="lhdata").find(id="maintable")
                # get table body
                tBody = table.find("tbody")
                for row in tBody.find_all("tr"):
                    cols = row.find_all('td')
                    doc = {};
                    for idx,val in enumerate(cols):
                        if(heads[idx] == "序号" or heads[idx] == "相关链接"):continue
                        doc[heads[idx]] = val.text.strip()
                    doc["日期"] = date
                    collection.insert_one(doc)
            currentPage += 1
            

def EMOverviewDataUrlGen(date):
    return "http://data.eastmoney.com/stock/lhb/"+date+".html"

def GetBillBoardDataFromEM(date):
    # connect to mongodb
    client = MongoClient()
    db = client.BillBoard
    collection = db.BillBoardEveryDay

    date = "2015-11-01"
    #date = "2016-04-01"
    r = requests.get(EMOverviewDataUrlGen(date))

    heads = []

    if(r.status_code):
        #print(r.text)
        soup = bs(r.text,"lxml")
        
        # get table data
        table = soup.find(id="dt_1",recursive=False)
        # get table heads
        tHead = table.find("thead")
        headRow = tHead.find("tr")
        headNames = headRow.find_all("th")
        heads = ["".join(ele.text.split()) for ele in headNames]
        # get table body
        tBody = table.find("tbody")
        rowSpanData = []
        rowSpanCount = 0
        for row in tBody.find_all("tr"):
            cols = row.find_all('td')
            if(len(cols) == 1): break
            doc = {};

            if(rowSpanCount > 0):
                for idx,val in enumerate(rowSpanData):
                    if(heads[idx] == "序号" or heads[idx] == "相关链接"):continue
                    doc[heads[idx]] = val
            else:
                rowSpanData = []
                rowSpanCount = 0

            idxOffset = len(rowSpanData)
            for idx,val in enumerate(cols):
                # set row Span
                if val.has_attr("rowspan"): 
                    rowSpanCount = int(val["rowspan"])
                    rowSpanData.append("".join(val.text.split()))
                if(heads[idx+idxOffset] == "序号" or heads[idx+idxOffset] == "相关链接"):continue

                doc[heads[idx+idxOffset]] = "".join(val.text.split())

            doc["日期"] = date
            collection.insert_one(doc)
            rowSpanCount -= 1
    else:
        print(r.status_code)
      
def EMDetailviewDataUrlGen(date,stockCode):
    return "http://data.eastmoney.com/stock/lhb,"+date+","+stockCode+".html"

def GetBillBoardDetailDataFromEM(date,stockCode):
    # connect to mongodb
    client = MongoClient()
    db = client.BillBoard
    collection = db.BillBoardDetail

    r = requests.get(EMDetailviewDataUrlGen(date,stockCode))
    heads = []

    if(r.status_code):
        #print(r.text)
        soup = bs(r.text,"lxml")
        
        # get table data
        tables = soup.find(id="cont1").find_all("table",recursive=False)
        divtips = soup.find(id="cont1").find_all("div",class_="divtips")
        for i  in range(len(divtips)):
            doc = {}

            tips = divtips[i].find_all("li")
            tip1 = tips[0].text.replace("：",":").split(":")
            doc[tip1[0]] = tip1[1] 

            for item in tips[1].text.replace("：",":").split():
                doc[item.split(":")[0]] = item.split(":")[1]
  
            table = tables[i*2]
            headRow = table.find("thead").find_all("tr")[1]
            headNames = headRow.find_all("th")
            heads = ["".join(ele.text.split()) for ele in headNames]
            ## get table body
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
                buyers["b"+str(index)] = oneBuyer
            doc["buyers"] = buyers

            tBody = tables[i*2 + 1].find("tbody")
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
                sellers["s"+str(index)] = oneSeller
            doc["sellers"] = sellers
            for idx,val in enumerate(rows[rowsCount - 1].find_all('td')):
                if(idx == 0):continue
                doc[heads[idx+1]] = "".join(val.text.split())
       
            collection.insert_one(doc)
    else:
        print(r.status_code)