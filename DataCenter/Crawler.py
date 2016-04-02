from bs4 import BeautifulSoup as bs
import requests
from pymongo import MongoClient

def DetailDataUrlGen(stockCode,date):
    return "http://data.10jqka.com.cn/market/lhbcjmx/code/"+stockCode+"/date/"+date+"/ajax/1"

def OverviewDataUrlGen(date,page = 1):
    return "http://data.10jqka.com.cn/market/longhu/date/"+date+"/ajax/1/page/"+ str(page)


def GetBillBoardDataFromTHSOn(date = None):
    # connect to mongodb
    client = MongoClient()
    db = client.BillBoard
    collection = db.BillBoardEveryDay

    date = "2015-11-16"
    #date = "2016-04-01"
    r = requests.get(OverviewDataUrlGen(date))

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
            doc = {};
            for idx,val in enumerate(cols):
                if(heads[idx] == "序号" or heads[idx] == "相关链接"):continue
                doc[heads[idx]] = val.text.strip()
            doc["日期"] = date
            collection.insert_one(doc)
        
        currentPage = 2
        while currentPage <= pageCount:
            r = requests.get(OverviewDataUrlGen(date,currentPage))
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
            
