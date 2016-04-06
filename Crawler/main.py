from datetime import date
from pymongo import MongoClient
from DataFromQQ import CrawlBillBoardData


if __name__ == "__main__":
    # connect to mongodb
    client = MongoClient()
    db = client.BillBoard
    collection = db.BillBoardEveryDay
    
    start = date(2016,4,6)
    end = date(2016,1,1)
    CrawlBillBoardData(start,end,collection)