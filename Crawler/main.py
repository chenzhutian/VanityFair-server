""" main entry """
from datetime import date
from pymongo import MongoClient
from DataFromQQ import crawl_billboard_data


if __name__ == "__main__":
    # connect to mongodb
    client = MongoClient("localhost",27016)
    db = client.BillBoard
    collection = db.BillBoardEveryDay
    
    start = date(2013, 1, 1)
    end = date(2013, 12, 31)
    crawl_billboard_data(start, end, collection)
    