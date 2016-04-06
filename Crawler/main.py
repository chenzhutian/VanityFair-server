""" main entry """
from datetime import date
from pymongo import MongoClient
from DataFromQQ import crawl_billboard_data


if __name__ == "__main__":
    # connect to mongodb
    client = MongoClient()
    db = client.BillBoard
    collection = db.BillBoardEveryDay
    
    start = date(2016, 1, 1)
    end = date(2016, 4, 6)
    crawl_billboard_data(start, end, collection)
    