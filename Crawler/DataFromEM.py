""" empty """
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import gevent.monkey
gevent.monkey.patch_all()
import requests
import gevent
import json
from bs4 import BeautifulSoup as bs
from datetime import datetime

REQUEST_HEADERS = {'user-agent':"Mozilla/5.0 (Windows NT 6.1) \
                                 AppleWebKit/537.36 (KHTML, like Gecko) \
                                 Chrome/41.0.2228.0 Safari/537.36"}
LOGFILE = "crawlerLogfile.log"
LOG_TO_FILE = open(LOGFILE, 'a')
RETRIES_NUM_LIMIT = 5


def overview_data_url_gen(date):
    """ empty """
    return "http://data.eastmoney.com/stock/lhb/" + date.strftime("%Y-%m-%d") + ".html"

def detail_data_url_gen(date, stock_code):
    """ empty """
    return "http://data.eastmoney.com/stock/lhb," + date.strftime("%Y-%m-%d") + \
            "," + stock_code + ".html"

def get_overview_data(date, collection):
    """ empty """
    requests_result = requests.get(overview_data_url_gen(date), headers=REQUEST_HEADERS)
    retries_num = 1
    while requests_result.status_code != requests.codes.ok:
        if retries_num < RETRIES_NUM_LIMIT:
            requests_result = requests.get(overview_data_url_gen(date), headers=REQUEST_HEADERS)
            retries_num += 1
        else:
            json.dump({"date":date.strftime("%Y-%m-%d"), "statusCode":requests_result.status_code,
                       "time":datetime.now()}, LOG_TO_FILE)
            return False

    soup = bs(requests_result.text, "lxml")
    # get table data
    table = soup.find(id="dt_1")
    heads = ["序号", "代码", "名称", "相关链接", "涨跌幅", "龙虎榜成交额", "买入额", "占总成交比例", 
             "卖出额", "占总成交比例", "上榜原因"]
    # get table body
    table_body = table.find("tbody")
    row_span_data = []
    row_span_count = 0
    docs = {}
    stock_codes = set()
    for row in table_body.find_all("tr", class_="all"):
        cols = row.find_all('td')
        if len(cols) == 1:
            break
        doc = {}

        ## whether it is rowspan
        if row_span_count > 0:
            for idx, val in enumerate(row_span_data):
                if idx == 0 or idx == 3:
                    continue
                doc[heads[idx]] = val
        else:
            row_span_data = []
            row_span_count = 0

        ## get data from each columns
        idx_offset = len(row_span_data)
        filter_cols = {0, 3, 5, 6, 7, 8, 9}
        for idx, val in enumerate(cols):
            # set row Span
            if val.has_attr("rowspan"):
                row_span_count = int(val["rowspan"])
                row_span_data.append("".join(val.text.split()))
            if  idx + idx_offset in filter_cols:
                continue
            doc[heads[idx + idx_offset]] = "".join(val.text.split())
        doc["日期"] = date.strftime("%Y-%m-%d")
        stock_codes.add(doc["代码"])
        if doc["代码"] not in docs:
            docs[doc["代码"]] = []
        docs[doc["代码"]].append(doc)

        if row_span_count > 0: 
            row_span_count -= 1

    threads = [gevent.spawn(merge_detail_with_overview_data, docs[stock_code], date,
                            stock_code, collection) for stock_code in stock_codes]

    gevent.joinall(threads)
    return True

def merge_detail_with_overview_data(docs, date, stock_code, collection):
    """ empty """
    detail_docs = get_detail_data(date, stock_code)
    if detail_docs is not None:
        for detail_doc in detail_docs:
            target_doc = None
            for doc in docs:
                if detail_doc["上榜原因"] == doc["上榜原因"]:
                    target_doc = doc
            for key in detail_doc:
                target_doc[key] = detail_doc[key]
            target_doc["涨跌幅"] = target_doc["涨跌幅"][:-1]
            if "收盘价" in target_doc: 
                del target_doc["收盘价"]
            collection.insert_one(target_doc)

def get_detail_data(date, stock_code):
    """ empty """
    request_result = requests.get(detail_data_url_gen(date, stock_code), headers=REQUEST_HEADERS)
    retries_num = 1
    while request_result.status_code != requests.codes.ok:
        if retries_num < RETRIES_NUM_LIMIT:
            request_result = requests.get(detail_data_url_gen(date, stock_code), 
                                          headers=REQUEST_HEADERS)
            retries_num += 1
        else:
            json.dump({"date":date.strftime("%Y-%m-%d"), "stockCode":stock_code,
                       "statusCode":request_result.status_code, "time":datetime.now()},
                      LOG_TO_FILE)
            return None

    docs = []
    soup = bs(request_result.text, "lxml") 
    # get table data
    target_div = soup.find(id="cont1")
    tables = target_div.find_all("table")
    divtips = target_div.find_all("div", class_="divtips")
    for i, divtip in enumerate(divtips):
        doc = {}
        ## get tips data
        tiplis = divtip.find_all("li")
        tipli = tiplis[0].text.replace("：", ":").split(":")
        doc["类型" if tipli[0] == "上榜原因" else tipli[0]] = tipli[1] 

        if len(tiplis) > 1:
            for item in tiplis[1].text.replace("：", ":").split():
                item = item.split(":")
                if item[0] == "成交金额":
                    doc["成交额"] = item[1][:-2]
                elif item[0] == "成交量":
                    doc[item[0]] = item[1][:-2]

        ## get table head
        table = tables[i * 2]
        heads = ["序号", "单位名称", "买入额", "占总成交比例", "卖出额", "占总成交比例", "净额"]
        ## get buyer table body
        table_body = table.find("tbody")
        buyers = []
        filter_cols = {0, 3, 5, 6}
        for row in table_body.find_all("tr"):
            cols = row.find_all('td')
            if len(cols) == 1: 
                break

            one_buyer = {}
            for idx, val in enumerate(cols):
                if idx in filter_cols:
                    continue
                one_buyer[heads[idx]] = "".join(val.text.split())
            buyers.append(one_buyer)
        doc["buyers"] = buyers

        ## get seller table body
        table_body = tables[i * 2 + 1].find("tbody")
        sellers = []
        filter_cols = {0, 3, 5, 6}
        for row in table_body.find_all("tr"):
            cols = row.find_all('td')
            if len(cols) == 1: 
                break
                
            one_seller = {}
            for idx, val in enumerate(cols):
                if idx in filter_cols:
                    continue
                one_seller[heads[idx]] = "".join(val.text.split())
            sellers.append(one_seller)
        doc["sellers"] = sellers
        docs.append(doc)
    return docs
