""" Empty """
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import gevent.monkey
gevent.monkey.patch_all()
import requests
import gevent
import re
import json
from datetime import timedelta, datetime

# 日期 代码 名称 上榜原因 收盘价 涨跌幅 成交量 成交额 买入s：[代码 日期 单位名称,买入额，卖出额] 卖出s:[代码 日期
# 单位名称,买入额，卖出额]
REQUEST_HEADERS = {'user-agent':"Mozilla/5.0 (Windows NT 6.1) \
                                AppleWebKit/537.36 (KHTML, like Gecko) \
                                Chrome/41.0.2228.0 Safari/537.36"}
LOG_FILE_NAME = "crawlerLogfile.log"
LOG_TO_FILE = open(LOG_FILE_NAME, 'a')
RETRIES_NUM_LIMIT = 5

def overview_data_url_gen(start_date, end_date):
    """ generate the url to get overview data of billBoard from qq finance """
    return "http://stock.finance.qq.com/cgi-bin/sstock/q_lhb_js?t=2&b=" + \
            start_date.strftime("%Y%m%d") + "&e=" + end_date.strftime("%Y%m%d") + "&ol=6"

def detail_data_url_gen(date, stock_code, detail_code):
    """ generate the url to get detail data of billBoard from qq finance """
    if type(date) is str:
        date = "".join(date.split("-"))
    else:
        date = date.strftime("%Y%m%d")
    return "http://stock.finance.qq.com/cgi-bin/sstock/q_lhb_xx_js?c=" + stock_code + "&b=" + \
            date + "&l=" + detail_code

def get_overview_data(start_date, end_date, collection):
    """ get overview data of billBoard from qq finance """
    request_result = requests.get(overview_data_url_gen(start_date, end_date), 
                                  headers=REQUEST_HEADERS)
    retries_num = 1
    while request_result.status_code != requests.codes.ok:
        if retries_num < RETRIES_NUM_LIMIT:
            request_result = requests.get(overview_data_url_gen(start_date, end_date), 
                                          headers=REQUEST_HEADERS)
            retries_num += 1
        else:
            json.dump({"date":start_date.strftime("%Y-%m-%d"), 
                       "statusCode":request_result.status_code, 
                       "time":datetime.now()}, LOG_TO_FILE)
            return False

    result = re.sub('([{,])([^{:\s"]*):', lambda m: '%s"%s":' % (m.group(1), m.group(2)), 
                    request_result.text[14:-2])
    try:
        result = json.loads(result)
    except:
        json.dump({"date":start_date.strftime("%Y-%m-%d"), "statusCode":request_result.status_code,
                   "time":datetime.now()}, LOG_TO_FILE)
        return False
    
    heads = ["日期", "代码", "名称", "上榜原因", "detail_code", "收盘价", "涨跌幅"]
    docs = {}
    stock_detail_codes = set()
    for data in result['_datas']:
        doc = {}
        for i, col in enumerate(data):
            if i == 5: # head[i] == "收盘价"
                continue
            doc[heads[i]] = col
        docs[doc["代码"] + "_" + doc["detail_code"]] = doc
        stock_detail_codes.add(doc["代码"] + "_" + doc["detail_code"])
        
    threads = []
    for stock_detail_code in stock_detail_codes:
        stock_code = stock_detail_code.split("_")[0]
        detail_code = stock_detail_code.split("_")[1]
        threads.append(gevent.spawn(merge_detail_with_overview_data,
                                    docs[stock_detail_code], docs[stock_detail_code]["日期"], 
                                    stock_code, detail_code, collection))
            
    gevent.joinall(threads)
    print(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
    return True

def merge_detail_with_overview_data(doc, date, stock_code, detail_code, collection):
    """ merge detail data to overview data"""
    detail_doc = get_detail_data(date, stock_code, detail_code)
    if detail_doc is not None:
        for key in detail_doc:
            doc[key] = detail_doc[key]
        del doc["detail_code"]
        collection.insert_one(doc)

def get_detail_data(date, stock_code, detail_code):
    """ get detail data of billBoard from qq finance """
    requests_result = requests.get(detail_data_url_gen(date, stock_code, detail_code), 
                                   headers=REQUEST_HEADERS)
    retries_num = 1
    while requests_result.status_code != requests.codes.ok:
        if retries_num < RETRIES_NUM_LIMIT:
            requests_result = requests.get(detail_data_url_gen(date, stock_code, detail_code), 
                                           headers=REQUEST_HEADERS)
            retries_num += 1
        else:
            json.dump({"date":date, "stockCode":stock_code, "detail_code":detail_code, 
                       "statusCode":requests_result.status_code, 
                       "time":datetime.now()}, LOG_TO_FILE)
            return None

    result = re.sub('([{,])([^{:\s"]*):', lambda m: '%s"%s":' % (m.group(1), m.group(2)),
                    requests_result.text[16:-2])
    try:
        result = json.loads(result)
    except:
        json.dump({"date":date.strftime("%Y-%m-%d"), "statusCode":requests_result.status_code,
                   "time":datetime.now()}, LOG_TO_FILE)
        return False

    heads = ["代码", "名称", "BorS", "nouse", "日期", "单位名称", "买入额", "卖出额"]
    try:
        cje = round(float(result["_cje"]) / 10000, 2)
    except ValueError:
        cje = "--" 
    detail_doc = {"成交额":cje, "成交量":result["_cjl"]}
    buyers = []
    sellers = []
    filter_cols = {0, 1, 3, 4}
    for cols in result["_datas"]:
        someone = {}
        for i, col in enumerate(cols):
            if i in filter_cols: #if(heads[i] == "名称" or heads[i] == "nouse"):
                continue
            if i == 6 or i == 7:
                try:
                    someone[heads[i]] = round(float(col) / 10000, 2)
                except ValueError:
                    someone[heads[i]] = "--"
            else:
                someone[heads[i]] = col
        if someone["BorS"] == "B":
            buyers.append(someone)
        else:
            sellers.append(someone)
        del someone["BorS"]
    detail_doc["buyers"] = buyers
    detail_doc["sellers"] = sellers

    return detail_doc

def crawl_billboard_data(start, end, collection):
    """ crawl the billborad data from start to end"""
    current = start
    next_current = start
    span = 5
    while current < end:
        next_current += timedelta(span)
        if next_current > end:
            next_current = end
        get_overview_data(current, next_current, collection)
        current = next_current + timedelta(1)
    LOG_TO_FILE.close()
   