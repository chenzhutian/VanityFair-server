""" empty """
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import gevent.monkey
gevent.monkey.patch_all()
import requests
import gevent
import json
import re
import time
import os.path
from bs4 import BeautifulSoup as bs
import PyPDF2

HOST_URL = "http://ieeexplore.ieee.org"
REQUEST_HEADERS = {'user-agent':"Mozilla/5.0 (Windows NT 6.1) \
                                AppleWebKit/537.36 (KHTML, like Gecko) \
                                Chrome/41.0.2228.0 Safari/537.36"}

PAPER_CACHE_DATA = "paper_cached_data.json"
CACHE_JSON_OBJECT = None

def crawl_url_of_each_year():
    url = HOST_URL + "/xpl/RecentIssue.jsp?punumber=2945"
    request_result = requests.get(url)
    soup = bs(request_result.text,"lxml")
    uls = soup.find(id="past-issues").find("div", class_="volumes").find("div", class_="level") \
                .find_all("ul")
    data_of_hrefs = []
    for ul in uls:
        lis = ul.find_all("li")
        year = ul["id"][3:]
        for li in lis:
            href = li.find("a", href=True)
            data_of_hrefs.append({"url":href["href"], "year":year})

    def get_key(data_of_href):
        beg = data_of_href["url"].index("=")
        end = data_of_href["url"].index("&")
        return int(data_of_href["url"][beg+1:end])

    #data_of_hrefs.sort(key=get_key)
    data_of_hrefs.sort(key=get_key, reverse=True)
    return data_of_hrefs


def extract_pdf_url(data_of_paper, cookies=None):
    # return if paper data is exists
    global CACHE_JSON_OBJECT
    if os.path.isfile(PAPER_CACHE_DATA):
        if CACHE_JSON_OBJECT is None:
            paper_cache_file = open(PAPER_CACHE_DATA, "r")
            CACHE_JSON_OBJECT = json.load(paper_cache_file)
            paper_cache_file.close()

        if data_of_paper["id"] in CACHE_JSON_OBJECT:
            print(data_of_paper["id"]+" is exitst")
            return True
        
    request_result_of_pdf_page = requests.get(HOST_URL+data_of_paper["url"], cookies=cookies, 
                                              headers=REQUEST_HEADERS)
    cookies = request_result_of_pdf_page.cookies
    pdf_page = bs(request_result_of_pdf_page.text, "lxml")
    frames = pdf_page.find_all("frame")
    src = None
    for frame in frames:
        if frame["src"].startswith("http"):
            src = frame["src"]
    if src is None:
        with open("paper_cache_file.log","a") as log:
            json.dump(data_of_paper,log)
        print(data_of_paper["id"]+" is error")
        return False
    src = src[:src.index("?")]
    # cache data
    data_of_paper["src"] = src
    if os.path.isfile(PAPER_CACHE_DATA):
        CACHE_JSON_OBJECT[data_of_paper["id"]] = data_of_paper
        paper_cache_file = open(PAPER_CACHE_DATA, "w+")
        json.dump(CACHE_JSON_OBJECT, paper_cache_file)
        paper_cache_file.close()
    else:
        with open(PAPER_CACHE_DATA,"w") as paper_cache:
            cache = [data_of_paper]
            json.dump(cache, paper_cache)
    print(data_of_paper["id"]+" is finished")
    return True

def download_pdf(data_of_paper, cookies):
    filename = data_of_paper["year"] + "_" +data_of_paper["id"]+"_"+data_of_paper["title"]+".pdf"
    isInvalid = False
    if os.path.isfile(filename):
        try:
            PyPDF2.PdfFileReader(open(filename, "rb"))
        except PyPDF2.utils.PdfReadError:
            print("invalid PDF file:"+filename)
            isInvalid = True
        else:
            print(data_of_paper["id"]+" is exitst")
            return True
    
    if isInvalid:
        print("re-download paper")

    request_result_of_pdf_page = requests.get(HOST_URL+data_of_paper["url"], cookies=cookies, 
                                              headers=REQUEST_HEADERS)
    cookies = request_result_of_pdf_page.cookies
    pdf_page = bs(request_result_of_pdf_page.text, "lxml")
    frames = pdf_page.find_all("frame")
    src = None
    for frame in frames:
        if frame["src"].startswith("http"):
            src = frame["src"]
    if src is None:
        #with open("tvcg_log_file.log","a") as log:
        #    json.dump(data_of_paper,log)
        print(data_of_paper["id"]+" is error")
        return False
    src = src[:src.index("?")]
   
    request_result_of_pdf = requests.get(src, stream=True, cookies=cookies, headers=REQUEST_HEADERS)
    with open(filename, 'wb') as fd:
        for chunk in request_result_of_pdf.iter_content(chunk_size=1024):
            if chunk:
                fd.write(chunk)
    print(data_of_paper["id"]+" is finished")
    return True


def fix_error_pdf_url(file_name):
    with open(file_name,"r") as error_file:
        data_of_papers = json.load(error_file)

    for data_of_paper in data_of_papers:
        if extract_pdf_url(data_of_paper):
            pass
        else:
            time.sleep(15)

def download_pdfs(page, year, cookies):
    if page is None:
        return False
    ul = page.find("ul", class_="results")
    if ul is None:
        return False
    else:
        lis = ul.find_all("li")
    
    data_of_papers = []
    for li in lis:
        span = li.find("span", class_="select")
        id = None if span is None else span.find("input", type="checkbox")["id"]
        h3 = li.find("div", class_="txt")
        if h3 is None:
            #with open("tvcg_log_file.log","a") as log:
            #    log.write(li.text+"\r\n")
            print("page fail at "+year)
            return False
        h3 = h3.find("h3")
        title_span = h3.find("span")
        title = h3.text if title_span is None else title_span.text
        title = " ".join([re.sub("[^a-zA-Z]+", "", item) for item in title.split()])
        authors = li.find("div", class_="authors")
        authors_name = []
        if authors is not None:
            for a in authors.find_all("a"):
                authors_name.append("".join(a.find("span", id="preferredName")["class"]))
        if id is not None:
            data_of_papers.append({"url":"/stamp/stamp.jsp?tp=&arnumber="+id, 
                                      "id":id, "title":title, "year":year, 
                                      "authors_name":authors_name})

    for data_of_paper in data_of_papers:
        if extract_pdf_url(data_of_paper, cookies):
            pass
        else:
            time.sleep(15)

    #for data_of_paper in data_of_papers:
    #    if download_pdf(data_of_paper, cookies):
    #        time.sleep(4)
    #    else:
    #        time.sleep(30)
    return True

def crawl_all_pdfs():
    urls_of_volumes = crawl_url_of_each_year()
    file = open("urls_of_volumes.txt","w")
    json.dump(urls_of_volumes,file)
    file.close()
    for url_of_each_volume in urls_of_volumes:
        request_result_of_volume = requests.get(HOST_URL+url_of_each_volume["url"])
        volum_page = bs(request_result_of_volume.text, "lxml")
        download_pdfs(volum_page,url_of_each_volume["year"], request_result_of_volume.cookies)
        
        a_of_pages = volum_page.find("div", class_="pagination-wrap")
        a_of_pages = None if a_of_pages is None else a_of_pages.find_all("a")
        pages_num = 1 if a_of_pages is None else len(a_of_pages) - 2
        current_page = 2
        while current_page <= pages_num:
            url_of_next_page = url_of_each_volume["url"] + "&pageNumber=" + str(current_page)
            request_result_of_page = requests.get(HOST_URL+url_of_next_page)
            volum_page = bs(request_result_of_page.text, "lxml")
            if download_pdfs(volum_page,url_of_each_volume["year"], request_result_of_page.cookies) is False:
                print(url_of_next_page)
            current_page += 1

if __name__ == "__main__":
    #crawl_all_pdfs()
    fix_error_pdf_url("paper_cache_file.json")