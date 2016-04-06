""" 
    Url generators
    this file may be deprecated 
"""

def snowball_overview_data_url_gen(date):
    """ generate the url to get overveiw data of billBoard from SnowBall """
    return "http://xueqiu.com/stock/f10/bizunittrdinfo.json?date=" + date.strftime("%Y%m%d")

def snowball_detail_data_url_gen(date, stock_code):
    """ generate the url to get detail data of billBoard from SnowBall """
    return "http://xueqiu.com/stock/f10/bizunittrdinfo.json?symbol=" + stock_code + "&date=" + \
            date.strftime("%Y%m%d")
