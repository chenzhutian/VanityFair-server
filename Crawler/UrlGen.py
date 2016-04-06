## this file may be deprecated

def SnowBallOverviewDataUrlGen(date):
    return "http://xueqiu.com/stock/f10/bizunittrdinfo.json?date=" + date.strftime("%Y%m%d")

def SnowBallDetailDataUrlGen(date,stockCode):
    return "http://xueqiu.com/stock/f10/bizunittrdinfo.json?symbol=" + stockCode + "&date=" + date.strftime("%Y%m%d")

