def EMOverviewDataUrlGen(date):
    return "http://data.eastmoney.com/stock/lhb/" + date.strftime("%Y-%m-%d") + ".html"

def EMDetailDataUrlGen(date,stockCode):
    return "http://data.eastmoney.com/stock/lhb," + date.strftime("%Y-%m-%d") + "," + stockCode + ".html"

def SnowBallOverviewDataUrlGen(date):
    return "http://xueqiu.com/stock/f10/bizunittrdinfo.json?date=" + date.strftime("%Y%m%d")

def SnowBallDetailDataUrlGen(date,stockCode):
    return "http://xueqiu.com/stock/f10/bizunittrdinfo.json?symbol=" + stockCode + "&date=" + date.strftime("%Y%m%d")

def QQOverviewDataUrlGen(date):
    return "http://stock.finance.qq.com/cgi-bin/sstock/q_lhb_js?t=2&b=" + date.strftime("%Y%m%d") + "&e=" + date.strftime("%Y%m%d") + "&ol=6"

def QQDetailDataUrlGen(date,stockCode,detailCode):
    return "http://stock.finance.qq.com/cgi-bin/sstock/q_lhb_xx_js?c=" + stockCode + "&b=" + date.strftime("%Y%m%d") + "&l=" + detailCode
