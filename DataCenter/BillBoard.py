import tushare as ts


def GetBillBoardOn(date):
    """
    Get BillBoard data according to date.

    Args:
        date: a data string whose format is "YYYY-MM-DD"

    Returns:
    """
    return ts.top_list(date)

def GetStockCountOnBillBoardIn(days):
    """
    Args:
        days: days should be 5,10,30 or 60
    """
    return ts.cap_tops(date)

def GetBrokerCountOnBillBoardIn(days):
    """
    Args:
        days: days should be 5,10,30 or 60
    """
    return ts.broker_tops(days)

def GetInstitutionCountOnBillBoardIn(days):
    """
    Args:
        days: days should be 5,10,30 or 60
    """
    return ts.broker_tops(days)

def GetInstitutionDetailAtLastDay():
    """
    """
    return ts.inst_detail()