#coding=utf-8

import CSZLData

class CSZLFeatureEngineering(object):
    """负责进行特征工程"""

    def __init__(self,start_date,end_date):

        self.dfDailydata=null
        self.start_date=start_date
        self.end_date=end_date

        self.CSZLDataLoader=CSZLData.CSZLData("20220101","20220301")

        pass

    def LoaddfDailydata():
        getDataSet

    def create_target():



