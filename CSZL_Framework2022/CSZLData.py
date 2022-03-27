#coding=utf-8

import tushare as ts

class CSZLData(object):
    """description of class"""

    def __init__(self):
        print("CSZLData init")
        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        self.pro = ts.pro_api(token)

    def updatedaily(self,start_date,end_date):

        date=self.pro.query('trade_cal', start_date=start_date, end_date=end_date)

        print(date)

        asdfasfd=1