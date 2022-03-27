#coding=utf-8

import tushare as ts
import pandas as pd
import CSZLUtils

import time


def catch_exception_log_time(origin_func):
    def wrapper(*args, **kwargs):
        try:
            start = time.time()
            print('function name :%s' % origin_func.__name__)
            #print('function name:%s' % func.__name__)
            u = origin_func(*args, **kwargs)

            print('time costing:', time.time() - start)
            return u
        except Exception:
            return 'an Exception raised.'
    return wrapper

class CSZLData(object):
    """description of class"""

    def __init__(self):
        print("CSZLData init")
        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        self.pro = ts.pro_api(token)

    
    @catch_exception_log_time
    def updatedaily(self,start_date,end_date):
       
        CSZLUtils.CSZLUtils.mkdir('./Database')

        #读取历史数据防止重复
        try:
            df_test=pd.read_pickle('./Database/Dailydata.pkl')
            date_list_old=df_test['trade_date'].unique().astype(str)

            xxx=1
        except Exception as e:
            #没有的情况下list为空
            date_list_old=[]
            df_test=pd.DataFrame(columns=('ts_code','trade_date','open','high','low','close','pre_close','change','pct_chg','vol','amount'))

        date=self.pro.query('trade_cal', start_date=start_date, end_date=end_date)

        date=date[date["is_open"]==1]
        bufferlist=date["cal_date"]

        get_list=bufferlist[~bufferlist.isin(date_list_old)].values
        if len(get_list)<2:
            if len(get_list)==1:
                first_date=get_list[0]
                df_all=self.pro.daily(trade_date=first_date)
            else:
                return
        else:
            first_date=get_list[0]
            next_date=get_list[1:]

            df_all=self.pro.daily(trade_date=first_date)

            zcounter=0
            zall=get_list.shape[0]
            for singledate in next_date:
                zcounter+=1
                print(zcounter*100/zall)

                dec=5
                while(dec>0):
                    try:
                        time.sleep(1)
                        df = self.pro.daily(trade_date=singledate)
                        
                        df_all=pd.concat([df_all,df])

                        #df_last
                        #print(df_all)
                        break

                    except Exception as e:
                        dec-=1
                        time.sleep(5-dec)

                if(dec==0):
                    fsefe=1

        df_all=pd.concat([df_all,df_test])
        df_all[["trade_date"]]=df_all[["trade_date"]].astype(int)
        df_all.sort_values("trade_date",inplace=True)

        df_all=df_all.reset_index(drop=True)
        #688del
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        #df_all.to_csv('./Database/Dailydata.csv')
        df_all.to_pickle("./Database/Dailydata.pkl")
        dsdfsf=1

        print(date)

        asdfasfd=1