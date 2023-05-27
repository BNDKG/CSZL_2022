#coding=utf-8

import tushare as ts
import pandas as pd
import CSZLUtils
import os

import random
import time

#装饰器用于catch 错误和计算函数执行时间
def decorator_catch_exception(origin_func):
    def wrapper(*args, **kwargs):
        try:
            start = time.time()
            print('function name :%s' % origin_func.__name__)
            #print('function name:%s' % func.__name__)
            u = origin_func(*args, **kwargs)

            print('time costing:', time.time() - start)
            return u
        except Exception as e:
            print('function crash 有可能是设置了全局代理 或是调用参数错误 :%s' % origin_func.__name__)
            print('str(Exception):\t', str(Exception))
            print('str(e):\t\t', str(e))
            return 'an Exception raised.'
    return wrapper

class CSZLData(object):
    """description of class"""

    def __init__(self,start_date,end_date):
        print("CSZLData init")
        #读取token
        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        self.pro = ts.pro_api(token)
        self.start_date=start_date
        self.end_date=end_date

        #self.datez=self.pro.query('trade_cal', start_date=self.start_date, end_date=self.end_date)

        #time.sleep(61)  #有时候接口抽风可能会有bug
        self.datez=self.pro.trade_cal(exchange='', start_date=self.start_date, end_date=self.end_date)
        #print(self.datez)


    #更新全部数据
    def update_all(self):
        self.updatedaily()
        self.updatedaily_adj_factor()
        self.updatedaily_long_factor()
        self.update_stk_limit()
        self.update_moneyflow()

    #更新日线数据
    @decorator_catch_exception
    def updatedaily(self):
        dfcolumn=pd.DataFrame(columns=('ts_code','trade_date','open','high','low','close','pre_close','change','pct_chg','vol','amount'))
        self.updatedatas('Dailydata.pkl',dfcolumn,self.pro.daily)
    @decorator_catch_exception
    def updatedaily_adj_factor(self):
        dfcolumn=pd.DataFrame(columns=('ts_code','trade_date','adj_factor'))
        self.updatedatas('Daily_adj_factor.pkl',dfcolumn,self.pro.adj_factor)
    @decorator_catch_exception
    def updatedaily_long_factor(self):
        dfcolumn=pd.DataFrame(columns=('ts_code','trade_date','turnover_rate','volume_ratio','pe','pb','ps_ttm','dv_ttm','circ_mv','total_mv'))
        self.updatedatas('Daily_long_factor.pkl',dfcolumn,self._get_daily_basic)
    def _get_daily_basic(self,trade_date):
        return self.pro.daily_basic(ts_code='',trade_date=trade_date,fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb,ps_ttm,dv_ttm,circ_mv,total_mv')
    @decorator_catch_exception
    def update_stk_limit(self):
        dfcolumn=pd.DataFrame(columns=('trade_date','ts_code','up_limit','down_limit'))
        self.updatedatas('Daily_stk_limit.pkl',dfcolumn,self._get_stk_limit)
    def _get_stk_limit(self,trade_date):
        return self.pro.stk_limit(ts_code='',trade_date=trade_date,limit=6900)
    @decorator_catch_exception
    def update_moneyflow(self):
        dfcolumn=pd.DataFrame(columns=('ts_code','trade_date','buy_sm_vol','buy_sm_amount','sell_sm_vol',
                                          'sell_sm_amount','buy_md_vol','buy_md_amount','sell_md_vol','sell_md_amount',
                                          'buy_lg_vol','buy_lg_amount','sell_lg_vol','sell_lg_amount','buy_elg_vol','buy_elg_amount',
                                          'sell_elg_vol','sell_elg_amount','net_mf_vol','net_mf_amount'))
        self.updatedatas('Daily_moneyflow.pkl',dfcolumn,self._get_moneyflow)
    def _get_moneyflow(self,trade_date):
        return self.pro.moneyflow(ts_code='',trade_date=trade_date)

    @decorator_catch_exception
    def updatecbdaily(self):
        dfcolumn=pd.DataFrame(columns=('ts_code','trade_date','open','high','low','close','pre_close','change','pct_chg','vol','amount'))
        self.updatedatas('CBDaily.pkl',dfcolumn,self.pro.cb_daily)

    #更新期权数据
    @decorator_catch_exception
    def update_opt(self):
        dfcolumn=pd.DataFrame(columns=('ts_code','trade_date','exchange','pre_settle','pre_close','open','high','low','close','settle','vol','amount','oi'))
        self.updatedatas('Daily_opt.pkl',dfcolumn,self._get_opt)
    def _get_opt(self,trade_date):
        return self.pro.opt_daily(ts_code='',trade_date=trade_date,exchange='SSE')


    #更新数据通用逻辑
    def updatedatas(self,data_name,dfcolumn,useapi):

        CSZLUtils.CSZLUtils.mkdir('./Database')
        #Dailydata.pkl
        savepath='./Database/'+data_name

        #读取历史数据防止重复
        try:
            df_test=pd.read_pickle(savepath)
            date_list_old=df_test['trade_date'].unique().astype(str)

            xxx=1
        except Exception as e:
            #没有的情况下list为空
            date_list_old=[]
            df_test=dfcolumn

        date=self.datez.copy(deep=True)

        date=date[date["is_open"]==1]
        bufferlist=date["cal_date"]

        get_list=bufferlist[~bufferlist.isin(date_list_old)].values
        if len(get_list)<2:
            if len(get_list)==1:
                first_date=get_list[0]
                df_all=useapi(trade_date=first_date)
            else:
                return
        else:
            first_date=get_list[0]
            next_date=get_list[1:]

            df_all=useapi(trade_date=first_date)

            zcounter=0
            zall=get_list.shape[0]
            for singledate in next_date:
                zcounter+=1
                print(zcounter*100/zall)

                dec=5
                while(dec>0):
                    try:
                        time.sleep(0.2)
                        df = useapi(trade_date=singledate)                       
                        df_all=pd.concat([df_all,df])
                        break

                    except Exception as e:
                        dec-=1
                        time.sleep(5-dec)

                if(dec==0):
                    pass

        df_all=pd.concat([df_all,df_test])
        df_all[["trade_date"]]=df_all[["trade_date"]].astype(int)
        df_all.sort_values("trade_date",inplace=True)

        df_all=df_all.reset_index(drop=True)
        #688del
        #df_all=df_all[df_all['ts_code'].str.startswith('688')==False]
        #df_all.to_csv('./Database/Dailydata.csv')
        #减少存储，但可能会导致复权数据不准确，所以原始数据不使用
        #df_all=CSZLUtils.CSZLUtils.reduce_mem_usage(df_all)
        df_all.to_pickle(savepath)


    #生成所有数据集
    def getDataSet_all(self,folderpath):
        self.getDataSet(folderpath)
        self.getDataSet_adj_factor(folderpath)
        self.getDataSet_long_factor(folderpath)
        self.getDataSet_stk_limit(folderpath)
        self.getDataSet_moneyflow(folderpath)

    @decorator_catch_exception
    def getDataSet(self,folderpath):
        #这里获取前先直接调用一下下载
        self.updatedaily()
        return self.getDataSets(folderpath,'Dailydata')
    @decorator_catch_exception
    def getDataSet_adj_factor(self,folderpath):
        #这里获取前先直接调用一下下载
        self.updatedaily_adj_factor()
        return self.getDataSets(folderpath,'Daily_adj_factor')
    @decorator_catch_exception
    def getDataSet_long_factor(self,folderpath):
        #这里获取前先直接调用一下下载
        self.updatedaily_long_factor()
        return self.getDataSets(folderpath,'Daily_long_factor')
    @decorator_catch_exception
    def getDataSet_stk_limit(self,folderpath):
        #这里获取前先直接调用一下下载
        self.update_stk_limit()
        return self.getDataSets(folderpath,'Daily_stk_limit')
    @decorator_catch_exception
    def getDataSet_moneyflow(self,folderpath):
        #这里获取前先直接调用一下下载
        self.update_moneyflow()
        return self.getDataSets(folderpath,'Daily_moneyflow')

    @decorator_catch_exception
    def getDataSet_cbdaily(self,folderpath):
        #这里获取前先直接调用一下下载
        self.updatecbdaily()
        return self.getDataSets(folderpath,'CBDaily')

    #获取数据集通用逻辑
    def getDataSets(self,folderpath,savename):
        #获取某日到某日的数据,并保存到temp中
        filename=folderpath+savename+self.start_date+'to'+self.end_date+'.pkl'
        filename=CSZLUtils.CSZLUtils.pathchange(filename)

        #检查目录是否存在
        CSZLUtils.CSZLUtils.mkdir(folderpath)
        #检查文件是否存在
        if(os.path.exists(filename)==False):       
            try:
                readname='./Database/'+savename+'.pkl'
                df_get=pd.read_pickle(readname)
                
                df_get=df_get[df_get['trade_date']>=int(self.start_date)]
                df_get=df_get[df_get['trade_date']<=int(self.end_date)]
                df_get=df_get.reset_index(drop=True)
                #减少存储(暂时不用在这里)
                #df_get=CSZLUtils.CSZLUtils.reduce_mem_usage(df_get)

                #df_get.to_csv(filename)
                CSZLUtils.CSZLUtils.Savedata(df_get,filename)
                xxx=1
                print(savename+'数据集生成完成')
            except Exception as e:
                #没有的情况下list为空
                print("错误，请先调用update下载数据或检查其他问题")
        return filename


class CSZLDataWithoutDate(object):


    def get_realtime_quotes(Default_folder_path,startdate,enddate):
        DataLoader=CSZLData(startdate,enddate)

        loadpath=DataLoader.getDataSet(Default_folder_path)
        dfDailydata_realtime=CSZLUtils.CSZLUtils.Loaddata(loadpath)

        codelistbuffer=dfDailydata_realtime['ts_code']
        codelistbuffer=codelistbuffer.unique()

        codelist=codelistbuffer.tolist()

        CSZLDataWithoutDate.get_realtime_quotes_withlist(codelist,"real_buffer.csv",500)

    def get_realtime_quotes_withlist(codelist,result_path,minnum):

        code_counter=0
        bufferlist=[]
        df_real=[]

        printcounter=0.0

        for curcode in codelist:

            curcode_str=curcode[:-3]

            #curcode_str=str(curcode).zfill(6)
            bufferlist.append(curcode_str)
            code_counter+=1
            if(code_counter>=minnum):
                if(len(df_real)):
                    wrongcounter=0
                    while(1):
                        try:
                            df_real2=[]
                            df_real2=ts.get_realtime_quotes(bufferlist)
                            df_real=df_real.append(df_real2)
                            break
                        except Exception as e:
                            sleeptime2=random.randint(100,199)
                            time.sleep(sleeptime2/40)
                            wrongcounter+=1
                            if(wrongcounter>10):
                                break
                else:
                    #df_real=ts.get_realtime_quotes(bufferlist)
                    wrongcounter=0
                    while(1):
                        try:
                            df_real=ts.get_realtime_quotes(bufferlist)
                            break
                        except Exception as e:
                            sleeptime2=random.randint(100,199)
                            time.sleep(sleeptime2/40)
                            wrongcounter+=1
                            if(wrongcounter>10):
                                break
                bufferlist=[]            
                code_counter=0
                sleeptime=random.randint(100,199)
                time.sleep(sleeptime/40)
                print(printcounter/len(codelist))

            printcounter+=1
        time.sleep(2)
        if(len(bufferlist)):
            wrongcounter=0
            while(1):
                try:
                    df_real2=[]
                    df_real2=ts.get_realtime_quotes(bufferlist)
                    df_real=df_real.append(df_real2)
                    break
                except Exception as e:
                    sleeptime2=random.randint(100,199)
                    time.sleep(sleeptime2/40)
                    wrongcounter+=1
                    if(wrongcounter>10):
                        break



        #'tomorrow_chg'
        df_real.drop(['name','bid','ask','volume','b1_v','b2_v','b3_v','b4_v','b5_v','b1_p','b2_p','b3_p','b4_p','b5_p'],axis=1,inplace=True)
        df_real.drop(['a1_v','a2_v','a3_v','a4_v','a5_v','a1_p','a2_p','a3_p','a4_p','a5_p'],axis=1,inplace=True)
        df_real.drop(['time'],axis=1,inplace=True)

        df_real['amount'] = df_real['amount'].apply(float)
        df_real['amount']=df_real['amount']/1000

        #df[txt] = df[txt].map(lambda x : x[:-2])

        df_real['date']=df_real['date'].map(lambda x : x[:4]+x[5:7]+x[8:10])
    
        df_real['price'] = df_real['price'].apply(float)
        df_real['pre_close'] = df_real['pre_close'].apply(float)

        df_real['pct_chg']=(df_real['price']-df_real['pre_close'])*100/(df_real['pre_close'])


        df_real=df_real.rename(columns={'price':'close','date':'trade_date','code':'ts_code'})

        df_real.to_csv(result_path)

    def get_realtime_quotes_CB(Default_folder_path,startdate,enddate):

        DataLoader=CSZLData(startdate,enddate)

        loadpath=DataLoader.getDataSet_cbdaily(Default_folder_path)
        dfDailydata_realtime=CSZLUtils.CSZLUtils.Loaddata(loadpath)

        codelistbuffer=dfDailydata_realtime['ts_code']
        codelistbuffer=codelistbuffer.unique()

        codelist=codelistbuffer.tolist()

        CSZLDataWithoutDate.get_realtime_quotes_withlist(codelist,"real_buffer_CB.csv",200)

        pass

    #获取指数行情
    def get_baseline(basecode='000905.SH'):

        #000001.SH 上证 000016.SH 50 000688.SH 科创50 000905.SH 中证500 399006.SZ 创业板指
        #399300.SZ 300 000852.SH 1000 

        savedir='./Database/indexdata'
        #检查目录是否存在
        CSZLUtils.CSZLUtils.mkdir(savedir)

        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        pro = ts.pro_api(token)

        df = pro.index_daily(ts_code=basecode)

        savepth=savedir+'/'+basecode+'.csv'
        df.to_csv(savepth)


        return savepth

    #获取股票列表
    def get_stocklist():

        savedir='./Database'
        #检查目录是否存在
        CSZLUtils.CSZLUtils.mkdir(savedir)

        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        pro = ts.pro_api(token)

        df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

        savepth=savedir+'/stocklist.csv'
        df.to_csv(savepth,encoding='utf-8-sig')

        return savepth

    #获取可转债列表
    def get_cb_basic():

        savedir='./Database'
        #检查目录是否存在
        CSZLUtils.CSZLUtils.mkdir(savedir)

        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        pro = ts.pro_api(token)

        df = pro.cb_basic(fields="ts_code,bond_short_name,stk_code,stk_short_name,list_date,conv_price,delist_date,issue_rating")

        print(df)
        savepth=savedir+'/cb_basic.csv'
        df.to_csv(savepth,encoding='utf-8-sig')

        return savepth

        pass

    def get_index_weight(basecode='000905.SH'):

        #000001.SH 上证 000016.SH 50 000688.SH 科创50 000905.SH 中证500 399006.SZ 创业板指
        #399300.SZ 300 000300.SH 300 000852.SH 1000 

        savedir='./Database/indexdata'
        #检查目录是否存在
        CSZLUtils.CSZLUtils.mkdir(savedir)

        f = open('token.txt')
        token = f.read()     #将txt文件的所有内容读入到字符串str中
        f.close()

        pro = ts.pro_api(token)

        df = pro.index_weight(index_code=basecode)

        savepth=savedir+'/'+basecode+'weight.csv'
        df.to_csv(savepth)


        return savepth