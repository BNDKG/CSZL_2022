#coding=utf-8

import os
import CSZLData
import CSZLFeatureEngineering as FE
import CSZLModel
import CSZLDisplay
import CSZLUtils
import pandas as pd
import datetime
import time

class CSZLWorkflow(object):
    """各种workflow 主要就是back testing"""

    def BackTesting(self):

        #Default_folder_path='./temp/'
        Default_folder_path='D:/temp2/'

        #zzzz=CSZLData.CSZLData("20220101","20220301")

        #zzzz.getDataSet_all(Default_folder_path)

        #"20150801","20220425"
        dayA='20150101'#nomal/small
        dayB='20170301'
        dayB='20200101'
        #dayA='20150801'#nomal/small
        #dayB='20220425'
        dayC='20170301'
        dayD='20220425'

        #dayA='20150801'#nomal/small
        #dayB='20220425'
        #dayC='20220201'
        #dayD='20220429'


        dayC='20200301'
        dayD='20220505'

        zzzz=FE.CSZLFeatureEngineering(dayA,dayB,Default_folder_path)
        trainpath=zzzz.FE03()
        #zzzz=FE.CSZLFeatureEngineering("20170301","20220301",Default_folder_path)
        #testpath=zzzz.FE03()
        zzzz=FE.CSZLFeatureEngineering(dayC,dayD,Default_folder_path)
        testpath=zzzz.FE03()

        #zzzz=FE.CSZLFeatureEngineering("20220101","20220408",Default_folder_path)
        #testpath=zzzz.FE03()

        #zzzz=FE.CSZLFeatureEngineering("20190101","20190301",Default_folder_path)
        #trainpath=zzzz.FE03()
        #zzzz=FE.CSZLFeatureEngineering("20220101","20220301",Default_folder_path)
        #testpath=zzzz.FE03()

        #zzzz=FE.CSZLFeatureEngineering("20190101","20210101",Default_folder_path)
        #trainpath=zzzz.FE03()
        #zzzz=FE.CSZLFeatureEngineering("20220101","20220401",Default_folder_path)
        #testpath=zzzz.FE03()


        cur_model=CSZLModel.CSZLModel()

        cur_model_path=cur_model.LGBmodeltrain(trainpath)
        #resultpath2=cur_model.LGBmodelpredict(trainpath,cur_model_path)
        resultpath=cur_model.LGBmodelpredict(testpath,cur_model_path)


        #cur_model_path2=cur_model.LGBmodelretrain(trainpath,resultpath2)
        #resultpath3=cur_model.LGBmodelrepredict(testpath,resultpath,cur_model_path2)

        resultpath=cur_model.MixOutputresult_groupbalence(testpath,cur_model_path)

        today_df = pd.read_csv(resultpath,index_col=0,header=0)

        #lastday=today_df['trade_date'].max()
        #today_df['ts_code']=today_df['ts_code'].apply(lambda x : x[:-3])
        #copy_df=today_df[today_df['trade_date']==lastday]
        #copy_df.to_csv("Today_NEXT_predict.csv")

        curdisplay=CSZLDisplay.CSZLDisplay()
        curdisplay.Topk_nextopen(resultpath)

        pass

    def BackTesting_static_0501(self):

        #生成需要的数据集
        nowTime=datetime.datetime.now()
        delta = datetime.timedelta(days=63)
        delta_one = datetime.timedelta(days=1)
        LastTime=nowTime-delta_one
        month_ago = LastTime - delta
        month_ago_next=month_ago+delta_one
        Day_start=month_ago_next.strftime('%Y%m%d')  
        Day_end=LastTime.strftime('%Y%m%d')  
        Day_now=nowTime.strftime('%Y%m%d')

        #Default_folder_path='./temp2/'
        Default_folder_path='D:/temp2/'

        dayA='20150801'#nomal/small
        dayB='20220425'


        #dayA='20150801'#nomal/small
        #dayB='20220425'
        dayC=Day_start
        dayD=Day_now

        zzzz=FE.CSZLFeatureEngineering(dayA,dayB,Default_folder_path)
        trainpath=zzzz.FE03()

        zzzz=FE.CSZLFeatureEngineering(dayC,dayD,Default_folder_path)
        testpath=zzzz.FE03()

        cur_model=CSZLModel.CSZLModel()

        cur_model_path=cur_model.LGBmodeltrain(trainpath)

        cur_model.LGBmodelpredict(testpath,cur_model_path)


        resultpath=cur_model.MixOutputresult_groupbalence(testpath,cur_model_path)

        today_df = pd.read_csv(resultpath,index_col=0,header=0)

        lastday=today_df['trade_date'].max()
        today_df['ts_code']=today_df['ts_code'].apply(lambda x : x[:-3])
        copy_df=today_df[today_df['trade_date']==lastday]
        copy_df.to_csv("Today_NEXT_predict.csv")


        #curdisplay=CSZLDisplay.CSZLDisplay()
        #curdisplay.Topk_nextopen(resultpath)

        pass

    def BackTesting2(self):

        #Default_folder_path='./temp/'
        Default_folder_path='D:/temp2/'

        #zzzz=CSZLData.CSZLData("20220101","20220301")

        #zzzz.getDataSet_all(Default_folder_path)

        zzzz=FE.CSZLFeatureEngineering("20130101","20170301",Default_folder_path)
        trainpath=zzzz.FE03()
        zzzz=FE.CSZLFeatureEngineering("20170301","20220301",Default_folder_path)
        testpath=zzzz.FE03()

        #zzzz=FE.CSZLFeatureEngineering("20220101","20220408",Default_folder_path)
        #testpath=zzzz.FE03()

        #zzzz=FE.CSZLFeatureEngineering("20190101","20190301",Default_folder_path)
        #trainpath=zzzz.FE03()
        #zzzz=FE.CSZLFeatureEngineering("20220101","20220301",Default_folder_path)
        #testpath=zzzz.FE03()

        #zzzz=FE.CSZLFeatureEngineering("20190101","20200301",Default_folder_path)
        #trainpath=zzzz.FE03()
        #zzzz=FE.CSZLFeatureEngineering("20210101","20220301",Default_folder_path)
        #testpath=zzzz.FE03()


        cur_model=CSZLModel.CSZLModel()

        cur_model_path=cur_model.LGBmodeltrain(trainpath)
        resultpath2=cur_model.LGBmodelpredict(trainpath,cur_model_path)
        resultpath=cur_model.LGBmodelpredict(testpath,cur_model_path)


        cur_model_path2=cur_model.LGBmodelretrain(trainpath,resultpath2)
        resultpath3=cur_model.LGBmodelrepredict(testpath,resultpath,cur_model_path2)

        #resultpath=cur_model.MixOutputresult(testpath,cur_model_path)


        curdisplay=CSZLDisplay.CSZLDisplay()
        curdisplay.Topk_nextopen(resultpath3)

        pass

    def RealTimePredict(self):

        Default_folder_path='./temp2/'
        #Default_folder_path='D:/temp2/'

        #cur_model_path="D:/temp2/FE0320190101to20210101_0/LGBmodeltrainLGBmodel_003"
        #cur_model_path="D:/temp2/FE0320150801to20220425_0/LGBmodeltrainLGBmodel_003"
        cur_model_path="./temp2/FE0320150801to20220425_0/LGBmodeltrainLGBmodel_003"
        #是否需要重新生成
        if False:
            #zzzz=FE.CSZLFeatureEngineering("20190101","20210101",Default_folder_path)
            #trainpath=zzzz.FE03()
            zzzz=FE.CSZLFeatureEngineering("20150801","20220425",Default_folder_path)
            trainpath=zzzz.FE03()
            cur_model=CSZLModel.CSZLModel()
            cur_model_path=cur_model.LGBmodeltrain(trainpath)
        

        #生成需要的数据集
        nowTime=datetime.datetime.now()
        delta = datetime.timedelta(days=63)
        delta_one = datetime.timedelta(days=1)
        LastTime=nowTime-delta_one
        month_ago = LastTime - delta
        month_ago_next=month_ago+delta_one
        Day_start=month_ago_next.strftime('%Y%m%d')  
        Day_end=LastTime.strftime('%Y%m%d')

        Day_now=nowTime.strftime('%Y%m%d')  

        CSZLData.CSZLDataWithoutDate.get_realtime_quotes(Default_folder_path,Day_start,Day_end)
        zzzz=FE.CSZLFeatureEngineering(Day_start,Day_end,Default_folder_path)
        #zzzz=FE.CSZLFeatureEngineering("20220301","20220420",Default_folder_path)
        #trainpath=zzzz.FE03()
        #bbbb=pd.read_pickle(trainpath)
        #aaaa=bbbb.head(10)
        #aaaa=aaaa.to_csv("tttt.csv")

        zzzz.FE03_real(int(Day_now))
        featurepath="Today_Joinfeature.csv"

        cur_model=CSZLModel.CSZLModel()
        #resultpath2=cur_model.LGBmodelpredict(trainpath,cur_model_path)
        resultpath=cur_model.LGBmodelpredict(featurepath,cur_model_path)
        
        resultpath=cur_model.MixOutputresult_groupbalence(featurepath,cur_model_path,resultpath)    
        


        pass

    def RealTimePredict_CB(self):

        Default_folder_path='./temp2/'
        #Default_folder_path='D:/temp2/'

        #cur_model_path="D:/temp2/FE0320190101to20210101_0/LGBmodeltrainLGBmodel_003"
        #cur_model_path="D:/temp2/FE0320150801to20220425_0/LGBmodeltrainLGBmodel_003"
        cur_model_path="./temp2/FECB0120130101to20210301_0/LGBmodeltrain_CBLGBmodel_003"
        #是否需要重新生成
        if False:
            dayA='20130101'#nomal/small
            dayB='20210301'
            zzzz=FE.CSZLFeatureEngineering(dayA,dayB,Default_folder_path)
            trainpath=zzzz.FECB01()
            cur_model=CSZLModel.CSZLModel()
            cur_model_path=cur_model.LGBmodeltrain_CB(trainpath)
        

        #生成需要的数据集
        nowTime=datetime.datetime.now()
        delta = datetime.timedelta(days=63)
        delta_one = datetime.timedelta(days=1)
        LastTime=nowTime-delta_one
        month_ago = LastTime - delta
        month_ago_next=month_ago+delta_one
        Day_start=month_ago_next.strftime('%Y%m%d')  
        Day_end=LastTime.strftime('%Y%m%d')

        Day_now=nowTime.strftime('%Y%m%d')  

        CSZLData.CSZLDataWithoutDate.get_realtime_quotes_CB(Default_folder_path,Day_start,Day_end)
        zzzz=FE.CSZLFeatureEngineering(Day_start,Day_end,Default_folder_path)


        zzzz.FECB01_real(int(Day_now))
        featurepath="Today_Joinfeature_CB.csv"

        cur_model=CSZLModel.CSZLModel()
        #resultpath2=cur_model.LGBmodelpredict(trainpath,cur_model_path)
        resultpath=cur_model.LGBmodelpredict_CB(featurepath,cur_model_path)
        
        resultpath=cur_model.MixOutputresult_groupbalence_CB(featurepath,cur_model_path,resultpath)    
        
        pass

    def CBBackTesting(self):

        Default_folder_path='D:/temp2/'

        dayA='20130101'#nomal/small
        dayB='20210301'

        dayC='20210301'
        dayD='20220505'

        zzzz=FE.CSZLFeatureEngineering(dayA,dayB,Default_folder_path)
        trainpath=zzzz.FECB01()
        zzzz=FE.CSZLFeatureEngineering(dayC,dayD,Default_folder_path)
        testpath=zzzz.FECB01()

        cur_model=CSZLModel.CSZLModel()

        cur_model_path=cur_model.LGBmodeltrain_CB(trainpath)

        cur_model.LGBmodelpredict_CB(testpath,cur_model_path)

        resultpath=cur_model.MixOutputresult_groupbalence_CB(testpath,cur_model_path)

        curdisplay=CSZLDisplay.CSZLDisplay()
        curdisplay.Topk_nextopen_CB(resultpath)

        pass


    def Todays_action(self,last_path,Today_result_path,changenum_max,singleamout,Auto=False):

        #最高换手个数
        #changenum_max=2
        ##total_amount=2000000
        ##单个买入额
        #singleamout=1000

        #修改显示行列数
        pd.set_option('display.width', 5000)
        pd.set_option('display.max_rows', 500)
        pd.set_option('display.max_columns', 500)

        if False:
            df_stocklist=pd.read_csv(CSZLData.CSZLDataWithoutDate.get_stocklist(),index_col=0,header=0)
        else:
            df_stocklist=pd.read_csv("./Database/stocklist.csv",index_col=0,header=0)
        
        df_stocklist_merge=df_stocklist[['ts_code','name']]
        df_stocklist_merge['ts_code']=df_stocklist_merge['ts_code'].map(lambda x : x[:6])
        df_stocklist_merge['ts_code']=df_stocklist_merge['ts_code'].fillna(0).apply(pd.to_numeric)

        #print(df_stocklist_merge)

        df_last=pd.read_csv(last_path,index_col=0,header=0)

        df_hold=df_last[['ts_code','hold']]

        #print(df_last)

        df=pd.read_csv(Today_result_path,index_col=0,header=0)
        #删除科创版和北交所以及ST含有的股票

        df=df[df['ts_code']<688000]
        df=pd.merge(df, df_stocklist_merge, how='left', on=['ts_code'])
        df=df[~df['name'].str.contains('st|ST',na=False)]
        df['mix_rank'].fillna(-99.99, inplace=True)
        df['num_rank']=df['mix_rank'].rank(pct=False,ascending=False,method='min')

        oldnumbers=df_last.shape[0]

        df_oldcode_set=df[df['ts_code'].isin(df_last['ts_code'])]
        df_oldcode_set=df_oldcode_set.sort_values(by=['num_rank'])
        df_oldcode_set=pd.merge(df_oldcode_set, df_hold, how='left', on=['ts_code'])

        # 排名1000以内的不更换，除非无更换对象
        # 超过更换数量的更换指定数量
        # 当无更换对象时只更换最后一个

        enable_change_df=df_oldcode_set[df_oldcode_set['mix_rank']>-98]
        dddd=enable_change_df[enable_change_df['num_rank']>1000]

        real_change_df = dddd if dddd.shape[0]<changenum_max else dddd.tail(changenum_max)

        if real_change_df.shape[0]==0:
            real_change_df=enable_change_df.tail(1)

        changenum_real=real_change_df.shape[0]
        #print(real_change_df)

        #固定变化后几位
        df_holdset=df_oldcode_set[~df_oldcode_set['ts_code'].isin(real_change_df['ts_code'])]

        del_show=df_oldcode_set[df_oldcode_set['ts_code'].isin(real_change_df['ts_code'])]
        print("===show sell===")
        if Auto:
            print(del_show)
            df_del_sever=del_show[["ts_code","0","19","mix_rank"]]
        else:
            print(del_show[['ts_code','name','num_rank','hold','close_show']])
        print("===show sell end===")

        df_choiceset=df[~df['ts_code'].isin(df_last['ts_code'])]
        #防止无法买入，加的判断条件
        df_choiceset=df_choiceset[df_choiceset['close_show']<(singleamout/100)]

        df_choiceset=df_choiceset.sort_values(by=['num_rank'])
        df_choiceset=df_choiceset.head(changenum_real)
        df_choiceset['hold']=(singleamout/df_choiceset['close_show'])//100 * 100

        print("===show buy===")
        if Auto:
            print(df_choiceset)
            df_add_sever=df_choiceset[["ts_code","0","19","mix_rank"]]
        #else:          
        #    print(del_show[['ts_code','name','num_rank','hold','close_show']])
        print(df_choiceset[['ts_code','hold','close_show','0','name']])
        print("===show buy end===")

        df_newset=df_holdset.append(df_choiceset, ignore_index=True)

        df_newset=df_newset[['ts_code','trade_date','Shift_1total_mv_rank','0','19','num_rank','close_show','hold','name']]
        print(df_newset)
        #df_newset['price']=df_newset['close_show']*df_newset['hold']
        #print(df_newset['price'])

        #是否覆盖
        if Auto:
            df_server=df_del_sever.append(df_add_sever, ignore_index=True)
            print(df_server)
            df_server.to_csv("today_real_remix_result.csv")
            df_newset.to_csv(last_path,encoding='utf-8-sig')

        else:
            print("是否覆盖前日结果 y/n")
            if_cover=input()
            if if_cover=='y' or if_cover=='Y':
                df_newset.to_csv(last_path,encoding='utf-8-sig')
            else:
                df_newset.to_csv("temp_result.csv",encoding='utf-8-sig')

        pass

    def update_all(self):

        Default_folder_path='./temp/'

        zzzz=CSZLData.CSZLData("20220101","20220301")

        zzzz.getDataSet_all(Default_folder_path)

    def Haitong2CSZL(self):
        #获取文件夹里面最后更新的文档

        sourcepath="./Source"
        file_name = CSZLUtils.CSZLUtils.getFlist(sourcepath)
        final_name=file_name[-1]

        #修改显示行列数
        pd.set_option('display.width', 5000)
        pd.set_option('display.max_rows', 500)
        pd.set_option('display.max_columns', 500)

        path = sourcepath+"/"+final_name
        read_excel = pd.read_excel(path,engine='openpyxl')   # 直接使用 read_excel() 方法读取

        #总资产
        total_amount=read_excel.iat[1,7]

        df_today = read_excel.iloc[10:,1:9]

        #证券代码	证券余额	证券可用	冻结数量	最新价	成本价	成本价(港币)	市值
        df_today.columns = ['ts_code','totol','hold','cold','frash','hold_price','hold_price_g','avalue']

        df_today['t'] = df_today['ts_code'].str.isdigit()

        df_today = df_today[df_today['t']!=False]
        df_today['ts_code'] = df_today['ts_code'].astype(int)

        df_today=df_today[(df_today['ts_code']<100000)|((df_today['ts_code']>300000)&(df_today['ts_code']<400000))|((df_today['ts_code']>600000)&(df_today['ts_code']<700000))]
        df_today=df_today[df_today['avalue']>2000]


        df_today.to_csv("last_result_real.csv")


        pass

    def TodayResult2ServerData(self):

        sourcepath="Today_result.csv"

        df=pd.read_csv(sourcepath,index_col=0,header=0)

        ServerData=df[["ts_code","0","19","mix_rank"]]

        ServerData.sort_values(by=['mix_rank'],ascending=False, inplace=True)
        ServerData.dropna(axis=0, how='any', inplace=True)
        print(ServerData)
        ServerData

        ServerData.to_csv("today_real_remix_result.csv")

        pass

    def PredictBackRound(self):

        cur_date=datetime.datetime.now().strftime("%Y-%m-%d")
        change_flag=0
        while(True):
            date=datetime.datetime.now()
            day = date.weekday()
            if(day>4):
                time.sleep(10000)
                continue
                dawd=5


            if(self.TimeCheck()):       


                self.RealTimePredict()
                self.Todays_action('last_result_real.csv',"Today_result.csv",3,7000,True)

                print("Today_over")
                time.sleep(10000) 
        

            print(date)
            time.sleep(10)

    def PKL2CSV(self):

        sourcepath="./transform"
        file_name = CSZLUtils.CSZLUtils.getFlist(sourcepath)

        for file in file_name:

            (filename, extension) = os.path.splitext(file)
            if extension=='.csv':
                continue
            pathread=sourcepath+'/'+file
            df=pd.read_pickle(pathread)

            print(df)

            pathwrite=sourcepath+'/'+filename+'.csv'

            df.to_csv(pathwrite)

            pass

    def TimeCheck(self):
        global CurHour
        global CurMinute



        CurHour=int(time.strftime("%H", time.localtime()))
        CurMinute=int(time.strftime("%M", time.localtime()))

        caltemp=CurHour*100+CurMinute

        #return True

        if (caltemp>=1450 and caltemp<=1500):
            return True
        else:
            return False 