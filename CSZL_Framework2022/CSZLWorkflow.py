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
#import xlrd


class CSZLWorkflow(object):
    """各种workflow 主要就是back testing"""

    def BackTesting(self):

        #Default_folder_path='./temp/'
        Default_folder_path='D:/temp2/'

        df_current_indexweight_merge=self.getcurrent_indexweight_all_merge(False)

        #zzzz=CSZLData.CSZLData("20220101","20220301")

        #zzzz.getDataSet_all(Default_folder_path)

        backtestmode='nomalreverse'

        #dayA='20150101'
        #dayB='20230401'
        #dayC='20221231'
        #dayD='20230317'
        #dayD='20230425'
        dayA='20130101'
        dayB='20180601'
        dayC='20180601'
        dayD='20230425'


        if backtestmode=='reverse':
            dayA='20180601'
            dayB='20220817'
            dayC='20130101'
            dayD='20180601'

        elif backtestmode=='little':
            dayA='20200101'
            dayB='20200601'
            dayC='20220101'
            dayD='20220817'

        elif backtestmode=='nomal':
            dayA='20130101'
            dayB='20180601'
            dayC='20180601'
            dayD='20230425'
        elif backtestmode=='nomalreverse':
            dayA='20180601'
            dayB='20230425'
            dayC='20130101'
            dayD='20180601'

        elif backtestmode=='small':
            dayA='20170101'
            dayB='20200601'
            dayC='20200601'
            dayD='20220817'

        elif backtestmode=='smallreverse':
            dayA='20200601'
            dayB='20220817'
            dayC='20170101'
            dayD='20200601'

        elif backtestmode=='real':
            dayA='20140601'
            dayB='20220801'
            dayC='20220501'
            dayD='20221028'

            dayA='20150101'
            dayB='20221231'
            dayC='20220501'
            dayD='20221231'

            dayA='20150101'
            dayB='20230428'
            dayC='20230501'
            dayD='20231231'

        zzzz=FE.CSZLFeatureEngineering(dayA,dayB,Default_folder_path)
        trainpath=zzzz.FE09d()

        zzzz=FE.CSZLFeatureEngineering(dayC,dayD,Default_folder_path)
        testpath=zzzz.FE09d()


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
        #curdisplay.Topk_nextopen_trend(resultpath)
        curdisplay.Topk_nextopen_addopt(resultpath)

        pass

    def BackTesting_compare(self):

        #Default_folder_path='./temp/'
        Default_folder_path='D:/temp2/'

        df_current_indexweight_merge=self.getcurrent_indexweight_all_merge(False)

        #zzzz=CSZLData.CSZLData("20220101","20220301")

        #zzzz.getDataSet_all(Default_folder_path)

        backtestmode='small'

        dayA='20150101'
        dayB='20221231'
        dayC='20220501'
        dayD='20221231'

        if backtestmode=='reverse':
            dayA='20180601'
            dayB='20220817'
            dayC='20130101'
            dayD='20180601'

        elif backtestmode=='little':
            dayA='20200101'
            dayB='20200601'
            dayC='20220101'
            dayD='20220817'

        elif backtestmode=='nomal':
            dayA='20130101'
            dayB='20180601'
            dayC='20180601'
            dayD='20220817'

        elif backtestmode=='small':
            dayA='20170101'
            dayB='20200601'
            dayC='20200601'
            dayD='20220817'

        elif backtestmode=='smallreverse':
            dayA='20200601'
            dayB='20220817'
            dayC='20170101'
            dayD='20200601'

        elif backtestmode=='real':
            dayA='20140601'
            dayB='20220801'
            dayC='20220501'
            dayD='20221028'

            dayA='20150101'
            dayB='20221231'
            dayC='20220501'
            dayD='20221231'

        zzzz=FE.CSZLFeatureEngineering(dayA,dayB,Default_folder_path)
        trainpath=zzzz.FE09d()

        zzzz=FE.CSZLFeatureEngineering(dayC,dayD,Default_folder_path)
        testpath=zzzz.FE09d()


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
        #curdisplay.Topk_nextopen_trend(resultpath)
        curdisplay.Topk_nextopen_addopt_comp(resultpath)

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
        #dayD='20220506'

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

    def BackTesting_static_0515(self):

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
        #dayD='20220517'

        zzzz=FE.CSZLFeatureEngineering(dayA,dayB,Default_folder_path)
        trainpath=zzzz.FE05()

        zzzz=FE.CSZLFeatureEngineering(dayC,dayD,Default_folder_path)
        testpath=zzzz.FE05()

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

    def BackTesting_static_0814(self):

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

        dayA='20130101'#nomal/small
        dayB='20220805'


        #dayA='20150801'#nomal/small
        #dayB='20220425'
        dayC=Day_start
        dayD=Day_now
        #dayD='20220517'

        zzzz=FE.CSZLFeatureEngineering(dayA,dayB,Default_folder_path)
        trainpath=zzzz.FE05()

        zzzz=FE.CSZLFeatureEngineering(dayC,dayD,Default_folder_path)
        testpath=zzzz.FE05()

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

    def BackTesting_static_0828(self):

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

        dayA='20140601'#nomal/small
        dayB='20220801'


        #dayA='20150801'#nomal/small
        #dayB='20220425'
        dayC=Day_start
        dayD=Day_now
        #dayD='20220826'

        zzzz=FE.CSZLFeatureEngineering(dayA,dayB,Default_folder_path)
        trainpath=zzzz.FE09d()

        zzzz=FE.CSZLFeatureEngineering(dayC,dayD,Default_folder_path)
        testpath=zzzz.FE09d()

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

    def BackTesting_static_230502(self):

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

        dayA='20150101'#nomal/small
        dayB='20230401'


        #dayA='20150801'#nomal/small
        #dayB='20220425'
        dayC=Day_start
        dayD=Day_now
        #dayD='20220826'

        zzzz=FE.CSZLFeatureEngineering(dayA,dayB,Default_folder_path)
        trainpath=zzzz.FE09d()

        zzzz=FE.CSZLFeatureEngineering(dayC,dayD,Default_folder_path)
        testpath=zzzz.FE09d()

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
        cur_model_path="./temp2/FE0520150801to20220425_0/LGBmodeltrainLGBmodel_003"
        #cur_model_path="./temp2/FE09c20140601to20220801_0/LGBmodeltrainLGBmodel_003"

        #是否需要重新生成
        if False:
            #zzzz=FE.CSZLFeatureEngineering("20190101","20210101",Default_folder_path)
            #trainpath=zzzz.FE03()
            zzzz=FE.CSZLFeatureEngineering("20140601","20220801",Default_folder_path)
            trainpath=zzzz.FE05()
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

        zzzz.FE05_real(int(Day_now))
        featurepath="Today_Joinfeature.csv"

        cur_model=CSZLModel.CSZLModel()
        #resultpath2=cur_model.LGBmodelpredict(trainpath,cur_model_path)
        resultpath=cur_model.LGBmodelpredict(featurepath,cur_model_path)
        
        resultpath=cur_model.MixOutputresult_groupbalence(featurepath,cur_model_path,resultpath)    
        


        pass

    def RealTimePredict_FE09c(self):

        Default_folder_path='./temp2/'
        #Default_folder_path='D:/temp2/'

        #cur_model_path="D:/temp2/FE0320190101to20210101_0/LGBmodeltrainLGBmodel_003"
        #cur_model_path="D:/temp2/FE0320150801to20220425_0/LGBmodeltrainLGBmodel_003"
        #cur_model_path="./temp2/FE0520150801to20220425_0/LGBmodeltrainLGBmodel_003"
        cur_model_path="./temp2/FE09c20140601to20220801_0/LGBmodeltrainLGBmodel_003"

        #是否需要重新生成
        if False:
            #zzzz=FE.CSZLFeatureEngineering("20190101","20210101",Default_folder_path)
            #trainpath=zzzz.FE03()
            zzzz=FE.CSZLFeatureEngineering("20140601","20220801",Default_folder_path)
            trainpath=zzzz.FE09c()
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

        zzzz.FE09c_real(int(Day_now))
        featurepath="Today_Joinfeature.csv"

        cur_model=CSZLModel.CSZLModel()
        #resultpath2=cur_model.LGBmodelpredict(trainpath,cur_model_path)
        resultpath=cur_model.LGBmodelpredict(featurepath,cur_model_path)
        
        resultpath=cur_model.MixOutputresult_groupbalence(featurepath,cur_model_path,resultpath)    
        


        pass

    def RealTimePredict_FE09d(self):

        Default_folder_path='./temp2/'
        #Default_folder_path='D:/temp2/'

        #cur_model_path="D:/temp2/FE0320190101to20210101_0/LGBmodeltrainLGBmodel_003"
        #cur_model_path="D:/temp2/FE0320150801to20220425_0/LGBmodeltrainLGBmodel_003"
        #cur_model_path="./temp2/FE0520150801to20220425_0/LGBmodeltrainLGBmodel_003"
        #cur_model_path="./temp2/FE09c20140601to20220801_0/LGBmodeltrainLGBmodel_003"
        cur_model_path="./temp2/FE09d20150101to20230401_0/LGBmodeltrainLGBmodel_003"

        #是否需要重新生成
        if False:
            #zzzz=FE.CSZLFeatureEngineering("20190101","20210101",Default_folder_path)
            #trainpath=zzzz.FE03()
            zzzz=FE.CSZLFeatureEngineering("20150101","20230401",Default_folder_path)
            trainpath=zzzz.FE09c()
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

        zzzz.FE09d_real(int(Day_now))
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
        cur_model_path="./temp2/FECB0320130101to20220501_0/LGBmodeltrain_CBLGBmodel_003"
        #是否需要重新生成
        if False:
            dayA='20130101'#nomal/small
            dayB='20220501'
            zzzz=FE.CSZLFeatureEngineering(dayA,dayB,Default_folder_path)
            trainpath=zzzz.FECB02()
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


        zzzz.FECB03_real(int(Day_now))
        featurepath="Today_Joinfeature_CB.csv"

        cur_model=CSZLModel.CSZLModel()
        #resultpath2=cur_model.LGBmodelpredict(trainpath,cur_model_path)
        resultpath=cur_model.LGBmodelpredict_CB(featurepath,cur_model_path)
        
        resultpath=cur_model.MixOutputresult_groupbalence_CB(featurepath,cur_model_path,resultpath)    
        
        pass

    def CBBackTesting(self):

        Default_folder_path='D:/temp2/'

        dayA='20130101'#nomal/small
        dayB='20200101'

        dayC='20200101'
        dayD='20220505'
        dayD='20220722'
        #dayD='20220506'
        #dayD='20220513'

        zzzz=FE.CSZLFeatureEngineering(dayA,dayB,Default_folder_path)
        trainpath=zzzz.FECB03()
        zzzz=FE.CSZLFeatureEngineering(dayC,dayD,Default_folder_path)
        testpath=zzzz.FECB03()

        cur_model=CSZLModel.CSZLModel()

        cur_model_path=cur_model.LGBmodeltrain_CB(trainpath)

        cur_model.LGBmodelpredict_CB(testpath,cur_model_path)

        resultpath=cur_model.MixOutputresult_groupbalence_CB(testpath,cur_model_path)

        curdisplay=CSZLDisplay.CSZLDisplay()
        curdisplay.Topk_nextopen_CB(resultpath)

        pass

    def CBBackTesting_static_0508(self):

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


        dayA='20130101'#nomal/small
        dayB='20220301'

        dayC=Day_start
        dayD=Day_now

        zzzz=FE.CSZLFeatureEngineering(dayA,dayB,Default_folder_path)
        trainpath=zzzz.FECB02()
        zzzz=FE.CSZLFeatureEngineering(dayC,dayD,Default_folder_path)
        testpath=zzzz.FECB02()

        cur_model=CSZLModel.CSZLModel()

        cur_model_path=cur_model.LGBmodeltrain_CB(trainpath)

        cur_model.LGBmodelpredict_CB(testpath,cur_model_path)

        resultpath=cur_model.MixOutputresult_groupbalence_CB(testpath,cur_model_path)

        today_df = pd.read_csv(resultpath,index_col=0,header=0)

        lastday=today_df['trade_date'].max()
        today_df['ts_code']=today_df['ts_code'].apply(lambda x : x[:-3])
        copy_df=today_df[today_df['trade_date']==lastday]
        copy_df.to_csv("Today_NEXT_predict_CB.csv")

        #curdisplay=CSZLDisplay.CSZLDisplay()
        #curdisplay.Topk_nextopen_CB(resultpath)

        pass

    def CBBackTesting_static_0515(self):

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


        dayA='20130101'#nomal/small
        dayB='20220501'

        dayC=Day_start
        dayD=Day_now
        #dayD='20220517'

        zzzz=FE.CSZLFeatureEngineering(dayA,dayB,Default_folder_path)
        trainpath=zzzz.FECB03()
        zzzz=FE.CSZLFeatureEngineering(dayC,dayD,Default_folder_path)
        testpath=zzzz.FECB03()

        cur_model=CSZLModel.CSZLModel()

        cur_model_path=cur_model.LGBmodeltrain_CB(trainpath)

        cur_model.LGBmodelpredict_CB(testpath,cur_model_path)

        resultpath=cur_model.MixOutputresult_groupbalence_CB(testpath,cur_model_path)

        today_df = pd.read_csv(resultpath,index_col=0,header=0)

        lastday=today_df['trade_date'].max()
        today_df['ts_code']=today_df['ts_code'].apply(lambda x : x[:-3])
        copy_df=today_df[today_df['trade_date']==lastday]
        copy_df.to_csv("Today_NEXT_predict_CB.csv")

        #curdisplay=CSZLDisplay.CSZLDisplay()
        #curdisplay.Topk_nextopen_CB(resultpath)

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

        #添加个指数成分权重

        df_current_indexweight_merge=self.getcurrent_indexweight_all_merge()

        if True:
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

        #加上成分股表示
        df_indexw=pd.merge(df, df_current_indexweight_merge, how='left', on=['ts_code'])
        print(df_indexw)
        df_indexw.to_csv("Today_NEXT_predict_withindex.csv",encoding='utf-8-sig')

        #删除科创版和北交所以及ST含有的股票
        #df=df[df['ts_code']<688000]
        df=pd.merge(df, df_stocklist_merge, how='left', on=['ts_code'])
        df=df[~df['name'].str.contains('st|ST',na=False)]
        df['mix_rank'].fillna(-99.99, inplace=True)
        df['num_rank']=df['mix_rank'].rank(pct=False,ascending=False,method='min')

        df_web_show=df[['ts_code','name','num_rank','close_show','Shift_1total_mv_rank','trade_date','0','19']]
        df_web_show=df_web_show.sort_values(by=['num_rank'])

        #打印今天完整的预测
        df_web_show.to_csv("Today_NEXT_predict_ALL.csv",encoding='utf-8-sig')

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
        
        df_newset_show=pd.merge(df_newset, df_current_indexweight_merge, how='left', on=['ts_code'])

        print(df_newset_show)

        df_newset_show.to_csv("Today_NEXT_predict_HOLD.csv",encoding='utf-8-sig')
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

    def Todays_action_CB(self,last_path,Today_result_path,changenum_max,singleamout,Auto=False):

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
            df_stocklist=pd.read_csv(CSZLData.CSZLDataWithoutDate.get_cb_basic(),index_col=0,header=0)
        else:
            df_stocklist=pd.read_csv("./Database/cb_basic.csv",index_col=0,header=0)
        
        df_stocklist_merge=df_stocklist[['ts_code','bond_short_name','stk_short_name']]
        df_stocklist_merge['ts_code']=df_stocklist_merge['ts_code'].map(lambda x : x[:6])
        df_stocklist_merge['ts_code']=df_stocklist_merge['ts_code'].fillna(0).apply(pd.to_numeric)

        #print(df_stocklist_merge)

        df_last=pd.read_csv(last_path,index_col=0,header=0)

        df_hold=df_last[['ts_code','hold']]

        #print(df_last)

        df=pd.read_csv(Today_result_path,index_col=0,header=0)
        #删除科创版和北交所以及ST含有的股票

        #df=df[df['ts_code']<200000]
        #df=df[df['ts_code']>100000]
        df=pd.merge(df, df_stocklist_merge, how='left', on=['ts_code'])
        df=df[~df['stk_short_name'].str.contains('st|ST',na=False)]
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
        dddd=enable_change_df[enable_change_df['num_rank']>50]

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
            print(del_show[['ts_code','bond_short_name','num_rank','hold','close_show']])
        print("===show sell end===")

        df_choiceset=df[~df['ts_code'].isin(df_last['ts_code'])]
        #防止无法买入，加的判断条件
        df_choiceset=df_choiceset[df_choiceset['close_show']<(singleamout/10)]
        df_choiceset=df_choiceset[df_choiceset['close_show']<115]

        amountlimit=1000
        if CSZLUtils.CSZLUtils.TimeUpper(1430):
            amountlimit=1000
        elif CSZLUtils.CSZLUtils.TimeUpper(1330):
            amountlimit=700
        elif CSZLUtils.CSZLUtils.TimeUpper(1030):
            amountlimit=500
        elif CSZLUtils.CSZLUtils.TimeUpper(930):
            amountlimit=300

        df_choiceset=df_choiceset[df_choiceset['amount_show']>amountlimit]

        df_choiceset=df_choiceset.sort_values(by=['num_rank'])
        df_choiceset=df_choiceset.head(changenum_real)
        df_choiceset['hold']=(singleamout/df_choiceset['close_show'])//10 * 10

        print("===show buy===")
        if Auto:
            print(df_choiceset)
            df_add_sever=df_choiceset[["ts_code","0","19","mix_rank"]]
        #else:          
        #    print(del_show[['ts_code','name','num_rank','hold','close_show']])
        print(df_choiceset[['ts_code','hold','close_show','0','bond_short_name']])
        print("===show buy end===")

        df_newset=df_holdset.append(df_choiceset, ignore_index=True)

        df_newset=df_newset[['ts_code','trade_date','0','19','num_rank','close_show','hold','bond_short_name']]
        print(df_newset)
        #df_newset['price']=df_newset['close_show']*df_newset['hold']
        #print(df_newset['price'])

        #是否覆盖
        if Auto:
            df_server=df_del_sever.append(df_add_sever, ignore_index=True)
            print(df_server)
            df_server.to_csv("today_real_remix_result_CB.csv")
            df_newset.to_csv(last_path,encoding='utf-8-sig')

        else:
            print("是否覆盖前日结果 y/n")
            if_cover=input()
            if if_cover=='y' or if_cover=='Y':
                df_newset.to_csv(last_path,encoding='utf-8-sig')
            else:
                df_newset.to_csv("temp_result_CB.csv",encoding='utf-8-sig')

        pass

    def getcurrent_indexweight(self,indexcode='000905.SH'):

        if(False):
            #000001.SH 上证 000016.SH 50 000688.SH 科创50 000905.SH 中证500 399006.SZ 创业板指
            #399300.SZ 300 000300.SH 300 000852.SH 1000 
            CSZLData.CSZLDataWithoutDate.get_index_weight(indexcode)


        index_path='./Database/indexdata/'+indexcode+'weight.csv'
        index_indexweight=pd.read_csv(index_path,index_col=0,header=0)

        index_indexweight=index_indexweight.sort_values(by=['trade_date'],ascending=False)
        index_indexweight.reset_index(inplace=True,drop=True)

        datelist=index_indexweight['trade_date'].unique()
        curmonth=datelist[0]

        index_indexweight=index_indexweight[index_indexweight["trade_date"]==curmonth]

        return index_indexweight

    def getcurrent_indexweight_all(self):

        df_indexw=self.getcurrent_indexweight('000905.SH')

        #df_indexw=df_indexw.append(self.getcurrent_indexweight('000016.SH'))
        df_indexw=df_indexw.append(self.getcurrent_indexweight('000300.SH'))
        df_indexw=df_indexw.append(self.getcurrent_indexweight('399006.SZ'))

        df_indexw.reset_index(inplace=True,drop=True)

        return df_indexw

    def getcurrent_indexweight_all_merge(self,changets_code=True):

        current_indexweight=self.getcurrent_indexweight_all()

        current_indexweight=current_indexweight.rename(columns={'con_code':'ts_code',})
        df_current_indexweight_merge=current_indexweight[['ts_code','index_code']]
        if(changets_code):
            df_current_indexweight_merge['ts_code']=df_current_indexweight_merge['ts_code'].map(lambda x : x[:6])
            df_current_indexweight_merge['ts_code']=df_current_indexweight_merge['ts_code'].fillna(0).apply(pd.to_numeric)

        df_current_indexweight_merge.loc[df_current_indexweight_merge["index_code"]=="000300.SH", "index_code"] = "沪深300"
        df_current_indexweight_merge.loc[df_current_indexweight_merge["index_code"]=="000905.SH", "index_code"] = "中证500"
        df_current_indexweight_merge.loc[df_current_indexweight_merge["index_code"]=="000016.SH", "index_code"] = "上证50"
        df_current_indexweight_merge.loc[df_current_indexweight_merge["index_code"]=="399006.SZ", "index_code"] = "创业版"

        df_current_indexweight_merge.drop_duplicates('ts_code',inplace = True)
        df_current_indexweight_merge.to_csv("current_indexweight_merge.csv",encoding='utf-8-sig')

        return df_current_indexweight_merge

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

    def Zhaoshang2CSZL(self):
        #获取文件夹里面最后更新的文档

        #sourcepath="./Source"
        #file_name = CSZLUtils.CSZLUtils.getFlist(sourcepath)
        #final_name=file_name[-1]

        #修改显示行列数
        pd.set_option('display.width', 5000)
        pd.set_option('display.max_rows', 500)
        pd.set_option('display.max_columns', 500)

        #path = sourcepath+"/"+final_name

        path = "./Source/bbb.csv"
        read_df = pd.read_csv(path)
        #read_excel = pd.read_csv(path,index_col=0,header=0)
        #read_excel = xlrd.open_workbook(path)
        #read_excel = pd.read_excel(path,engine='openpyxl')   # 直接使用 read_excel() 方法读取

        print(read_df)

        usecol=['证券代码','可卖数量','成本金额']

        df_today=read_df[usecol]
        print(df_today)

        #证券代码	证券余额	证券可用	冻结数量	最新价	成本价	成本价(港币)	市值
        df_today.columns = ['ts_code','hold','avalue']

        #df_today['t'] = df_today['ts_code'].str.isdigit()

        #df_today = df_today[df_today['t']!=False]
        #df_today['ts_code'] = df_today['ts_code'].astype(int)

        df_today=df_today[(df_today['ts_code']<100000)|((df_today['ts_code']>300000)&(df_today['ts_code']<400000))|((df_today['ts_code']>600000)&(df_today['ts_code']<700000))]
        df_today=df_today[df_today['avalue']>2000]

        print(df_today)

        df_today.to_csv("last_result_real.csv")


        pass

    def Zhaoshang2CSZL_CB(self):
        #获取文件夹里面最后更新的文档

        #sourcepath="./Source"
        #file_name = CSZLUtils.CSZLUtils.getFlist(sourcepath)
        #final_name=file_name[-1]

        #修改显示行列数
        pd.set_option('display.width', 5000)
        pd.set_option('display.max_rows', 500)
        pd.set_option('display.max_columns', 500)

        #path = sourcepath+"/"+final_name

        path = "./Source/aaa.csv"
        read_df = pd.read_csv(path)
        #read_excel = pd.read_csv(path,index_col=0,header=0)
        #read_excel = xlrd.open_workbook(path)
        #read_excel = pd.read_excel(path,engine='openpyxl')   # 直接使用 read_excel() 方法读取

        #print(read_df)

        usecol=['证券代码','可卖数量','成本金额']

        df_today=read_df[usecol]
        #print(df_today)

        #证券代码	证券余额	证券可用	冻结数量	最新价	成本价	成本价(港币)	市值
        df_today.columns = ['ts_code','hold','avalue']

        #df_today['t'] = df_today['ts_code'].str.isdigit()

        #df_today = df_today[df_today['t']!=False]
        #df_today['ts_code'] = df_today['ts_code'].astype(int)

        df_today=df_today[(df_today['ts_code']<200000)&(df_today['ts_code']>100000)]
        df_today=df_today[df_today['avalue']>2000]
        df_today=df_today[(df_today['ts_code']<131000)|(df_today['ts_code']>132000)]
        df_today['hold']=100


        #print(df_today)

        df_today.to_csv("last_result_real_CB_ZS.csv")


        pass

    def Haitong2CSZL_CB(self):
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

        df_today=df_today[(df_today['ts_code']<200000)&(df_today['ts_code']>100000)]
        df_today=df_today[df_today['avalue']>2000]


        df_today.to_csv("last_result_real_CB.csv")


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


                self.RealTimePredict_FE09d()
                self.Todays_action('last_result_real.csv',"Today_result.csv",3,10000,True)

                print("Today_over")
                time.sleep(10000) 
        

            print(date)
            time.sleep(10)


    def PredictBackRound_CB(self):

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

                self.RealTimePredict_CB()
                self.Todays_action_CB('last_result_real_CB.csv',"Today_result_CB.csv",4,7000,True)

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

        if (caltemp>=1452 and caltemp<=1500):
            return True
        else:
            return False 