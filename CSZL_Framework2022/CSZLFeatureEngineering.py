#coding=utf-8

import CSZLData
import pandas as pd
import numpy as np
import gc
import os

class CSZLFeatureEngineering(object):
    """负责进行特征工程"""

    def __init__(self,start_date,end_date,Default_folder_path):

        self.start_date=start_date
        self.end_date=end_date
        self.Default_folder_path=Default_folder_path
        self.CSZLDataLoader=CSZLData.CSZLData(start_date,end_date)

        #初始化所有的df
        self.dfDailydata=pd.DataFrame([])
        self.dfAdj_factor=pd.DataFrame([])
        self.dfMoneyflow=pd.DataFrame([])
        self.dfLimit=pd.DataFrame([])
        self.dfLongfactor=pd.DataFrame([])

        pass

    def LoaddfDailydata(self):
        if(self.dfDailydata.empty):
            loadpath=self.CSZLDataLoader.getDataSet(self.Default_folder_path)
            self.dfDailydata=pd.read_csv(loadpath,index_col=0,header=0)

        return self.dfDailydata

    def LoaddfAdj_factor(self):
        if(self.dfAdj_factor.empty):
            loadpath=self.CSZLDataLoader.getDataSet_adj_factor(self.Default_folder_path)
            self.dfAdj_factor=pd.read_csv(loadpath,index_col=0,header=0)

        return self.dfAdj_factor

    def LoaddfMoneyflow(self):
        if(self.dfMoneyflow.empty):
            loadpath=self.CSZLDataLoader.getDataSet_moneyflow(self.Default_folder_path)
            self.dfMoneyflow=pd.read_csv(loadpath,index_col=0,header=0)

        return self.dfMoneyflow

    def LoaddfLimit(self):
        if(self.dfLimit.empty):
            loadpath=self.CSZLDataLoader.getDataSet_stk_limit(self.Default_folder_path)
            self.dfLimit=pd.read_csv(loadpath,index_col=0,header=0)

        return self.dfLimit

    def LoaddfLongfactor(self):
        if(self.dfLongfactor.empty):
            loadpath=self.CSZLDataLoader.getDataSet_long_factor(self.Default_folder_path)
            self.dfLongfactor=pd.read_csv(loadpath,index_col=0,header=0)

        return self.dfLongfactor

    def FE01(self):
        self.create_target()
        self.create_Longfeature(1)
        self.create_Limitfeature()
        self.create_dayfeature()
        self.create_Moneyflowfeature(0)


        pass

    def create_target(self):

        savepath=self.Default_folder_path+"Traget"+self.start_date+"to"+self.end_date+".pkl"
        if(os.path.exists(savepath)==True):
            print("数据集已创建")
            return

        df=self.LoaddfDailydata().copy(deep=True)
        df2=self.LoaddfAdj_factor().copy(deep=True)

        df=pd.merge(df, df2, how='inner', on=['ts_code','trade_date'])

        df['real_price']=df['close']*df['adj_factor']

        df=df[["ts_code","trade_date","real_price"]]

        df=self.PedictDaysRank(df,5)

        df=df[["ts_code","trade_date","tomorrow_chg","tomorrow_chg_rank"]]

        #print(df)
        df.to_pickle(savepath)

        del df,df2
        gc.collect()
        pass

    def create_dayfeature(self):

        savepath=self.Default_folder_path+"Dayfeature"+self.start_date+"to"+self.end_date+".pkl"
        if(os.path.exists(savepath)==True):
            print("数据集已创建")
            return

        df=self.LoaddfDailydata().copy(deep=True)
        df2=self.LoaddfAdj_factor().copy(deep=True)

        df=pd.merge(df, df2, how='inner', on=['ts_code','trade_date'])

        df['real_price']=df['close']*df['adj_factor']

        df=df[["ts_code","trade_date","real_price"]]

        df=self.CloseWithHighLow(df,5)
        df=self.CloseWithHighLow(df,12)
        df=self.CloseWithHighLow(df,25)
        df=self.CloseWithHighLow(df,5,'max')
        df=self.CloseWithHighLow(df,12,'max')
        df=self.CloseWithHighLow(df,25,'max')

        #print(df)
        #df.to_csv("sdddddd.csv")
        df.to_pickle(savepath)

        del df,df2
        gc.collect()
        pass

    def create_Moneyflowfeature(self,shift=0):

        savepath=self.Default_folder_path+"Moneyflowfeature"+self.start_date+"to"+self.end_date+"shift"+str(shift)+".pkl"
        if(os.path.exists(savepath)==True):
            print("数据集已创建")
            return

        df=self.LoaddfMoneyflow().copy(deep=True)

        df.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df['sm_amount']=df['buy_sm_amount']-df['sell_sm_amount']
        df['lg_amount']=df['buy_lg_amount']-df['sell_lg_amount']


        df.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)

        if(shift!=0):
            df['sm_amount']=df.groupby('ts_code')['sm_amount'].shift(shift)
            df['lg_amount']=df.groupby('ts_code')['lg_amount'].shift(shift)
            df['net_mf_amount']=df.groupby('ts_code')['net_mf_amount'].shift(shift)

        df=self.InputChgSum(df,5,'sm_amount')
        df=self.InputChgSum(df,5,'lg_amount')
        df=self.InputChgSum(df,5,'net_mf_amount')

        df=self.InputChgSum(df,12,'sm_amount')
        df=self.InputChgSum(df,12,'lg_amount')
        df=self.InputChgSum(df,12,'net_mf_amount')

        df=self.InputChgSum(df,25,'sm_amount')
        df=self.InputChgSum(df,25,'lg_amount')
        df=self.InputChgSum(df,25,'net_mf_amount')

        print(df)
        #df.to_csv("sdddddd.csv")
        df.to_pickle(savepath)

        del df
        gc.collect()
        pass

    def create_Limitfeature(self):

        savepath=self.Default_folder_path+"Limitfeature"+self.start_date+"to"+self.end_date+".pkl"
        if(os.path.exists(savepath)==True):
            print("数据集已创建")
            return

        df=self.LoaddfLimit().copy(deep=True)

        df['limit_percent']=df['down_limit']/df['up_limit']

        #是否st或其他
        df['st_or_otherwrong']=0
        df.loc[(df['limit_percent']<0.85) & (0.58<df['limit_percent']),'st_or_otherwrong']=1

        df.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        #print(df)
        #df.to_csv("sdddddd.csv")
        df.to_pickle(savepath)

        del df
        gc.collect()
        pass

    def create_Longfeature(self,shift=0):

        savepath=self.Default_folder_path+"Longfeature"+self.start_date+"to"+self.end_date+"shift"+str(shift)+".pkl"
        if(os.path.exists(savepath)==True):
            print("数据集已创建")
            return

        df=self.LoaddfLongfactor().copy(deep=True)

        df.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df['total_mv_rank']=df.groupby('trade_date')['total_mv'].rank(pct=True)
        df['total_mv_rank']=df.groupby('ts_code')['total_mv_rank'].shift(shift)
        df['total_mv_rank']=df['total_mv_rank']*19.9//1


        df['pb_rank']=df.groupby('trade_date')['pb'].rank(pct=True)
        df['pb_rank']=df.groupby('ts_code')['pb_rank'].shift(shift)
        #df['pb_rank']=df['pb_rank']*10//1

        df['circ_mv_pct']=(df['total_mv']-df['circ_mv'])/df['total_mv']
        df['circ_mv_pct']=df.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        df['circ_mv_pct']=df.groupby('ts_code')['circ_mv_pct'].shift(shift)
        #df['circ_mv_pct']=df['circ_mv_pct']*10//1
        
        df['ps_ttm']=df.groupby('trade_date')['ps_ttm'].rank(pct=True)
        df['ps_ttm']=df.groupby('ts_code')['ps_ttm'].shift(shift)

        df.drop(['total_mv','pb','circ_mv'],axis=1,inplace=True)

        df=df.replace(np.nan, 0)
        #print(df)
        df.to_csv("sdddddd.csv")
        dfnames=df.columns.values.tolist()


        df.to_pickle(savepath)
        del df
        gc.collect()
        
        return dfnames

    ######dataframe处理
    def CloseWithHighLow(self,df_all,days,minmax='min'):
        #输入几日和最高或最低返回排名
        #30日最低比值

        stringdisplay=str(days)+'_pct_rank_'+minmax
        if(minmax=='min'):
            xxx=df_all.groupby('ts_code')['real_price'].rolling(days).min().reset_index()
        else:
            xxx=df_all.groupby('ts_code')['real_price'].rolling(days).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_groupagg')

        df_all['groupagg_pct']=(100*(df_all['real_price']+0.001-df_all['real_price_groupagg']))/df_all['real_price_groupagg']

        #TODO这里可能不用rank会好一点
        df_all[stringdisplay]=df_all['groupagg_pct']
        #df_all[stringdisplay]=df_all.groupby('trade_date')['groupagg_pct'].rank(pct=True)

        df_all.drop(['groupagg_pct','real_price_groupagg'],axis=1,inplace=True)

        return df_all

    def InputChgSum(self,df_all,days,sumlinename,intflag=False):

        bufferbak='_'+str(days)
        stringdisplay=sumlinename+'_'+str(days)

        xxx=df_all.groupby('ts_code')[sumlinename].rolling(days).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        #TODO这里可能不用rank会好一点
        #df_all[stringdisplay]=df_all.groupby('trade_date')[stringdisplay].rank(pct=True)

        return df_all

    def PedictDaysRank(self,df_all,days):

        nextstart=df_all.groupby('ts_code')['real_price'].shift(0)
        nextnstart=df_all.groupby('ts_code')['real_price'].shift(0-days)

        df_all['tomorrow_chg']=((nextnstart-nextstart)/nextstart)*100

        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*19.9//1

        return df_all


    def create_dayline(self):

        df=self.LoaddfDailydata().copy(deep=True)

        print(df)


        del df
        pass

