#coding=utf-8
import pandas as pd
import numpy as np
import CSZLData

import matplotlib
import matplotlib.pyplot as plt

from sklearn.utils import shuffle

class CSZLDisplay(object):
    """description of class"""


    def Topk_nextopen(self,resultpath):

        #df_all = pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)
        #df_adj_all=pd.read_csv('./Database/Daily_adj_factor.csv',index_col=0,header=0)
        #df_limit_all=pd.read_csv('./Database/Daily_stk_limit.csv',index_col=0,header=0)
        
        df_all = pd.read_pickle('./Database/Dailydata.pkl')
        df_adj_all = pd.read_pickle('./Database/Daily_adj_factor.pkl')
        df_limit_all = pd.read_pickle('./Database/Daily_stk_limit.pkl')

        df_all=pd.merge(df_all, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='left', on=['ts_code','trade_date'])

        score_df = pd.read_csv(resultpath,index_col=0,header=0)
        #score_df=score_df[['ts_code','trade_date','mix']]
        score_df=score_df[['ts_code','trade_date','mix','Shift_1total_mv_rank']]

        #score_df = pd.read_csv('zzzzfackdatapred_fullold.csv',index_col=0,header=0)
        
        #print(df_all)
        print(score_df)

        #hold_all=5
        #change_num=1
        hold_all=100
        change_num=20
        account=100000000
        accountbase=account
        buy_pct=0.9
        Trans_cost=0.997        #千三
        # balance random none
        choicepolicy="balance"

        ###添加停牌计算和涨跌停简单策略

        #stop_state             当日不停牌为0，当日停牌为1 (TODO:前日停牌本日不停牌为2,不每日刷新)，每日刷新
        #control_state_open     当日不停牌且开盘未触及涨跌停为0，当日开盘触及跌停为1，当日开盘触及涨停为2，每日刷新
        #control_state_close    当日不停牌且收盘没有触及涨跌停为0，当日收盘触及跌停1，当日收盘触及涨停为2,每日刷新
        #last_action_flag       前日不需要买入卖出为0，前日需要卖出为1，前日需要买入为2

        codelist=pd.DataFrame(columns=('ts_code','lastprice','buy_amount','last_adj_factor','last_action_flag'))
        codelist_buffer=pd.DataFrame(columns=('ts_code','lastprice','buy_amount','last_adj_factor','last_action_flag'))
        #codelist=codelist.append([{'ts_code':1,'lastprice':1,'amount':1,'adjflag':1}])
        #print(codelist)

        score_df=score_df.sort_values(by=['trade_date'])
        datelist=score_df['trade_date'].unique()
        cur_hold_num=0
        print(datelist)
    

        days=0
        show3=[]

        last_cur_merge_df=[]

        for cur_date in datelist:

            #这里注意停牌的不包含在这个list中
            cur_df_all=df_all[df_all['trade_date'].isin([cur_date])]

            cur_score_df=score_df[score_df['trade_date'].isin([cur_date])]
            cur_merge_df=pd.merge(cur_df_all,cur_score_df, how='left', on=['trade_date','ts_code'])

            cur_merge_df['mix'].fillna(-99.99, inplace=True)
            if len(last_cur_merge_df):
                cur_merge_df=pd.merge(cur_merge_df,last_cur_merge_df, how='left', on=['ts_code'])
                cur_merge_df['last_mix'].fillna(-99.99, inplace=True)
            
            #if cur_date>20180102 :
            #    cur_merge_df=cur_merge_df.to_csv("dsdf.csv")

            code_value_sum=0
            if codelist.shape[0]>0 :

                codelist_buffer=pd.merge(codelist,cur_merge_df, how='left', on=['ts_code'])
                #刷新停牌的close和adj价值
                codelist_buffer['adj_factor'].fillna(9999.99, inplace=True)
                codelist_buffer['close'].fillna(9999.99, inplace=True)
                codelist_buffer['open'].fillna(9999.99, inplace=True)
                codelist_buffer['control_state_open']=0
                codelist_buffer['control_state_close']=0

                codelist_buffer['stop_state']=0
                codelist_buffer.loc[codelist_buffer['adj_factor']==9999.99,'stop_state']=1
                codelist_buffer.loc[codelist_buffer['open']==codelist_buffer['down_limit'],'control_state_open']=1
                codelist_buffer.loc[codelist_buffer['open']==codelist_buffer['up_limit'],'control_state_open']=2
                codelist_buffer.loc[codelist_buffer['close']==codelist_buffer['down_limit'],'control_state_close']=1
                codelist_buffer.loc[codelist_buffer['close']==codelist_buffer['up_limit'],'control_state_close']=2

                codelist_buffer.loc[codelist_buffer['adj_factor']==9999.99,'adj_factor']=codelist_buffer['last_adj_factor']
                codelist_buffer.loc[codelist_buffer['open']==9999.99,'open']=codelist_buffer['lastprice']
                
                ###更新除权
                ##print(codelist_buffer.head(10))
                codelist_buffer.loc[:,'buy_amount']=codelist_buffer['buy_amount']*codelist_buffer['adj_factor']/codelist_buffer['last_adj_factor']

                #codelist_buffer.loc[:,'last_adj_factor']=codelist_buffer['adj_factor']
                #codelist_buffer.loc[:,'last_adj_factor']=codelist_buffer['adj_factor']

                #print(codelist_buffer.head(10))
                codelist.loc[:,'buy_amount']=codelist_buffer['buy_amount']
                codelist.loc[:,'last_adj_factor']=codelist_buffer['adj_factor']
                codelist.loc[:,'lastprice']=codelist_buffer['open']

                codelist_buffer['value']=codelist_buffer['buy_amount']*codelist_buffer['open']
                #codelist_buffer.reset_index(inplace=True,drop=True)
                
                #code_value_sum=codelist_buffer['value'].sum()

            #todo fillna
            #pd.merge(df_all, df_long_all, how='inner', on=['ts_code','trade_date'])
            #print(cur_merge_df)

            #sell==========================
            sellto=hold_all-change_num
            sellnum=cur_hold_num-sellto

            if sellnum>0:

                #初始化本日卖出flag sell_value 每日刷新
                #初始化本日卖出计数
                codelist_buffer['sell_value']=0
                #sell_count=0
                #先看open是否为2，是则消除前日的卖出flag
                #codelist_buffer.loc[codelist_buffer['control_state_open']==2,'last_action_flag']=0
                #按open更新当日的sell_value，并且增加计数
                #(前日卖出flag为1，当日open是1，当日close不是1,这种情况按score来算)
                #see=codelist_buffer[codelist_buffer['last_action_flag']==1].shape[0]
                #if(see>0):
                #    print(codelist_buffer)

                #codelist_buffer.loc[(codelist_buffer['last_action_flag']==1)&(codelist_buffer['control_state_open']!=1),'sell_value']=codelist_buffer['open']*codelist_buffer['buy_amount']
                #codelist_buffer.loc[(codelist_buffer['last_action_flag']==1)&(codelist_buffer['control_state_close']!=1),'sell_value']=codelist_buffer['open']*codelist_buffer['buy_amount']
                #sell_count=codelist_buffer[codelist_buffer['sell_value']>0].shape[0]
                #if(sell_count!=0):
                #    print(codelist_buffer)
                #sellnum=sellnum-sell_count
                #根据分数排序
                codelist_buffer=codelist_buffer.sort_values(by=['last_mix'])
                
                #先将这些分数低的更新last_action_flag为1
                codelist_buffer.loc[codelist_buffer['ts_code'].isin(codelist_buffer['ts_code'].head(sellnum)),'last_action_flag']=1
                codelist.loc[codelist['ts_code'].isin(codelist_buffer['ts_code'].head(sellnum)),'last_action_flag']=1

                #更新当日control_state_close不跌停的的sell_value
                codelist_buffer.loc[(codelist_buffer['last_action_flag']==1)&(codelist_buffer['control_state_open']!=1),'sell_value']=codelist_buffer['value']
                codelist_buffer.loc[(codelist_buffer['last_action_flag']==1)&(codelist_buffer['control_state_open']==1)&(codelist_buffer['control_state_close']!=1),'sell_value']=codelist_buffer['close']*codelist_buffer['buy_amount']


                #排除跌停卖出
                #统计所有的sell_value大于0的并drop掉更新list
                account=account+codelist_buffer['sell_value'].sum()*Trans_cost
                cur_hold_num-=codelist_buffer[codelist_buffer['sell_value']>0].shape[0]
                #if(cur_hold_num!=80):
                #    print(codelist_buffer)

                #codelist_buffer.drop(codelist_buffer['sell_value']>0,inplace=True)
                codelist_buffer=codelist_buffer[codelist_buffer['sell_value']==0]
                codelist=codelist[codelist['ts_code'].isin(codelist_buffer['ts_code'])]

                sdfafa=1

            #buy==========================
            buyto=hold_all
            buynum=buyto-cur_hold_num

            if(buynum>0 and len(last_cur_merge_df)):

                buy_all_value=0
                if(codelist.shape[0]>0):
                    hold_code_sum=codelist_buffer['value'].sum()
                    buy_all_value=(account+hold_code_sum)*buy_pct-hold_code_sum

                else:
                    buy_all_value=account*buy_pct

                #when account too low then don't do anything
                if(buy_all_value<10000):
                    continue

                code_amount_buy=buy_all_value/buynum

                cur_merge_df=cur_merge_df.sort_values(by=['last_mix'])

                buylist=cur_merge_df
                #single code no repeat
                buylist=buylist[~buylist['ts_code'].isin(codelist['ts_code'])]

                #todo can't buy highstop
                #buylist=buylist[buylist['pct_chg']<4]
                buylist=buylist[buylist['open']!=buylist['up_limit']]
                #buylist=buylist[buylist['pct_chg']>-9]


                if choicepolicy=="random":
                    buylist = shuffle(buylist,random_state=4)
                elif choicepolicy=="balance":
                    headnum=buynum/20+1
                    test=buylist.groupby('Shift_1total_mv_rank').tail(headnum)
                    #print(test)
                    buylist=test.sort_values(by=['last_mix'])


                buylist=buylist.tail(buynum)

                buylist.loc[:,'buyuse']=code_amount_buy/buylist['open']
                #buylist['buyuse']=code_amount_buy/buylist['close']
                buylist.loc[:,'buyuse']=buylist['buyuse'].round(-2)
                buylist.loc[:,'buyuse']=buylist['buyuse'].astype(int)
                buylist['value']=buylist['open']*buylist['buyuse']

                #seelist=buylist[['ts_code','trade_date','yesterday_1total_mv_rank']]

                #print(seelist)
                account=account-buylist['value'].sum()
                #上日控制flag用于给后一日提供买卖信息，默认为0
                buylist['last_action_flag']=0

                savebuylist=buylist[['ts_code','open','buyuse','adj_factor','last_action_flag']]
                savebuylist.columns = ['ts_code','lastprice','buy_amount','last_adj_factor','last_action_flag']

                codelist=codelist.append(savebuylist)
                #todo 这里因为下个循环drop会用到index如果不重新排序会造成问题，先这样改如果需要提升速度再进行修正
                codelist.reset_index(inplace=True,drop=True)
                cur_hold_num+=buynum
                sdfafa=1


            #print(codelist)
            #codelist_buffer=pd.merge(codelist,cur_merge_df, how='left', on=['ts_code'])
            bufferdf=codelist['buy_amount']*codelist['lastprice']
            #if(cur_date>20171018):
            #    print(codelist)
            #print(codelist)
            code_value_sum=bufferdf.sum()
            print(account+code_value_sum)
            print(cur_date)
            show3.append(account+code_value_sum)

            last_cur_merge_df=cur_merge_df[["ts_code","mix"]]
            last_cur_merge_df.columns =['ts_code','last_mix']
            #print(last_cur_merge_df)
            days+=1



        days=np.arange(1,datelist.shape[0]+1)

        eee=np.where(days%5==0)

        daysshow=days[eee]
        datashow=datelist[eee]
        #a = np.random.rand(days.shape[0], 1)

        if True :
            #000001.SH 上证 000016.SH 50 000688.SH 科创50 000905.SH 中证500 399006.SZ 创业板指
            #399300.SZ 300 000852.SH 1000 
            baselinecode='399300.SZ'
            baseline1=self.display_baseline(datelist,accountbase,baselinecode)

            plt.plot(days,baseline1,c='m',label=baselinecode)

            baselinecode='399006.SZ'
            baseline2=self.display_baseline(datelist,accountbase,baselinecode)

            plt.plot(days,baseline2,c='c',label=baselinecode)

            baselinecode='000852.SH'
            baseline3=self.display_baseline(datelist,accountbase,baselinecode)

            plt.plot(days,baseline3,c='y',label=baselinecode)

            baselinecode='000905.SH'
            baseline4=self.display_baseline(datelist,accountbase,baselinecode)

            plt.plot(days,baseline4,c='k',label=baselinecode)

        print(show3)
        plt.plot(days,show3,c='green',label="TOPK_open_head30")

        plt.xticks(daysshow, datashow,color='blue',rotation=60)


        plt.legend()

        plt.show()

        input()
        asdffd=1

    def display_baseline(self,datelist,accountbase,basecode='399300.SZ'):

        #if(True):
        #    CSZLData.CSZLDataWithoutDate.get_baseline(basecode)

        index_name=basecode
        index_path='./Database/indexdata/'+index_name+'.csv'
        index_baseline=pd.read_csv(index_path,index_col=0,header=0)
        index_use=index_baseline[['trade_date','close']]
        index_use.sort_values(by=['trade_date'],ascending=True, inplace=True)


        index_use=index_use[index_use['trade_date'].isin(datelist)]

        basepoint=index_use['close'].values[0]
        index_use['close']=index_use['close']*accountbase/basepoint

        return index_use['close']