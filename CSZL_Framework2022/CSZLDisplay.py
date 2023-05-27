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

        #修改显示行列数
        pd.set_option('display.width', 5000)
        pd.set_option('display.max_rows', 500)
        pd.set_option('display.max_columns', 500)


        #df_all = pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)
        #df_adj_all=pd.read_csv('./Database/Daily_adj_factor.csv',index_col=0,header=0)
        #df_limit_all=pd.read_csv('./Database/Daily_stk_limit.csv',index_col=0,header=0)
        
        df_all = pd.read_pickle('./Database/Dailydata.pkl')
        df_adj_all = pd.read_pickle('./Database/Daily_adj_factor.pkl')
        df_limit_all = pd.read_pickle('./Database/Daily_stk_limit.pkl')
        df_all_indexw = pd.read_csv('current_indexweight_merge.csv')

        df_all=pd.merge(df_all, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='left', on=['ts_code','trade_date'])

        df_all=pd.merge(df_all, df_all_indexw, how='left', on=['ts_code'])
        df_all['index_code']=df_all['index_code'].fillna(value='2000')
        print(df_all)

        score_df = pd.read_csv(resultpath,index_col=0,header=0)
        #score_df=score_df[['ts_code','trade_date','mix']]
        
        score_df=score_df[['ts_code','trade_date','mix_rank','Shift_1total_mv_rank','close_show','0']]

        #score_df = pd.read_csv('zzzzfackdatapred_fullold.csv',index_col=0,header=0)
        
        #print(df_all)
        print(score_df)

        #hold_all=5
        #change_num=1
        hold_all=30
        change_num=4
        account=100000000
        accountbase=account
        buy_pct=0.9
        Trans_cost=0.997        #千三
        # balance random none
        choicepolicy="random"

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
        #datelist=datelist[datelist>20220101]
    
        curMax=0
        curMaxDropDown=0

        days=0
        show3=[]

        last_cur_merge_df=[]

        holdlists=pd.DataFrame(columns=('trade_date','ts_code','lastprice','buy_amount','last_adj_factor','last_action_flag'))
        allvalue=0

        for cur_date in datelist:

            #这里注意停牌的不包含在这个list中
            cur_df_all=df_all[df_all['trade_date'].isin([cur_date])]

            cur_score_df=score_df[score_df['trade_date'].isin([cur_date])]
            cur_merge_df=pd.merge(cur_df_all,cur_score_df, how='left', on=['trade_date','ts_code'])

            #print(cur_merge_df)
            cur_merge_df['mix_rank'].fillna(-99.99, inplace=True)
            if len(last_cur_merge_df):
                cur_merge_df=pd.merge(cur_merge_df,last_cur_merge_df, how='left', on=['ts_code'])
                cur_merge_df['last_mix_rank'].fillna(-99.99, inplace=True)
            
            #if cur_date>20180102 :
            #    cur_merge_df=cur_merge_df.to_csv("dsdf.csv")

            code_value_sum=0
            if codelist.shape[0]>0 :

                codelist_buffer=pd.merge(codelist,cur_merge_df, how='left', on=['ts_code'])

                addholdlistb=codelist_buffer[['ts_code','lastprice','buy_amount','last_adj_factor','last_action_flag','pct_chg']]
                addholdlist=addholdlistb.copy(deep=True)
                addholdlist.reset_index(inplace=True,drop=True)

                addholdlist.loc[:,'trade_date']=cur_date
                addholdlist.loc[:,'allvalue']=allvalue
                holdlists=holdlists.append(addholdlist)

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
                codelist_buffer=codelist_buffer.sort_values(by=['last_mix_rank'])
                
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

                buylist=cur_merge_df
                buylist=buylist.sort_values(by=['last_mix_rank'])

                #cur_merge_df.reset_index(inplace=True,drop=True)
                buylist.reset_index(inplace=True,drop=True)
                buylist=buylist[buylist['index_code']!='2000']
                #buylist.reset_index(inplace=True,drop=True)

                #single code no repeat
                buylist=buylist[~buylist['ts_code'].isin(codelist['ts_code'])]

                #todo can't buy highstop
                #buylist=buylist[buylist['pct_chg']<4]
                buylist=buylist[buylist['open']!=buylist['up_limit']]
                #buylist=buylist[buylist['pct_chg']>-9]


                if choicepolicy=="random":                    
                    buylist = shuffle(buylist,random_state=20)
                elif choicepolicy=="balance":
                    headnum=buynum/20+1
                    
                    test=buylist.groupby('Shift_1total_mv_rank').tail(headnum)
                    #print(buylist)
                    buylist=test.sort_values(by=['last_mix_rank'])

                #错误示范，预知未来
                buylist=buylist[buylist['last_amount']>15000]
                #avg0=(buylist['last_0'].mean())
                #buylist=buylist[buylist['last_0']>avg0]
                #buylist=buylist[buylist['pre_close']>10]
                buylist=buylist[buylist['ts_code'].str.startswith('688')==False]
                #buylist.to_csv("comp.csv")
                #print(buylist)
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
            if(cur_date>20220601):
                print(codelist)
            #print(codelist)


            code_value_sum=bufferdf.sum()
            allvalue=account+code_value_sum

            #计算max drop down
            if(curMax<allvalue):
                curMax=allvalue

            curDropDown=(curMax-allvalue)/curMax
            
            if(curMaxDropDown<curDropDown):
                curMaxDropDown=curDropDown

            print(curMaxDropDown)
            print(allvalue)
            print(cur_date)
            
            show3.append(allvalue)

            last_cur_merge_df=cur_merge_df[["ts_code","mix_rank","amount","0"]]
            last_cur_merge_df.columns =['ts_code','last_mix_rank','last_amount','last_0']
            #print(last_cur_merge_df)
            days+=1

        holdlists.to_csv("seebug.csv")

        days=np.arange(1,datelist.shape[0]+1)

        eee=np.where(days%5==0)

        daysshow=days[eee]
        datashow=datelist[eee]
        #a = np.random.rand(days.shape[0], 1)


        #a=np.load('a.npy')
        #a=a.tolist()

        #print(a)

        #plt.plot(days,a,c='red',label='CB')

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
        plt.plot(days,show3,c='green',label="TOPK _open_head30")

        plt.xticks(daysshow, datashow,color='blue',rotation=60)


        plt.legend()

        plt.show()

        input()
        asdffd=1

    def Topk_nextopen_addopt(self,resultpath):

        #修改显示行列数
        pd.set_option('display.width', 5000)
        pd.set_option('display.max_rows', 500)
        pd.set_option('display.max_columns', 500)


        #df_all = pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)
        #df_adj_all=pd.read_csv('./Database/Daily_adj_factor.csv',index_col=0,header=0)
        #df_limit_all=pd.read_csv('./Database/Daily_stk_limit.csv',index_col=0,header=0)
        
        df_all = pd.read_pickle('./Database/Dailydata.pkl')
        df_adj_all = pd.read_pickle('./Database/Daily_adj_factor.pkl')
        df_limit_all = pd.read_pickle('./Database/Daily_stk_limit.pkl')
        df_all_indexw = pd.read_csv('current_indexweight_merge.csv')

        df_all=pd.merge(df_all, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='left', on=['ts_code','trade_date'])

        df_all=pd.merge(df_all, df_all_indexw, how='left', on=['ts_code'])
        df_all['index_code']=df_all['index_code'].fillna(value='2000')
        print(df_all)

        score_df = pd.read_csv(resultpath,index_col=0,header=0)
        #score_df=score_df[['ts_code','trade_date','mix']]
        
        score_df=score_df[['ts_code','trade_date','mix_rank','Shift_1total_mv_rank','close_show','0']]

        #score_df = pd.read_csv('zzzzfackdatapred_fullold.csv',index_col=0,header=0)
        
        #print(df_all)
        print(score_df)

        #hold_all=5
        #change_num=1
        hold_all=30
        change_num=5
        account=10000000
        accountbase=account
        buy_pct=0.9

        lastallvalue=account

        Trans_cost=0.997        #千三
        # balance random none
        choicepolicy="random"

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
        #datelist=datelist[datelist>20200101]
    
        curMax=0
        curMaxDropDown=0

        days=0
        show3=[]

        last_cur_merge_df=[]

        holdlists=pd.DataFrame(columns=('trade_date','ts_code','lastprice','buy_amount','last_adj_factor','last_action_flag'))
        allvalue=0

        #399300.SZ
        baselinecode='399300.SZ'
        baseline4=self.get_baseline_opt(datelist,accountbase,baselinecode)

        baseline5=pd.read_csv("saveoptresult.csv",index_col=0,header=0)
        
        baseline5=baseline5[baseline5['trade_date'].isin(datelist)]

        #print(baseline5)
        print(baseline4)

        counter22=0
        for cur_date in datelist:

            counter22+=1
            print(counter22)
            #这里注意停牌的不包含在这个list中
            cur_df_all=df_all[df_all['trade_date'].isin([cur_date])]

            cur_score_df=score_df[score_df['trade_date'].isin([cur_date])]
            cur_merge_df=pd.merge(cur_df_all,cur_score_df, how='left', on=['trade_date','ts_code'])

            #print(cur_merge_df)
            cur_merge_df['mix_rank'].fillna(-99.99, inplace=True)
            if len(last_cur_merge_df):
                cur_merge_df=pd.merge(cur_merge_df,last_cur_merge_df, how='left', on=['ts_code'])
                cur_merge_df['last_mix_rank'].fillna(-99.99, inplace=True)
            
            #if cur_date>20180102 :
            #    cur_merge_df=cur_merge_df.to_csv("dsdf.csv")

            code_value_sum=0
            if codelist.shape[0]>0 :

                codelist_buffer=pd.merge(codelist,cur_merge_df, how='left', on=['ts_code'])

                addholdlistb=codelist_buffer[['ts_code','lastprice','buy_amount','last_adj_factor','last_action_flag','pct_chg']]
                addholdlist=addholdlistb.copy(deep=True)
                addholdlist.reset_index(inplace=True,drop=True)

                addholdlist.loc[:,'trade_date']=cur_date
                addholdlist.loc[:,'allvalue']=allvalue
                holdlists=holdlists.append(addholdlist)

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
                codelist_buffer=codelist_buffer.sort_values(by=['last_mix_rank'])
                
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

            while(buynum>0 and len(last_cur_merge_df)):

                buy_all_value=0
                if(codelist.shape[0]>0):
                    hold_code_sum=codelist_buffer['value'].sum()
                    buy_all_value=(account+hold_code_sum)*buy_pct-hold_code_sum

                else:
                    buy_all_value=account*buy_pct

                #when account too low then don't do anything
                if(buy_all_value<10000):
                    break

                code_amount_buy=buy_all_value/buynum

                buylist=cur_merge_df
                buylist=buylist.sort_values(by=['last_mix_rank'])

                #cur_merge_df.reset_index(inplace=True,drop=True)
                buylist.reset_index(inplace=True,drop=True)
                #buylist=buylist[buylist['index_code']!='2000']
                #buylist.reset_index(inplace=True,drop=True)

                #single code no repeat
                buylist=buylist[~buylist['ts_code'].isin(codelist['ts_code'])]

                #todo can't buy highstop
                #buylist=buylist[buylist['pct_chg']<4]
                buylist=buylist[buylist['open']!=buylist['up_limit']]
                #buylist=buylist[buylist['pct_chg']>-9]


                if choicepolicy=="random":                    
                    buylist = shuffle(buylist,random_state=50)
                elif choicepolicy=="balance":
                    headnum=buynum/20+1
                    
                    test=buylist.groupby('Shift_1total_mv_rank').tail(headnum)
                    #print(buylist)
                    buylist=test.sort_values(by=['last_mix_rank'])

                #错误示范，预知未来
                #buylist=buylist[buylist['last_amount']>15000]
                #buylist=buylist[buylist['Shift_1total_mv_rank']>4]
                buylist=buylist[buylist['Shift_1total_mv_rank']>16]
                #avg0=(buylist['last_0'].mean())
                #buylist=buylist[buylist['last_0']<avg0*0.75]
                buylist=buylist[buylist['pre_close']>3]
                buylist=buylist[buylist['ts_code'].str.startswith('688')==False]
                buylist=buylist[buylist['ts_code'].str.startswith('8')==False]
                #buylist.to_csv("comp.csv")
                #print(buylist)
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

                break


            #print(codelist)
            #codelist_buffer=pd.merge(codelist,cur_merge_df, how='left', on=['ts_code'])
            bufferdf=codelist['buy_amount']*codelist['lastprice']
            if(cur_date>20220101):
                print(codelist)
            #print(codelist)

            #if(days>0 and days+1<1024):
            #    cur_changepct=baseline5["pct_chg"].values[days]
            #    #cur_changeval=abs(lastallvalue*cur_changepct/100)-lastallvalue/100

            #    ##cur_changeval=lastallvalue*cur_changepct/100
            #    account=account+lastallvalue*cur_changepct*1.2
            #    ##account=account+cur_changepct*10

            code_value_sum=bufferdf.sum()
            allvalue=account+code_value_sum

            #计算max drop down
            if(curMax<allvalue):
                curMax=allvalue

            curDropDown=(curMax-allvalue)/curMax
            
            if(curMaxDropDown<curDropDown):
                curMaxDropDown=curDropDown

            print(curMaxDropDown)
            print(allvalue)
            print(cur_date)
            
            show3.append(allvalue)

            lastallvalue=allvalue

            last_cur_merge_df=cur_merge_df[["ts_code","mix_rank","amount","0"]]
            last_cur_merge_df.columns =['ts_code','last_mix_rank','last_amount','last_0']
            print(days)
            print(len(show3))
            days+=1

        #holdlists.to_csv("seebug.csv")

        days=np.arange(1,datelist.shape[0]+1)

        eee=np.where(days%5==0)

        daysshow=days[eee]
        datashow=datelist[eee]
        #a = np.random.rand(days.shape[0], 1)


        #a=np.load('a.npy')
        #a=a.tolist()

        #print(a)

        #plt.plot(days,a,c='red',label='CB')

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
        plt.plot(days,show3,c='green',label="TOPK _open_head30")

        plt.xticks(daysshow, datashow,color='blue',rotation=60)


        plt.legend()

        plt.show()

        input()
        asdffd=1

    def Topk_nextopen_addopt_comp(self,resultpath):

        #修改显示行列数
        pd.set_option('display.width', 5000)
        pd.set_option('display.max_rows', 500)
        pd.set_option('display.max_columns', 500)


        #df_all = pd.read_csv('./Database/Dailydata.csv',index_col=0,header=0)
        #df_adj_all=pd.read_csv('./Database/Daily_adj_factor.csv',index_col=0,header=0)
        #df_limit_all=pd.read_csv('./Database/Daily_stk_limit.csv',index_col=0,header=0)
        
        df_all = pd.read_pickle('./Database/Dailydata.pkl')
        df_adj_all = pd.read_pickle('./Database/Daily_adj_factor.pkl')
        df_limit_all = pd.read_pickle('./Database/Daily_stk_limit.pkl')
        df_all_indexw = pd.read_csv('current_indexweight_merge.csv')

        df_all=pd.merge(df_all, df_adj_all, how='left', on=['ts_code','trade_date'])
        df_all=pd.merge(df_all, df_limit_all, how='left', on=['ts_code','trade_date'])

        df_all=pd.merge(df_all, df_all_indexw, how='left', on=['ts_code'])
        df_all['index_code']=df_all['index_code'].fillna(value='2000')
        print(df_all)

        score_df = pd.read_csv(resultpath,index_col=0,header=0)
        #score_df=score_df[['ts_code','trade_date','mix']]
        
        score_df=score_df[['ts_code','trade_date','mix_rank','Shift_1total_mv_rank','close_show','0']]

        score_df_other=pd.read_csv("oot_score.csv",header=0)

        print(df_all_indexw)
        print(score_df)
        print(score_df_other)

        score_df =pd.merge(score_df, score_df_other, how='left', on=['ts_code','trade_date'])
        
        score_df.rename(columns={'mix_rank':'mix_rankold'},inplace=True)
        score_df.rename(columns={'3-0-diff':'mix_rank'},inplace=True)

        score_df['mix_rank']=score_df['mix_rank']+2

        print(score_df)

        #print(df_all)

        #hold_all=5
        #change_num=1
        hold_all=100
        change_num=20
        account=100000000
        accountbase=account
        buy_pct=0.9

        lastallvalue=account

        Trans_cost=0.997        #千三
        # balance random none
        choicepolicy="none"

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
        datelist=datelist[datelist>20200101]
    
        curMax=0
        curMaxDropDown=0

        days=0
        show3=[]

        last_cur_merge_df=[]

        holdlists=pd.DataFrame(columns=('trade_date','ts_code','lastprice','buy_amount','last_adj_factor','last_action_flag'))
        allvalue=0

        #399300.SZ
        baselinecode='399300.SZ'
        baseline4=self.get_baseline_opt(datelist,accountbase,baselinecode)

        baseline5=pd.read_csv("saveoptresult.csv",index_col=0,header=0)
        
        baseline5=baseline5[baseline5['trade_date'].isin(datelist)]

        #print(baseline5)
        print(baseline4)

        counter22=0
        for cur_date in datelist:

            counter22+=1
            print(counter22)
            #这里注意停牌的不包含在这个list中
            cur_df_all=df_all[df_all['trade_date'].isin([cur_date])]

            cur_score_df=score_df[score_df['trade_date'].isin([cur_date])]
            cur_merge_df=pd.merge(cur_df_all,cur_score_df, how='left', on=['trade_date','ts_code'])

            #print(cur_merge_df)
            cur_merge_df['mix_rank'].fillna(-99.99, inplace=True)
            if len(last_cur_merge_df):
                cur_merge_df=pd.merge(cur_merge_df,last_cur_merge_df, how='left', on=['ts_code'])
                cur_merge_df['last_mix_rank'].fillna(-99.99, inplace=True)
            
            #if cur_date>20180102 :
            #    cur_merge_df=cur_merge_df.to_csv("dsdf.csv")

            code_value_sum=0
            if codelist.shape[0]>0 :

                codelist_buffer=pd.merge(codelist,cur_merge_df, how='left', on=['ts_code'])

                addholdlistb=codelist_buffer[['ts_code','lastprice','buy_amount','last_adj_factor','last_action_flag','pct_chg']]
                addholdlist=addholdlistb.copy(deep=True)
                addholdlist.reset_index(inplace=True,drop=True)

                addholdlist.loc[:,'trade_date']=cur_date
                addholdlist.loc[:,'allvalue']=allvalue
                holdlists=holdlists.append(addholdlist)

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
                codelist_buffer=codelist_buffer.sort_values(by=['last_mix_rank'])
                
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

            while(buynum>0 and len(last_cur_merge_df)):

                buy_all_value=0
                if(codelist.shape[0]>0):
                    hold_code_sum=codelist_buffer['value'].sum()
                    buy_all_value=(account+hold_code_sum)*buy_pct-hold_code_sum

                else:
                    buy_all_value=account*buy_pct

                #when account too low then don't do anything
                if(buy_all_value<10000):
                    break

                code_amount_buy=buy_all_value/buynum

                buylist=cur_merge_df
                buylist=buylist.sort_values(by=['last_mix_rank'])

                #cur_merge_df.reset_index(inplace=True,drop=True)
                buylist.reset_index(inplace=True,drop=True)
                #buylist=buylist[buylist['index_code']!='2000']
                #buylist.reset_index(inplace=True,drop=True)

                #single code no repeat
                buylist=buylist[~buylist['ts_code'].isin(codelist['ts_code'])]

                #todo can't buy highstop
                #buylist=buylist[buylist['pct_chg']<4]
                buylist=buylist[buylist['open']!=buylist['up_limit']]
                #buylist=buylist[buylist['pct_chg']>-9]


                if choicepolicy=="random":                    
                    buylist = shuffle(buylist,random_state=15)
                elif choicepolicy=="balance":
                    headnum=buynum/20+1
                    
                    test=buylist.groupby('Shift_1total_mv_rank').tail(headnum)
                    #print(buylist)
                    buylist=test.sort_values(by=['last_mix_rank'])

                #错误示范，预知未来
                buylist=buylist[buylist['last_amount']>15000]
                #avg0=(buylist['last_0'].mean())
                #buylist=buylist[buylist['last_0']>avg0]
                #buylist=buylist[buylist['pre_close']>10]
                buylist=buylist[buylist['ts_code'].str.startswith('688')==False]
                #buylist.to_csv("comp.csv")
                #print(buylist)
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

                break


            #print(codelist)
            #codelist_buffer=pd.merge(codelist,cur_merge_df, how='left', on=['ts_code'])
            bufferdf=codelist['buy_amount']*codelist['lastprice']
            if(cur_date>20220601):
                print(codelist)
            #print(codelist)

            #if(days>0 and days+1<1024):
            #    cur_changepct=baseline5["pct_chg"].values[days]
            #    #cur_changeval=abs(lastallvalue*cur_changepct/100)-lastallvalue/100

            #    ##cur_changeval=lastallvalue*cur_changepct/100
            #    account=account+lastallvalue*cur_changepct*1.2
            #    ##account=account+cur_changepct*10

            code_value_sum=bufferdf.sum()
            allvalue=account+code_value_sum

            #计算max drop down
            if(curMax<allvalue):
                curMax=allvalue

            curDropDown=(curMax-allvalue)/curMax
            
            if(curMaxDropDown<curDropDown):
                curMaxDropDown=curDropDown

            print(curMaxDropDown)
            print(allvalue)
            print(cur_date)
            
            show3.append(allvalue)

            lastallvalue=allvalue

            last_cur_merge_df=cur_merge_df[["ts_code","mix_rank","amount","0"]]
            last_cur_merge_df.columns =['ts_code','last_mix_rank','last_amount','last_0']
            print(days)
            print(len(show3))
            days+=1

        #holdlists.to_csv("seebug.csv")

        days=np.arange(1,datelist.shape[0]+1)

        eee=np.where(days%5==0)

        daysshow=days[eee]
        datashow=datelist[eee]
        #a = np.random.rand(days.shape[0], 1)


        #a=np.load('a.npy')
        #a=a.tolist()

        #print(a)

        #plt.plot(days,a,c='red',label='CB')

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
        plt.plot(days,show3,c='green',label="TOPK _open_head30")

        plt.xticks(daysshow, datashow,color='blue',rotation=60)


        plt.legend()

        plt.show()

        input()
        asdffd=1


    def Topk_nextopen_trend(self,resultpath):

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
        
        score_df=score_df[['ts_code','trade_date','mix_rank','Shift_1total_mv_rank','close_show','0']]

        #score_df = pd.read_csv('zzzzfackdatapred_fullold.csv',index_col=0,header=0)
        
        #print(df_all)
        print(score_df)

        #hold_all=5
        #change_num=1
        hold_all=30
        change_num=4
        account=100000000
        accountbase=account
        buy_pct=0.9
        Trans_cost=0.997        #千三
        # balance random none
        choicepolicy="none"

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
        #datelist=datelist[datelist>20220101]
        buyto=0

        curMax=0
        curMaxDropDown=0

        days=0
        show3=[]

        last_cur_merge_df=[]

        holdlists=pd.DataFrame(columns=('trade_date','ts_code','lastprice','buy_amount','last_adj_factor','last_action_flag'))
        allvalue=account

        ##trand comp
        trandbaselinecode='399300.SZ'
        trandbaseline1=self.display_baseline(datelist,accountbase,trandbaselinecode)
        print(trandbaseline1)

        curtrandbaseline=trandbaseline1.values[0]
        lasttrandbaseline=1
        todaybaselinechange=0

        lastmodelchange1=0
        lastmodelchange2=0
        lastmodelchange3=0
        lastmodelchange4=0
        todaymodelchange=0

        trendflag=True

        for cur_date in datelist:

            #这里注意停牌的不包含在这个list中
            cur_df_all=df_all[df_all['trade_date'].isin([cur_date])]

            cur_score_df=score_df[score_df['trade_date'].isin([cur_date])]
            cur_merge_df=pd.merge(cur_df_all,cur_score_df, how='left', on=['trade_date','ts_code'])

            cur_merge_df['mix_rank'].fillna(-99.99, inplace=True)
            if len(last_cur_merge_df):
                cur_merge_df=pd.merge(cur_merge_df,last_cur_merge_df, how='left', on=['ts_code'])
                cur_merge_df['last_mix_rank'].fillna(-99.99, inplace=True)
            
            #if cur_date>20180102 :
            #    cur_merge_df=cur_merge_df.to_csv("dsdf.csv")

            code_value_sum=0
            if codelist.shape[0]>0 :

                codelist_buffer=pd.merge(codelist,cur_merge_df, how='left', on=['ts_code'])

                addholdlistb=codelist_buffer[['ts_code','lastprice','buy_amount','last_adj_factor','last_action_flag','pct_chg']]
                addholdlist=addholdlistb.copy(deep=True)
                addholdlist.reset_index(inplace=True,drop=True)

                addholdlist.loc[:,'trade_date']=cur_date
                addholdlist.loc[:,'allvalue']=allvalue
                holdlists=holdlists.append(addholdlist)

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
            
            #if trendflag:
            #    sellto=buyto-change_num
            #else:
            #    if sellto-change_num<0:
            #        sellto=0 
            #    else:
            #        sellto-=change_num
            if buyto-change_num<0:
                sellto=0
            else:
                sellto=buyto-change_num

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
                codelist_buffer=codelist_buffer.sort_values(by=['last_mix_rank'])
                
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
                codelist.reset_index(inplace=True,drop=True)
                sdfafa=1

            #buy==========================
            #trend
            if trendflag:
                if buyto+change_num>hold_all:
                    buyto=hold_all
                else:
                    buyto+=change_num
            else:
                if buyto-change_num<0:
                    buyto=0 
                else:
                    buyto-=change_num

            
            buynum=buyto-cur_hold_num
            buynumold=hold_all-cur_hold_num

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

                code_amount_buy=buy_all_value/buynumold

                cur_merge_df=cur_merge_df.sort_values(by=['last_mix_rank'])

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
                    #print(buylist)
                    buylist=test.sort_values(by=['last_mix_rank'])

                #错误示范，预知未来
                #buylist=buylist[buylist['last_amount']>15000]
                #avg0=(buylist['last_0'].mean())
                #buylist=buylist[buylist['last_0']>avg0]
                #buylist=buylist[buylist['pre_close']>10]
                buylist=buylist[buylist['ts_code'].str.startswith('688')==False]
                #buylist.to_csv("comp.csv")
                #print(buylist)
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
            #print(codelist)

            bufferdf=codelist['buy_amount']*codelist['lastprice']
            if(cur_date>20220101):
                print(codelist)
            #print(codelist)


            code_value_sum=bufferdf.sum()
            lastallvalue=allvalue
            allvalue=account+code_value_sum
            lastmodelchange4=lastmodelchange3
            lastmodelchange3=lastmodelchange2
            lastmodelchange2=lastmodelchange1
            lastmodelchange1=todaymodelchange

            todaymodelchange=(allvalue-lastallvalue)/lastallvalue
            #print(todaymodelchange)

            lastbaselinechange=todaybaselinechange
            lasttrandbaseline=curtrandbaseline
            curtrandbaseline=trandbaseline1.values[days]
            todaybaselinechange=(curtrandbaseline-lasttrandbaseline)/lasttrandbaseline
            #print(lastbaselinechange)

            #if todaymodelchange-lastbaselinechange>-0.005:

            sumchange3=lastmodelchange2+lastmodelchange1+todaymodelchange+lastmodelchange3+lastmodelchange4
            if sumchange3>-0.001:
                trendflag=True
            else:
                trendflag=False

            #计算max drop down
            if(curMax<allvalue):
                curMax=allvalue

            curDropDown=(curMax-allvalue)/curMax
            
            if(curMaxDropDown<curDropDown):
                curMaxDropDown=curDropDown

            print(curMaxDropDown)
            print(allvalue)
            print(cur_date)
            
            show3.append(allvalue)

            last_cur_merge_df=cur_merge_df[["ts_code","mix_rank","amount","0"]]
            last_cur_merge_df.columns =['ts_code','last_mix_rank','last_amount','last_0']
            #print(last_cur_merge_df)
            days+=1

        holdlists.to_csv("seebug.csv")

        days=np.arange(1,datelist.shape[0]+1)

        eee=np.where(days%5==0)

        daysshow=days[eee]
        datashow=datelist[eee]
        #a = np.random.rand(days.shape[0], 1)


        #a=np.load('a.npy')
        #a=a.tolist()

        #print(a)

        #plt.plot(days,a,c='red',label='CB')

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
        plt.plot(days,show3,c='green',label="TOPK _open_head30")

        plt.xticks(daysshow, datashow,color='blue',rotation=60)


        plt.legend()

        plt.show()

        input()
        asdffd=1

    def Topk_nextopen_mix(self,resultpath):

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
        
        score_df=score_df[['ts_code','trade_date','mix','Shift_1total_mv_rank','close_show']]

        #score_df = pd.read_csv('zzzzfackdatapred_fullold.csv',index_col=0,header=0)
        
        #print(df_all)
        print(score_df)

        #hold_all=5
        #change_num=1
        hold_all=30
        change_num=6
        account=100000000
        accountbase=account
        buy_pct=0.9
        Trans_cost=0.997        #千三
        # balance random none
        choicepolicy="random"

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

    def Topk_nextopen_CB(self,resultpath):

        #修改显示行列数
        pd.set_option('display.width', 5000)
        pd.set_option('display.max_rows', 500)
        pd.set_option('display.max_columns', 500)
        
        df_all = pd.read_pickle('./Database/CBDaily.pkl')
        df_all=df_all[df_all["open"]!=0]     
        df_all.reset_index(inplace=True,drop=True)

        score_df = pd.read_csv(resultpath,index_col=0,header=0)

        score_df=score_df[['ts_code','trade_date','mix_rank','close_show']]

        print(score_df)

        #hold_all=5
        #change_num=1
        hold_all=10
        change_num=2
        account=100000000
        accountbase=account
        buy_pct=0.9
        Trans_cost=0.998        #千二
        # balance random none small
        choicepolicy="small"

        ###添加停牌计算和涨跌停简单策略

        #stop_state             当日不停牌为0，当日停牌为1 (TODO:前日停牌本日不停牌为2,不每日刷新)，每日刷新
        #control_state_open     当日不停牌且开盘未触及涨跌停为0，当日开盘触及跌停为1，当日开盘触及涨停为2，每日刷新
        #control_state_close    当日不停牌且收盘没有触及涨跌停为0，当日收盘触及跌停1，当日收盘触及涨停为2,每日刷新
        #last_action_flag       前日不需要买入卖出为0，前日需要卖出为1，前日需要买入为2

        codelist=pd.DataFrame(columns=('ts_code','lastprice','buy_amount','last_action_flag'))
        codelist_buffer=pd.DataFrame(columns=('ts_code','lastprice','buy_amount','last_action_flag'))
        #codelist=codelist.append([{'ts_code':1,'lastprice':1,'amount':1,'adjflag':1}])
        #print(codelist)

        score_df=score_df.sort_values(by=['trade_date'])
        datelist=score_df['trade_date'].unique()
        cur_hold_num=0
        print(datelist)
    

        curMax=0
        curMaxDropDown=0

        days=0
        show3=[]

        last_cur_merge_df=[]

        for cur_date in datelist:

            #这里注意停牌的不包含在这个list中
            cur_df_all=df_all[df_all['trade_date'].isin([cur_date])]

            cur_score_df=score_df[score_df['trade_date'].isin([cur_date])]
            cur_merge_df=pd.merge(cur_df_all,cur_score_df, how='left', on=['trade_date','ts_code'])

            cur_merge_df['mix_rank'].fillna(-99.99, inplace=True)
            if len(last_cur_merge_df):
                cur_merge_df=pd.merge(cur_merge_df,last_cur_merge_df, how='left', on=['ts_code'])
                cur_merge_df['last_mix_rank'].fillna(-99.99, inplace=True)
            
            #if cur_date>20180102 :
            #    cur_merge_df=cur_merge_df.to_csv("dsdf.csv")

            code_value_sum=0
            if codelist.shape[0]>0 :

                codelist_buffer=pd.merge(codelist,cur_merge_df, how='left', on=['ts_code'])
                #刷新停牌的close和adj价值

                codelist_buffer['close'].fillna(9999.99, inplace=True)
                codelist_buffer['open'].fillna(9999.99, inplace=True)
                codelist_buffer['control_state_open']=0
                codelist_buffer['control_state_close']=0

                codelist_buffer['stop_state']=0
                codelist_buffer.loc[codelist_buffer['open']==9999.99,'stop_state']=1
                #codelist_buffer.loc[codelist_buffer['open']==codelist_buffer['down_limit'],'control_state_open']=1
                #codelist_buffer.loc[codelist_buffer['open']==codelist_buffer['up_limit'],'control_state_open']=2
                #codelist_buffer.loc[codelist_buffer['close']==codelist_buffer['down_limit'],'control_state_close']=1
                #codelist_buffer.loc[codelist_buffer['close']==codelist_buffer['up_limit'],'control_state_close']=2

                #codelist_buffer.loc[codelist_buffer['adj_factor']==9999.99,'adj_factor']=codelist_buffer['last_adj_factor']
                codelist_buffer.loc[codelist_buffer['open']==9999.99,'open']=codelist_buffer['lastprice']
                
                ###更新除权
                ##print(codelist_buffer.head(10))
                #codelist_buffer.loc[:,'buy_amount']=codelist_buffer['buy_amount']

                #codelist_buffer.loc[:,'last_adj_factor']=codelist_buffer['adj_factor']
                #codelist_buffer.loc[:,'last_adj_factor']=codelist_buffer['adj_factor']

                #print(codelist_buffer.head(10))
                codelist.loc[:,'buy_amount']=codelist_buffer['buy_amount']
                #codelist.loc[:,'last_adj_factor']=codelist_buffer['adj_factor']
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
                codelist_buffer=codelist_buffer.sort_values(by=['last_mix_rank'])
                #print(codelist_buffer)

                #先将这些分数低的更新last_action_flag为1
                codelist_buffer.loc[codelist_buffer['ts_code'].isin(codelist_buffer['ts_code'].head(sellnum)),'last_action_flag']=1
                codelist.loc[codelist['ts_code'].isin(codelist_buffer['ts_code'].head(sellnum)),'last_action_flag']=1

                #更新当日control_state_close不跌停的的sell_value
                codelist_buffer.loc[(codelist_buffer['last_action_flag']==1)&(codelist_buffer['control_state_open']!=1),'sell_value']=codelist_buffer['value']
                codelist_buffer.loc[(codelist_buffer['stop_state']==1),'sell_value']=codelist_buffer['value']


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

                #cur_merge_df=cur_merge_df.sort_values(by=['last_mix_rank'])

                buylist=cur_merge_df
                #single code no repeat
                buylist=buylist[~buylist['ts_code'].isin(codelist['ts_code'])]


                #buylist=buylist[buylist['pct_chg']<4]
                #buylist=buylist[buylist['open']!=buylist['up_limit']]
                #buylist=buylist[buylist['pct_chg']>-9]


                if choicepolicy=="random":
                    buylist=buylist[buylist['last_amount']>1000]
                    buylist=buylist[buylist['last_close']<120]
                    #buylist = buylist[buylist['amount']>2000]
                    buylist = shuffle(buylist,random_state=12)

                if choicepolicy=="small":

                    pass
                    #print(buylist)
                    buylist=buylist[buylist['last_amount']>500]
                    buylist=buylist[buylist['last_close']<120]
                    #buylist = buylist[buylist['open']<120]
                    #buylist['amount_rank']=buylist['amount'].rank(pct=True)
                    #buylist = buylist[buylist['amount_rank']>0.9]
                    #buylist = buylist[buylist['amount']>5000]
                    buylist=buylist.sort_values(by=['last_mix_rank'])
                    #print(buylist)


                #print(buylist)
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

                savebuylist=buylist[['ts_code','open','buyuse','last_action_flag']]
                savebuylist.columns = ['ts_code','lastprice','buy_amount','last_action_flag']

                codelist=codelist.append(savebuylist)
                #todo 这里因为下个循环drop会用到index如果不重新排序会造成问题，先这样改如果需要提升速度再进行修正
                codelist.reset_index(inplace=True,drop=True)
                cur_hold_num+=buynum
                sdfafa=1


            #print(codelist)
            #codelist_buffer=pd.merge(codelist,cur_merge_df, how='left', on=['ts_code'])
            bufferdf=codelist['buy_amount']*codelist['lastprice']
            if(cur_date>20220105):
                pass
                print(codelist)
            #print(codelist)
            code_value_sum=bufferdf.sum()
            allvalue=account+code_value_sum


            #计算max drop down
            if(curMax<allvalue):
                curMax=allvalue

            curDropDown=(curMax-allvalue)/curMax
            
            if(curMaxDropDown<curDropDown):
                curMaxDropDown=curDropDown

            print(curMaxDropDown)
            print(allvalue)
            print(cur_date)
            show3.append(account+code_value_sum)

            last_cur_merge_df=cur_merge_df[["ts_code","mix_rank","amount","close"]]
            last_cur_merge_df.columns =['ts_code','last_mix_rank','last_amount',"last_close"]
            #print(last_cur_merge_df)
            days+=1



        days=np.arange(1,datelist.shape[0]+1)

        eee=np.where(days%5==0)

        daysshow=days[eee]
        datashow=datelist[eee]
        #a = np.random.rand(days.shape[0], 1)


        #a=np.array(show3)
        #np.save('a.npy',a) # 保存为.npy格式

        #a=np.load('a.npy')
        #a=a.tolist()

        #print(a)

        #plt.plot(days,a,c='red',label='CB')

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
        plt.plot(days,show3,c='green',label="TOPK_open_CB")

        plt.xticks(daysshow, datashow,color='blue',rotation=60)


        plt.legend()

        plt.show()

        input()
        asdffd=1

    def display_baseline(self,datelist,accountbase,basecode='399300.SZ'):

        if(True):
            CSZLData.CSZLDataWithoutDate.get_baseline(basecode)

        index_name=basecode
        index_path='./Database/indexdata/'+index_name+'.csv'
        index_baseline=pd.read_csv(index_path,index_col=0,header=0)
        index_use=index_baseline[['trade_date','close']]
        index_use.sort_values(by=['trade_date'],ascending=True, inplace=True)


        index_use=index_use[index_use['trade_date'].isin(datelist)]

        basepoint=index_use['close'].values[0]
        index_use['close']=index_use['close']*accountbase/basepoint

        return index_use['close']

    def get_baseline_opt(self,datelist,accountbase,basecode='399300.SZ'):

        if(True):
            CSZLData.CSZLDataWithoutDate.get_baseline(basecode)

        index_name=basecode
        index_path='./Database/indexdata/'+index_name+'.csv'
        index_baseline=pd.read_csv(index_path,index_col=0,header=0)
        print(index_baseline)
        index_use=index_baseline[['trade_date','pct_chg']]

        index_use.sort_values(by=['trade_date'],ascending=True, inplace=True)

        index_use=index_use[index_use['trade_date'].isin(datelist)]

        return index_use