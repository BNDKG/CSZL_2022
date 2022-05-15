#coding=utf-8
import CSZLUtils

import pandas as pd
import numpy as np
import lightgbm as lgb
import os
import sys

from sklearn.externals import joblib

class CSZLModel(object):
    """description of class"""

    def __init__(self):

        pass


    def LGBmodeltrain(self,featurepath):

        (filepath, tempfilename)=os.path.split(featurepath)
        (filename, extension) = os.path.splitext(tempfilename)

        modelfolder=filepath+'/'+filename
        CSZLUtils.CSZLUtils.mkdir(modelfolder)

        #基于使用的模块生成文件名
        pklname=modelfolder+'/'+sys._getframe().f_code.co_name
        lgb_model,pklname=self.LGBmodel_003(pklname)
        checkpath=pklname+'_0.pkl'

        featurepath=CSZLUtils.CSZLUtils.pathchange(featurepath)

        if(os.path.exists(checkpath)==True):
            return pklname

        x_train,y_train,_=self.LGBdatasetprepar(featurepath)

        self.split_dataset_and_train(x_train,y_train,pklname,lgb_model)


        return pklname

    def LGBmodeltrain_CB(self,featurepath):

        (filepath, tempfilename)=os.path.split(featurepath)
        (filename, extension) = os.path.splitext(tempfilename)

        modelfolder=filepath+'/'+filename
        CSZLUtils.CSZLUtils.mkdir(modelfolder)

        #基于使用的模块生成文件名
        pklname=modelfolder+'/'+sys._getframe().f_code.co_name
        lgb_model,pklname=self.LGBmodel_003(pklname)
        checkpath=pklname+'_0.pkl'

        featurepath=CSZLUtils.CSZLUtils.pathchange(featurepath)

        if(os.path.exists(checkpath)==True):
            return pklname

        x_train,y_train,_=self.LGBdatasetprepar_CB(featurepath)

        self.split_dataset_and_train(x_train,y_train,pklname,lgb_model)


        return pklname

    def LGBdatasetprepar(self,featurepath):

        #df_all=pd.read_csv(featurepath,index_col=0,header=0)
        #df_all=pd.read_pickle(featurepath)
        df_all=CSZLUtils.CSZLUtils.Loaddata(featurepath)

        df_all=df_all[df_all['st_or_otherwrong']==1]
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['close']>2]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['st_or_otherwrong','real_price'],axis=1,inplace=True)

        df_all=df_all.reset_index(drop=True)
        
        y_train = np.array(df_all['tomorrow_chg_rank'])
        train_label=df_all[['ts_code','trade_date']]
        x_train=df_all.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1)

        return x_train,y_train,train_label

    def LGBdatasetprepar_CB(self,featurepath):

        #df_all=pd.read_csv(featurepath,index_col=0,header=0)
        #df_all=pd.read_pickle(featurepath)
        df_all=CSZLUtils.CSZLUtils.Loaddata(featurepath)

        #df_all=df_all[df_all['high_stop']==0]
        #df_all=df_all[df_all['close']<115]
        #df_all=df_all[df_all['amount']>15000]

        df_all.drop(['real_price'],axis=1,inplace=True)

        df_all=df_all.reset_index(drop=True)
        
        y_train = np.array(df_all['tomorrow_chg_rank'])
        train_label=df_all[['ts_code','trade_date']]
        x_train=df_all.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1)

        return x_train,y_train,train_label

    def LGBdatasetrealpredictprepar(self,featurepath):

        df_all=pd.read_csv(featurepath,index_col=0,header=0)

        df_all=df_all[df_all['st_or_otherwrong']==1]
        df_all=df_all[df_all['high_stop']==0]
        df_all=df_all[df_all['close']>2]
        df_all=df_all[df_all['amount']>15000]

        df_all.drop(['st_or_otherwrong','real_price'],axis=1,inplace=True)

        df_all=df_all.reset_index(drop=True)
        
        train_label=df_all[['ts_code','trade_date']]
        x_train=df_all.drop(['ts_code','trade_date'],axis=1)

        return x_train,train_label

    def LGBdatasetrealpredictprepar_CB(self,featurepath):

        df_all=pd.read_csv(featurepath,index_col=0,header=0)

        df_all=df_all[df_all['close']>2]
        df_all=df_all[df_all['amount']>400]
        #df_all.dropna(axis=0,how='any',inplace=True)

        df_all.drop(['real_price'],axis=1,inplace=True)

        df_all=df_all.reset_index(drop=True)
        
        train_label=df_all[['ts_code','trade_date']]
        x_train=df_all.drop(['ts_code','trade_date'],axis=1)

        return x_train,train_label

    def LGBmodel_003(self,pklname):

        pklname=pklname+sys._getframe().f_code.co_name

        lgb_model = lgb.LGBMClassifier(max_depth=-1,
                                        n_estimators=250,
                                        learning_rate=0.05,
                                        num_leaves=2**8-1,
                                        colsample_bytree=0.6,
                                        objective='multiclass', 
                                        num_class=20,
                                        n_jobs=-1)



        return lgb_model,pklname

    def LGBmodel_sum20(self,pklname):

        pklname=pklname+sys._getframe().f_code.co_name

        #lightgbm训练的参数：注意，上面的（**params）中的**必须写
        params = {
        'boosting_type': 'gbdt',
        'objective':'regression',
        #'n_jobs':8,
        'subsample': 0.5,
        'subsample_freq': 1,
        'learning_rate': 0.05,
        'num_leaves': 2**8-1,
        'min_data_in_leaf': 2**9-1,
        'feature_fraction': 0.5,
        'max_bin': 100,
        'n_estimators': 250,
        'boost_from_average': False,
        "random_seed":1,
        }
        lgb_model = lgb.LGBMRegressor(**params)
        #model.fit(x_train, y_train, 
        #    eval_set=[(x_valid, y_valid)],  
        #    early_stopping_rounds=verbose, 
        #    verbose=verbose)

        return lgb_model,pklname

    def split_dataset_and_train(self,train,y_train,pklname,usemodel):


        train_ids = train.index.tolist()

        splitno=int(len(train_ids)*0.70)
        splitno10=int(len(train_ids)*0.15)
        splitno11=int(len(train_ids)*0.85)
        splitno20=int(len(train_ids)*0.35)
        splitno21=int(len(train_ids)*0.65)

        train_index_list=[]
        test_index_list=[]
        #1
        train_index_list.append(train_ids[:splitno])
        test_index_list.append(train_ids[(splitno+100):])
        #2
        train_index_list.append(train_ids[(len(train_ids)-splitno):])
        test_index_list.append(train_ids[:(len(train_ids)-splitno-100)])
        #3
        train_index_list.append(train_ids[splitno10:splitno11])
        buffer=train_ids[:(splitno10-100)]
        buffer.extend(train_ids[(splitno11+100):])
        test_index_list.append(buffer)
        #4
        buffer=train_ids[:(splitno20)]
        buffer.extend(train_ids[(splitno21):])
        train_index_list.append(buffer)
        test_index_list.append(train_ids[(splitno20+100):(splitno21-100)])

        train=train.values
        print(train)
        new_train_times=4

        for counter in range(new_train_times):
       
            X_fit, X_val = train[train_index_list[counter]],train[test_index_list[counter]]
            y_fit, y_val = y_train[train_index_list[counter]], y_train[test_index_list[counter]]    

            usemodel.fit(X_fit, y_fit, eval_metric='multi_error',
                            eval_set=[(X_val, y_val)], 
                            #sample_weight=sample_weights,
                            #eval_sample_weight=[sample_weight_vals],
                            #categorical_feature=[5,],
                            verbose=100, early_stopping_rounds=None)

            savepath_new=pklname+'_'+str(counter)+".pkl"
            joblib.dump(usemodel,savepath_new)     

            pass

        return


    def LGBmodelpredict(self,featurepath,modelpath):

        (filepath, tempfilename)=os.path.split(featurepath)
        (filename, extension) = os.path.splitext(tempfilename)

        modelname=modelpath+'_0.pkl'
        predictname=modelpath+'_'+filename+'_0.pkl'

        if os.path.exists(predictname)==True and featurepath!="Today_Joinfeature.csv":
            #result=pd.read_pickle(predictname)
            #result.to_csv("resultsee2.csv")

            print("预测结果已生成")
            return predictname

        if featurepath=="Today_Joinfeature.csv":
            x_train,train_label=self.LGBdatasetrealpredictprepar(featurepath)
        else:
            x_train,_,train_label=self.LGBdatasetprepar(featurepath)


        #x_train.to_csv("trainsee2.csv")
        #finaldf = pd.merge(train_label, x_train, how='left', left_index=True, right_index=True) 
        #print(finaldf)


        train2=train_label.copy(deep=True)

        for counter in range(4):
            modelpath_new=modelpath+'_'+str(counter)+".pkl"
            predictpath_new=modelpath+'_'+filename+'_'+str(counter)+".pkl"

            lgb_model = joblib.load(modelpath_new)

            dsadwd=lgb_model.feature_importances_
            print(dsadwd)

            pred_test = lgb_model.predict_proba(x_train)

            data1 = pd.DataFrame(pred_test)

            train3=train2.join(data1)
            print(train3)

            train3.to_pickle(predictpath_new)
            pass



        return predictname

    def LGBmodelpredict_CB(self,featurepath,modelpath):

        (filepath, tempfilename)=os.path.split(featurepath)
        (filename, extension) = os.path.splitext(tempfilename)

        modelname=modelpath+'_0.pkl'
        predictname=modelpath+'_'+filename+'_0.pkl'

        if os.path.exists(predictname)==True and featurepath!="Today_Joinfeature_CB.csv":
            #result=pd.read_pickle(predictname)
            #result.to_csv("resultsee2.csv")

            print("预测结果已生成")
            return predictname

        if featurepath=="Today_Joinfeature_CB.csv":
            x_train,train_label=self.LGBdatasetrealpredictprepar_CB(featurepath)
        else:
            x_train,_,train_label=self.LGBdatasetprepar_CB(featurepath)


        #x_train.to_csv("trainsee2.csv")
        #finaldf = pd.merge(train_label, x_train, how='left', left_index=True, right_index=True) 
        #print(finaldf)


        train2=train_label.copy(deep=True)

        for counter in range(4):
            modelpath_new=modelpath+'_'+str(counter)+".pkl"
            predictpath_new=modelpath+'_'+filename+'_'+str(counter)+".pkl"

            lgb_model = joblib.load(modelpath_new)

            dsadwd=lgb_model.feature_importances_
            print(dsadwd)

            pred_test = lgb_model.predict_proba(x_train)

            data1 = pd.DataFrame(pred_test)

            train3=train2.join(data1)
            print(train3)

            train3.to_pickle(predictpath_new)
            pass



        return predictname

    def MixOutputresult(self,featurepath,modelpath):

        (filepath, tempfilename)=os.path.split(featurepath)
        (filename, extension) = os.path.splitext(tempfilename)

        predictname=modelpath+'_'+filename+'_result.csv'

        if(os.path.exists(predictname)==True):
            print("合成预测结果已生成")
            return predictname


        resultdf=[]
        for counter in range(4):

            predictpath_new=modelpath+'_'+filename+'_'+str(counter)+".pkl"

            data1=pd.read_pickle(predictpath_new)

            #print(data1)

            data1['mix']=0

            multlist=[-9.34,-5.48,-4.2,-3.4,-2.7,-2.3,-1.86,-1.47,-1.09,-0.74,-0.38,0,0.398,0.838,1.35,1.96,2.74,3.81,5.58,10.77]

            for i in range(20):
                buffer=data1[i]*multlist[i]
                data1['mix']=data1['mix']+buffer

            data1['mix_rank']=data1.groupby('trade_date')['mix'].rank(ascending=True,pct=True,method='first')

            #print(data1)

            if(counter==0):
                resultdf=data1
            else:
                resultdf['mix']=resultdf['mix']+data1['mix']
                resultdf['mix_rank']=resultdf['mix_rank']+data1['mix_rank']
            pass

        print(resultdf)

        resultdf.to_csv(predictname)

        return predictname

    def MixOutputresult_groupbalence(self,featurepath,modelpath,real_predict=False):

        (filepath, tempfilename)=os.path.split(featurepath)
        (filename, extension) = os.path.splitext(tempfilename)

        predictname=modelpath+'_'+filename+'_result.csv'

        if(real_predict):
            mixdf=pd.read_csv(featurepath,index_col=0,header=0)
        else:
            mixdf=pd.read_pickle(featurepath)
        print(mixdf)
        mixdf=mixdf[['ts_code','trade_date','Shift_1total_mv_rank','close']]
        
        mixdf.rename(columns = {"close":"close_show"},  inplace=True)
        print(mixdf)

        if os.path.exists(predictname)==True and (not real_predict):
            #mixdf=pd.read_csv(predictname,index_col=0,header=0)
            #print(mixdf)

            print("合成预测结果已生成")
            return predictname


        resultdf=[]
        for counter in range(4):

            predictpath_new=modelpath+'_'+filename+'_'+str(counter)+".pkl"

            data1=pd.read_pickle(predictpath_new)

            #print(data1)

            data1['mix']=0

            multlist=[-9.34,-5.48,-4.2,-3.4,-2.7,-2.3,-1.86,-1.47,-1.09,-0.74,-0.38,0,0.398,0.838,1.35,1.96,2.74,3.81,5.58,10.77]

            for i in range(20):
                buffer=data1[i]*multlist[i]
                data1['mix']=data1['mix']+buffer

            data1['mix_rank']=data1.groupby('trade_date')['mix'].rank(ascending=True,pct=True,method='first')

            #print(data1)

            if(counter==0):
                resultdf=data1
            else:
                resultdf['mix']=resultdf['mix']+data1['mix']
                resultdf['mix_rank']=resultdf['mix_rank']+data1['mix_rank']
                resultdf[0]=resultdf[0]+data1[0]
                resultdf[19]=resultdf[19]+data1[19]
            pass

        print(resultdf)

        mixdf=pd.merge(mixdf, resultdf, how='left', on=['ts_code','trade_date'])
        print(mixdf)

        if(real_predict):
            predictname="Today_result.csv"

        mixdf.to_csv(predictname)

        return predictname

    def MixOutputresult_groupbalence_CB(self,featurepath,modelpath,real_predict=False):

        (filepath, tempfilename)=os.path.split(featurepath)
        (filename, extension) = os.path.splitext(tempfilename)

        predictname=modelpath+'_'+filename+'_result.csv'

        if(real_predict):
            mixdf=pd.read_csv(featurepath,index_col=0,header=0)
        else:
            mixdf=pd.read_pickle(featurepath)
        print(mixdf)
        mixdf=mixdf[['ts_code','trade_date','close']]
        
        mixdf.rename(columns = {"close":"close_show"},  inplace=True)
        print(mixdf)

        if os.path.exists(predictname)==True and (not real_predict):
            #mixdf=pd.read_csv(predictname,index_col=0,header=0)
            #print(mixdf)

            print("合成预测结果已生成")
            return predictname


        resultdf=[]
        for counter in range(4):

            predictpath_new=modelpath+'_'+filename+'_'+str(counter)+".pkl"

            data1=pd.read_pickle(predictpath_new)

            #print(data1)

            data1['mix']=0

            #multlist=[-9.34,-5.48,-4.2,-3.4,-2.7,-2.3,-1.86,-1.47,-1.09,-0.74,-0.38,0,0.398,0.838,1.35,1.96,2.74,3.81,5.58,10.77]
            multlist=[-7.26,-3.64,-2.52,-1.95,-1.45,-1.15,-0.85,-0.63,-0.36,-0.14,0,0.26,0.56,0.81,1.15,1.53,2.08,2.89,4.44,10.48]
            #multlist=[-3.55,-2.09,-1.55,-1.29,-1.00,-0.8,-0.6,-0.41,-0.25,-0.05,0.08,0.27,0.47,0.68,0.90,1.23,1.61,2.19,3.22,6.62]

            for i in range(20):
                buffer=data1[i]*multlist[i]
                data1['mix']=data1['mix']+buffer

            data1['mix_rank']=data1.groupby('trade_date')['mix'].rank(ascending=True,pct=True,method='first')

            #print(data1)

            if(counter==0):
                resultdf=data1
            else:
                resultdf['mix']=resultdf['mix']+data1['mix']
                resultdf['mix_rank']=resultdf['mix_rank']+data1['mix_rank']
                resultdf[0]=resultdf[0]+data1[0]
                resultdf[19]=resultdf[19]+data1[19]
            pass

        print(resultdf)

        mixdf=pd.merge(mixdf, resultdf, how='left', on=['ts_code','trade_date'])
        print(mixdf)

        if(real_predict):
            predictname="Today_result_CB.csv"

        mixdf.to_csv(predictname)

        return predictname

    #尝试将20分类结果进行回归
    def LGBmodelretrain(self,featurepath,resultpath):

        (filepath, tempfilename)=os.path.split(featurepath)
        (filename, extension) = os.path.splitext(tempfilename)

        modelfolder=filepath+'/'+filename
        CSZLUtils.CSZLUtils.mkdir(modelfolder)

        #基于使用的模块生成文件名
        pklname=modelfolder+'/'+sys._getframe().f_code.co_name
        lgb_model,pklname=self.LGBmodel_sum20(pklname)


        featurepath=CSZLUtils.CSZLUtils.pathchange(featurepath)

        counter=0

        for counter in range(4):

            modelsavepath=resultpath[:-5]
            modelfirstpredictpath=modelsavepath+str(counter)+'.pkl'
            modelsecpredictpath=modelsavepath+str(counter)+'_retrainmodel.pkl'
            #modelsecpredictresultpath=modelsavepath+str(counter)+'_retrainmodelresult.pkl'

            if(os.path.exists(modelsecpredictpath)==False):

                x_train,y_train,train_label=self.LGBdatasetprepar2(featurepath,modelfirstpredictpath)

                lgb_model.fit(x_train, y_train, 
                    #eval_set=[(x_valid, y_valid)],  
                    #early_stopping_rounds=verbose, 
                    verbose=50
                    )
                joblib.dump(lgb_model,modelsecpredictpath)


            #test_resultdf=lgb_model.predict(x_train)
            #data1 = pd.DataFrame(test_resultdf)
            #data2 = pd.DataFrame(y_train)
            #resultdf=train_label.join(data1)
            #resultdf2=train_label.join(data2)
            #showdf=pd.merge(resultdf, resultdf2, how='left', on=['ts_code','trade_date'])

            #print(showdf)
            #showdf.to_csv("sdf.csv")

            #savepath_new=pklname+'_'+str(counter)+".pkl"
            #joblib.dump(usemodel,savepath_new)     

        return modelfirstpredictpath

    def LGBmodelrepredict(self,featurepath,retraindata,retrainmodel):

        datapath=retraindata[:-5]
        finalpath=datapath+'retrainresult.pkl'

        if(os.path.exists(finalpath)==True):
            return checkpath

        counter=0

        resultdf=[]
        for counter in range(4):

            modelsavepath=retrainmodel[:-5]
            modelsecpredictpath=modelsavepath+str(counter)+'_retrainmodel.pkl'
            

            datapath=retraindata[:-5]
            modelfirstpredictpath=datapath+str(counter)+'.pkl'
            modelsecpredictresultpath=datapath+str(counter)+'_retrainresult.pkl'

            if(os.path.exists(modelsecpredictresultpath)==False):

                x_train,y_train,train_label=self.LGBdatasetprepar2(featurepath,modelfirstpredictpath)

                lgb_model = joblib.load(modelsecpredictpath)

                test_resultdf=lgb_model.predict(x_train)
                data1 = pd.DataFrame(test_resultdf)
                #data2 = pd.DataFrame(y_train)
                data2=train_label.join(data1)
                data2.rename(columns={0: 'mix',}, inplace=True)

                if(counter==0):
                    resultdf=data2
                    
                else:
                    resultdf['mix']=resultdf['mix']+data2['mix']
                    #resultdf['mix_rank']=resultdf['mix_rank']+data2['mix_rank']


                pass

            #savepath_new=pklname+'_'+str(counter)+".pkl"
            #joblib.dump(usemodel,savepath_new)     

        resultdf.to_csv(finalpath)
        return finalpath

    def LGBdatasetprepar2(self,featurepath,resultpath):

        #df_all=pd.read_csv(featurepath,index_col=0,header=0)
        #df_all=pd.read_pickle(featurepath)
        df_all=CSZLUtils.CSZLUtils.Loaddata(featurepath)
        df_predictfirst=CSZLUtils.CSZLUtils.Loaddata(resultpath)

        df_all=df_all[['ts_code','trade_date','tomorrow_chg','tomorrow_chg_rank']]

        df_all=pd.merge(df_all, df_predictfirst, how='inner', on=['ts_code','trade_date'])

        #df_all=df_all.reset_index(drop=True)
        
        y_train = np.array(df_all['tomorrow_chg'])
        train_label=df_all[['ts_code','trade_date']]
        x_train=df_all.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1)

        #print(x_train)

        return x_train,y_train,train_label