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
        lgb_model,pklname=self.LGBmodel_001(pklname)
        checkpath=pklname+'_0.pkl'

        featurepath=CSZLUtils.CSZLUtils.pathchange(featurepath)

        if(os.path.exists(checkpath)==True):
            return pklname

        x_train,y_train,_=self.LGBdatasetprepar(featurepath)

        self.split_dataset_and_train(x_train,y_train,pklname,lgb_model)


        return pklname

    def LGBdatasetprepar(self,featurepath):

        #df_all=pd.read_csv(featurepath,index_col=0,header=0)
        #df_all=pd.read_pickle(featurepath)
        df_all=CSZLUtils.CSZLUtils.Loaddata(featurepath)

        df_all=df_all[df_all['st_or_otherwrong']==1]
        df_all.drop(['st_or_otherwrong','real_price'],axis=1,inplace=True)

        df_all=df_all.reset_index(drop=True)
        
        y_train = np.array(df_all['tomorrow_chg_rank'])
        train_label=df_all[['ts_code','trade_date']]
        x_train=df_all.drop(['tomorrow_chg','tomorrow_chg_rank','ts_code','trade_date'],axis=1)

        return x_train,y_train,train_label

    def LGBmodel_001(self,pklname):

        pklname=pklname+sys._getframe().f_code.co_name

        lgb_model = lgb.LGBMClassifier(max_depth=-1,
                                        n_estimators=300,
                                        learning_rate=0.05,
                                        num_leaves=2**8-1,
                                        colsample_bytree=0.6,
                                        objective='multiclass', 
                                        num_class=20,
                                        n_jobs=-1)



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

        modelname=modelpath+'_0.pkl'
        predictname=modelpath+'_0_predict.pkl'

        if(os.path.exists(predictname)==True):
            print("预测结果已生成")
            return predictname


        x_train,y_train,train_label=self.LGBdatasetprepar(featurepath)

        #finaldf = pd.merge(train_label, x_train, how='left', left_index=True, right_index=True) 
        #print(finaldf)


        train2=train_label.copy(deep=True)

        for counter in range(4):
            modelpath_new=modelpath+'_'+str(counter)+".pkl"
            predictpath_new=modelpath+'_'+str(counter)+"_predict.pkl"

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


    def MixOutputresult(self,modelpath):

        predictname=modelpath+'_result.csv'

        if(os.path.exists(predictname)==True):
            print("合成预测结果已生成")
            return predictname


        resultdf=[]
        for counter in range(4):

            predictpath_new=modelpath+'_'+str(counter)+"_predict.pkl"

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
