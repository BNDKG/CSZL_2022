#coding=utf-8
import CSZLData
import CSZLFeatureEngineering as FE
import CSZLModel
import CSZLDisplay
import CSZLUtils
import pandas as pd

class CSZLWorkflow(object):
    """各种workflow 主要就是back testing"""

    def BackTesting(self):

        #Default_folder_path='./temp/'
        Default_folder_path='D:/temp2/'

        #zzzz=CSZLData.CSZLData("20220101","20220301")

        #zzzz.getDataSet_all(Default_folder_path)

        #zzzz=FE.CSZLFeatureEngineering("20130101","20170301",Default_folder_path)
        #trainpath=zzzz.FE03()
        #zzzz=FE.CSZLFeatureEngineering("20170301","20220301",Default_folder_path)
        #testpath=zzzz.FE03()

        #zzzz=FE.CSZLFeatureEngineering("20220101","20220408",Default_folder_path)
        #testpath=zzzz.FE03()

        #zzzz=FE.CSZLFeatureEngineering("20190101","20190301",Default_folder_path)
        #trainpath=zzzz.FE03()
        #zzzz=FE.CSZLFeatureEngineering("20220101","20220301",Default_folder_path)
        #testpath=zzzz.FE03()

        zzzz=FE.CSZLFeatureEngineering("20190101","20210101",Default_folder_path)
        trainpath=zzzz.FE03()
        zzzz=FE.CSZLFeatureEngineering("20220101","20220401",Default_folder_path)
        testpath=zzzz.FE03()


        cur_model=CSZLModel.CSZLModel()

        cur_model_path=cur_model.LGBmodeltrain(trainpath)
        #resultpath2=cur_model.LGBmodelpredict(trainpath,cur_model_path)
        resultpath=cur_model.LGBmodelpredict(testpath,cur_model_path)


        #cur_model_path2=cur_model.LGBmodelretrain(trainpath,resultpath2)
        #resultpath3=cur_model.LGBmodelrepredict(testpath,resultpath,cur_model_path2)

        resultpath=cur_model.MixOutputresult_groupbalence(testpath,cur_model_path)


        curdisplay=CSZLDisplay.CSZLDisplay()
        curdisplay.Topk_nextopen(resultpath)

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

        #Default_folder_path='./temp/'
        Default_folder_path='D:/temp2/'


        zzzz=FE.CSZLFeatureEngineering("20190101","20210101",Default_folder_path)
        trainpath=zzzz.FE03()


        CSZLData.CSZLDataWithoutDate.get_realtime_quotes(Default_folder_path,"20220301","20220414")
        zzzz=FE.CSZLFeatureEngineering("20220301","20220414",Default_folder_path)
        #trainpath=zzzz.FE03()
        #bbbb=pd.read_pickle(trainpath)
        #aaaa=bbbb.head(10)
        #aaaa=aaaa.to_csv("tttt.csv")

        zzzz.FE03_real(20220415)
        featurepath="Today_Joinfeature.csv"

        cur_model=CSZLModel.CSZLModel()

        cur_model_path=cur_model.LGBmodeltrain(trainpath)
        #resultpath2=cur_model.LGBmodelpredict(trainpath,cur_model_path)
        resultpath=cur_model.LGBmodelpredict(featurepath,cur_model_path)
        
        resultpath=cur_model.MixOutputresult_groupbalence(featurepath,cur_model_path,resultpath)    
        
        pass

    def update_all(self):

        Default_folder_path='./temp/'

        zzzz=CSZLData.CSZLData("20220101","20220301")

        zzzz.getDataSet_all(Default_folder_path)

