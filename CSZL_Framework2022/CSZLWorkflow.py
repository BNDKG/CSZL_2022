#coding=utf-8
import CSZLData
import CSZLFeatureEngineering as FE
import CSZLModel
import CSZLDisplay

class CSZLWorkflow(object):
    """各种workflow 主要就是back testing"""

    def BackTesting(self):

        #Default_folder_path='./temp/'
        Default_folder_path='D:/temp2/'

        #zzzz=CSZLData.CSZLData("20220101","20220301")

        #zzzz.getDataSet_all(Default_folder_path)

        zzzz=FE.CSZLFeatureEngineering("20130101","20170301",Default_folder_path)
        trainpath=zzzz.FE03()
        zzzz=FE.CSZLFeatureEngineering("20170301","20220301",Default_folder_path)
        testpath=zzzz.FE03()

        #zzzz=FE.CSZLFeatureEngineering("20190101","20190301",Default_folder_path)
        #trainpath=zzzz.FE03()
        #zzzz=FE.CSZLFeatureEngineering("20220101","20220301",Default_folder_path)
        #testpath=zzzz.FE03()

        cur_model=CSZLModel.CSZLModel()

        cur_model_path=cur_model.LGBmodeltrain(trainpath)

        resultpath=cur_model.LGBmodelpredict(testpath,cur_model_path)
        resultpath=cur_model.MixOutputresult(testpath,cur_model_path)

        curdisplay=CSZLDisplay.CSZLDisplay()
        curdisplay.Topk_nextopen(resultpath)

        pass

    def update_all(self):

        Default_folder_path='./temp/'

        zzzz=CSZLData.CSZLData("20220101","20220301")

        zzzz.getDataSet_all(Default_folder_path)

