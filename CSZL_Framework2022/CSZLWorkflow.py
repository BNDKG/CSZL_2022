#coding=utf-8
import CSZLData
import CSZLFeatureEngineering as FE
import CSZLModel

class CSZLWorkflow(object):
    """各种workflow 主要就是back testing"""

    def BackTesting(self):

        Default_folder_path='./temp/'

        #zzzz=CSZLData.CSZLData("20220101","20220301")

        #zzzz.getDataSet_all(Default_folder_path)

        zzzz=FE.CSZLFeatureEngineering("20220101","20220301",Default_folder_path)
        featurepath=zzzz.FE01()
        zzzz=FE.CSZLFeatureEngineering("20210101","20210301",Default_folder_path)
        featurepath2=zzzz.FE01()

        cur_model=CSZLModel.CSZLModel()

        cur_model_path=cur_model.LGBmodeltrain(featurepath)

        cur_model.LGBmodelpredict(featurepath2,cur_model_path)


        pass

    def update_all(self):

        Default_folder_path='./temp/'

        zzzz=CSZLData.CSZLData("20220101","20220301")

        zzzz.getDataSet_all(Default_folder_path)
