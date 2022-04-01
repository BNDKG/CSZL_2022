#coding=utf-8
import CSZLData
import CSZLFeatureEngineering as FE

class CSZLWorkflow(object):
    """各种workflow 主要就是back testing"""

    def BackTesting(self):

        Default_folder_path='./temp/'

        #zzzz=CSZLData.CSZLData("20220101","20220301")

        #zzzz.getDataSet_all(Default_folder_path)

        zzzz=FE.CSZLFeatureEngineering("20220101","20220301",Default_folder_path)

        zzzz.FE01()


        asdfafsa=1

    def update_all(self):

        Default_folder_path='./temp/'

        zzzz=CSZLData.CSZLData("20220101","20220301")

        zzzz.getDataSet_all(Default_folder_path)
