#coding=utf-8
import CSZLData

class CSZLWorkflow(object):
    """各种workflow 主要就是back testing"""

    def BackTesting(self):

        Default_folder_path='./temp/'

        #zzzz=CSZLData.CSZLData("20220101","20220301")

        #zzzz.getDataSet_all(Default_folder_path)


    def update_all(self):

        Default_folder_path='./temp/'

        zzzz=CSZLData.CSZLData("20220101","20220301")

        zzzz.getDataSet_all(Default_folder_path)
