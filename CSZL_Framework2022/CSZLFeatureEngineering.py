#coding=utf-8

import CSZLData
import CSZLUtils
import pandas as pd
import numpy as np
import gc
import os
import sys
import re
from datetime import datetime

import talib as ta

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
        self.dfCBDaily=pd.DataFrame([])

        pass

    def FE03(self):


        savepath =self.create_trainingdatasets(self.create_target)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature3)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE05(self):


        savepath =self.create_trainingdatasets(self.create_target)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature5)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE06(self):


        savepath =self.create_trainingdatasets(self.create_target)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature6)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures_large,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE07(self):


        savepath =self.create_trainingdatasets(self.create_target)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature6)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures_1000,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE08(self):


        savepath =self.create_trainingdatasets(self.create_target)

        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature8)
        #savepath3 =self.create_trainingdatasets(self.create_dayfeature6)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures_1000,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE09(self):


        savepath =self.create_trainingdatasets(self.create_target)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature5)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        funname=sys._getframe().f_code.co_name
        funnameG=funname+"GroupFE"
        funpathG=self.Default_folder_path+funnameG+self.start_date+"to"+self.end_date+".pkl"

        savepath9 =self.create_trainingdatasets(self.create_Groupfeatures,0,funpathG,[savepath3,savepath4])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7,savepath9])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE09b(self):


        savepath =self.create_trainingdatasets(self.create_target)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature5)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        funname=sys._getframe().f_code.co_name
        funnameG=funname+"GroupFE"
        funpathG=self.Default_folder_path+funnameG+self.start_date+"to"+self.end_date+".pkl"

        savepath9 =self.create_trainingdatasets(self.create_Groupfeatures3,0,funpathG,[savepath3,savepath4])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7,savepath9])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE09c(self):


        savepath =self.create_trainingdatasets(self.create_target)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature5)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        funname=sys._getframe().f_code.co_name
        funnameG=funname+"GroupFE"
        funpathG=self.Default_folder_path+funnameG+self.start_date+"to"+self.end_date+".pkl"

        savepath9 =self.create_trainingdatasets(self.create_Groupfeatures5,0,funpathG,[savepath3,savepath4])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7,savepath9])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE09d(self):

        savepath =self.create_trainingdatasets(self.create_target)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature5)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        funname=sys._getframe().f_code.co_name
        funnameG=funname+"GroupFE"
        funpathG=self.Default_folder_path+funnameG+self.start_date+"to"+self.end_date+".pkl"

        savepath9 =self.create_trainingdatasets(self.create_Groupfeatures6,0,funpathG,[savepath3,savepath4])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7,savepath9])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE11b(self):

        #create_Limitfeaturewithdate
        savepath =self.create_trainingdatasets(self.create_target)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeaturewithdate)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature5)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        funname=sys._getframe().f_code.co_name
        funnameG=funname+"GroupFE"
        funpathG=self.Default_folder_path+funnameG+self.start_date+"to"+self.end_date+".pkl"

        savepath9 =self.create_trainingdatasets(self.create_Groupfeatures6,0,funpathG,[savepath3,savepath4])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7,savepath9])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE11c(self):

        #create_Limitfeaturewithdate
        savepath =self.create_trainingdatasets(self.create_target2)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeaturewithdate)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature5)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        funname=sys._getframe().f_code.co_name
        funnameG=funname+"GroupFE"
        funpathG=self.Default_folder_path+funnameG+self.start_date+"to"+self.end_date+".pkl"

        savepath9 =self.create_trainingdatasets(self.create_Groupfeatures6,0,funpathG,[savepath3,savepath4])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7,savepath9])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE20d(self):

        savepath =self.create_trainingdatasets(self.create_target3)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature5)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        funname=sys._getframe().f_code.co_name
        funnameG=funname+"GroupFE"
        funpathG=self.Default_folder_path+funnameG+self.start_date+"to"+self.end_date+".pkl"

        savepath9 =self.create_trainingdatasets(self.create_Groupfeatures6,0,funpathG,[savepath3,savepath4])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7,savepath9])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE20e(self):

        savepath =self.create_trainingdatasets(self.create_target3)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature5)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        funname=sys._getframe().f_code.co_name
        funnameG=funname+"GroupFE"
        funpathG=self.Default_folder_path+funnameG+self.start_date+"to"+self.end_date+".pkl"

        savepath9 =self.create_trainingdatasets(self.create_Groupfeatures6,0,funpathG,[savepath3,savepath4])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7,savepath9])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE21e(self):

        savepath =self.create_trainingdatasets(self.create_target3)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature25)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature3)

        funname=sys._getframe().f_code.co_name
        funnameG=funname+"GroupFE"
        funpathG=self.Default_folder_path+funnameG+self.start_date+"to"+self.end_date+".pkl"

        savepath9 =self.create_trainingdatasets(self.create_Groupfeatures6,0,funpathG,[savepath3,savepath4])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7,savepath9])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE11cBigonly(self):

        #create_Limitfeaturewithdate
        savepath =self.create_trainingdatasets(self.create_target2)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeaturewithdate)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature5)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        funname=sys._getframe().f_code.co_name
        funnameG=funname+"GroupFE"
        funpathG=self.Default_folder_path+funnameG+self.start_date+"to"+self.end_date+".pkl"

        savepath9 =self.create_trainingdatasets(self.create_Groupfeatures6,0,funpathG,[savepath3,savepath4])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7,savepath9])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE11a(self):


        savepath =self.create_trainingdatasets(self.create_target)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeatureandindustry)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature5)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        funname=sys._getframe().f_code.co_name
        funnameG=funname+"GroupFE"
        funpathG=self.Default_folder_path+funnameG+self.start_date+"to"+self.end_date+".pkl"

        savepath9 =self.create_trainingdatasets(self.create_Groupfeatures6,0,funpathG,[savepath3,savepath4])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7,savepath9])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE10(self):


        savepath =self.create_trainingdatasets(self.create_target)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature_little)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        funname=sys._getframe().f_code.co_name
        funnameG=funname+"GroupFE"
        funpathG=self.Default_folder_path+funnameG+self.start_date+"to"+self.end_date+".pkl"

        savepath9 =self.create_trainingdatasets(self.create_Groupfeatures,0,funpathG,[savepath3,savepath4])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath9])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE10b(self):


        savepath =self.create_trainingdatasets(self.create_target)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature_little)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        funname=sys._getframe().f_code.co_name
        funnameG=funname+"GroupFE"
        funpathG=self.Default_folder_path+funnameG+self.start_date+"to"+self.end_date+".pkl"

        savepath9 =self.create_trainingdatasets(self.create_Groupfeatures2,0,funpathG,[savepath3,savepath4])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath9])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE10c(self):


        savepath =self.create_trainingdatasets(self.create_target)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature_little)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        funname=sys._getframe().f_code.co_name
        funnameG=funname+"GroupFE"
        funpathG=self.Default_folder_path+funnameG+self.start_date+"to"+self.end_date+".pkl"

        savepath9 =self.create_trainingdatasets(self.create_Groupfeatures3,0,funpathG,[savepath3,savepath4])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath9])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE10d(self):


        savepath =self.create_trainingdatasets(self.create_target)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature_little)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        funname=sys._getframe().f_code.co_name
        funnameG=funname+"GroupFE"
        funpathG=self.Default_folder_path+funnameG+self.start_date+"to"+self.end_date+".pkl"

        savepath9 =self.create_trainingdatasets(self.create_Groupfeatures5,0,funpathG,[savepath3,savepath4])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath9])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE10e(self):


        savepath =self.create_trainingdatasets(self.create_target)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature_little)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        funname=sys._getframe().f_code.co_name
        funnameG=funname+"GroupFE"
        funpathG=self.Default_folder_path+funnameG+self.start_date+"to"+self.end_date+".pkl"

        savepath9 =self.create_trainingdatasets(self.create_Groupfeatures5b,0,funpathG,[savepath3,savepath4])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath9])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE10f(self):


        savepath =self.create_trainingdatasets(self.create_target)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature_little2)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        funname=sys._getframe().f_code.co_name
        funnameG=funname+"GroupFE"
        funpathG=self.Default_folder_path+funnameG+self.start_date+"to"+self.end_date+".pkl"

        savepath9 =self.create_trainingdatasets(self.create_Groupfeatures6,0,funpathG,[savepath3,savepath4])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath9])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE04(self):


        savepath =self.create_trainingdatasets(self.create_target)
        savepath2 =self.create_trainingdatasets(self.create_Limitfeature)
        savepath3 =self.create_trainingdatasets(self.create_dayfeature3)
        savepath4 =self.create_trainingdatasets(self.create_Longfeature)
        savepath5 =self.create_trainingdatasets(self.create_Moneyflowfeature2)

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        savepath6 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath4)
        savepath7 =self.create_trainingdatasets(self.create_Shiftfeatures,1,savepath5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        #savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3,savepath6,savepath7])
        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2,savepath3])


        #self.create_Shiftfeatures(mpath,"Moneyflow",1)
        #self.create_Shiftfeatures(lpath,"Longfeature",1)

        return savepath8

    def FE03_real(self,date):

        #20220415
        df_dayfeature =self.create_dayfeature3(date)
        #print(df_featured)

        df_limit =self.create_Limitfeature(date)

        df_long =self.create_Longfeature(date)
        df_money =self.create_Moneyflowfeature2(date)

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        df_shift_long=self.create_Shiftfeatures([-1,df_long])
        df_shift_money=self.create_Shiftfeatures([-1,df_money])


        df_joinfeature=self.create_joinfeatures_real([df_dayfeature,df_limit,df_shift_long,df_shift_money])

        finalday=df_joinfeature['trade_date'].max()
        df_joinfeature=df_joinfeature[df_joinfeature['trade_date']==finalday]
        print(df_joinfeature)

        df_joinfeature.to_csv("Today_Joinfeature.csv")


        return df_joinfeature

    def FE05_real(self,date):

        #20220415
        df_dayfeature =self.create_dayfeature5(date)
        #print(df_featured)

        df_limit =self.create_Limitfeature(date)

        df_long =self.create_Longfeature(date)
        df_money =self.create_Moneyflowfeature2(date)

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        df_shift_long=self.create_Shiftfeatures([-1,df_long])
        df_shift_money=self.create_Shiftfeatures([-1,df_money])


        df_joinfeature=self.create_joinfeatures_real([df_dayfeature,df_limit,df_shift_long,df_shift_money])

        finalday=df_joinfeature['trade_date'].max()
        df_joinfeature=df_joinfeature[df_joinfeature['trade_date']==finalday]
        print(df_joinfeature)

        df_joinfeature.to_csv("Today_Joinfeature.csv")


        return df_joinfeature

    def FE09c_real(self,date):

        #20220415
        df_dayfeature =self.create_dayfeature5(date)
        #print(df_featured)

        df_limit =self.create_Limitfeature(date)

        df_long =self.create_Longfeature(date)
        df_money =self.create_Moneyflowfeature2(date)

        df_group=self.create_Groupfeatures5([-1,df_dayfeature,df_long])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        df_shift_long=self.create_Shiftfeatures([-1,df_long])
        df_shift_money=self.create_Shiftfeatures([-1,df_money])

        df_joinfeature=self.create_joinfeatures_real([df_dayfeature,df_limit,df_shift_long,df_shift_money,df_group])

        finalday=df_joinfeature['trade_date'].max()
        df_joinfeature=df_joinfeature[df_joinfeature['trade_date']==finalday]
        print(df_joinfeature)

        df_joinfeature.to_csv("Today_Joinfeature.csv")


        return df_joinfeature

    def FE09d_real(self,date):

        #20220415
        df_dayfeature =self.create_dayfeature5(date)
        #print(df_featured)

        df_limit =self.create_Limitfeature(date)

        df_long =self.create_Longfeature(date)
        df_money =self.create_Moneyflowfeature2(date)

        df_group=self.create_Groupfeatures6([-1,df_dayfeature,df_long])

        #函数名，区别数字(例如shift 1 就填入1)，区别函数(例如 Moneyflowpath)
        df_shift_long=self.create_Shiftfeatures([-1,df_long])
        df_shift_money=self.create_Shiftfeatures([-1,df_money])

        df_joinfeature=self.create_joinfeatures_real([df_dayfeature,df_limit,df_shift_long,df_shift_money,df_group])

        finalday=df_joinfeature['trade_date'].max()
        df_joinfeature=df_joinfeature[df_joinfeature['trade_date']==finalday]
        print(df_joinfeature)

        df_joinfeature.to_csv("Today_Joinfeature.csv")


        return df_joinfeature

    def FECB01(self):

        #20220415
        savepath =self.create_trainingdatasets(self.create_CBtarget)
        savepath2 =self.create_trainingdatasets(self.create_CBdayfeature3)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2])


        return savepath8

    def FECB02(self):

        #20220415
        savepath =self.create_trainingdatasets(self.create_CBtarget)
        savepath2 =self.create_trainingdatasets(self.create_CBdayfeature4)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2])


        return savepath8

    def FECB03(self):

        #20220415
        savepath =self.create_trainingdatasets(self.create_CBtarget)
        savepath2 =self.create_trainingdatasets(self.create_CBdayfeature5)

        funname=sys._getframe().f_code.co_name
        funpath=self.Default_folder_path+funname+self.start_date+"to"+self.end_date+".pkl"

        savepath8 =self.create_trainingdatasets(self.create_joinfeatures,0,funpath,[savepath,savepath2])


        return savepath8

    def FECB01_real(self,date):

        #20220415
        df_dayfeature =self.create_CBdayfeature3(date)
        #print(df_featured)

        df_joinfeature=df_dayfeature
        #df_joinfeature=self.create_joinfeatures_real([df_dayfeature])

        finalday=df_joinfeature['trade_date'].max()
        df_joinfeature=df_joinfeature[df_joinfeature['trade_date']==finalday]
        print(df_joinfeature)

        df_joinfeature.to_csv("Today_Joinfeature_CB.csv")


        return df_joinfeature

    def FECB02_real(self,date):

        #20220415
        df_dayfeature =self.create_CBdayfeature4(date)
        #print(df_featured)

        df_joinfeature=df_dayfeature
        #df_joinfeature=self.create_joinfeatures_real([df_dayfeature])

        finalday=df_joinfeature['trade_date'].max()
        df_joinfeature=df_joinfeature[df_joinfeature['trade_date']==finalday]
        print(df_joinfeature)

        df_joinfeature.to_csv("Today_Joinfeature_CB.csv")


        return df_joinfeature

    def FECB03_real(self,date):

        #20220415
        df_dayfeature =self.create_CBdayfeature5(date)
        #print(df_featured)

        df_joinfeature=df_dayfeature
        #df_joinfeature=self.create_joinfeatures_real([df_dayfeature])

        finalday=df_joinfeature['trade_date'].max()
        df_joinfeature=df_joinfeature[df_joinfeature['trade_date']==finalday]
        print(df_joinfeature)

        df_joinfeature.to_csv("Today_Joinfeature_CB.csv")


        return df_joinfeature

    def create_joinfeatures(self,arges):

        features=arges[2]

        df=[]
        count=0
        for featurename in features:
            if count==0:
                #df=pd.read_csv(featurename,index_col=0,header=0)
                df=CSZLUtils.CSZLUtils.Loaddata(featurename)
                #df=pd.read_pickle(featurename)
                count+=1
                continue
            #df2=pd.read_csv(featurename,index_col=0,header=0)
            df2=CSZLUtils.CSZLUtils.Loaddata(featurename)
            #df2=pd.read_pickle(featurename)
            df=pd.merge(df, df2, how='left', on=['ts_code','trade_date'])
            del df2
            gc.collect()
            count+=1

        df=df.replace(np.nan, 0)

        return df

    def create_joinfeatures_large(self,arges):

        features=arges[2]

        df=[]
        count=0
        for featurename in features:
            if count==0:
                #df=pd.read_csv(featurename,index_col=0,header=0)
                df=CSZLUtils.CSZLUtils.Loaddata(featurename)
                #df=pd.read_pickle(featurename)
                count+=1
                continue
            #df2=pd.read_csv(featurename,index_col=0,header=0)
            df2=CSZLUtils.CSZLUtils.Loaddata(featurename)
            #df2=pd.read_pickle(featurename)
            df=pd.merge(df, df2, how='left', on=['ts_code','trade_date'])
            del df2
            gc.collect()
            count+=1

        df=df.replace(np.nan, 0)

        df=df[df['amount_rank']>0.8]

        return df

    def create_joinfeatures_1000(self,arges):

        features=arges[2]

        df=[]
        count=0
        for featurename in features:
            if count==0:
                #df=pd.read_csv(featurename,index_col=0,header=0)
                df=CSZLUtils.CSZLUtils.Loaddata(featurename)
                #df=pd.read_pickle(featurename)
                count+=1
                continue
            #df2=pd.read_csv(featurename,index_col=0,header=0)
            df2=CSZLUtils.CSZLUtils.Loaddata(featurename)
            #df2=pd.read_pickle(featurename)
            df=pd.merge(df, df2, how='left', on=['ts_code','trade_date'])
            del df2
            gc.collect()
            count+=1

        df=df.replace(np.nan, 0)

        df=df[df['amount_rank']>0.5]

        return df

    def create_joinfeatures_real(self,features):

        df=[]
        count=0
        for df_feature in features:
            if count==0:
                #df=pd.read_csv(featurename,index_col=0,header=0)
                df=df_feature
                #df=pd.read_pickle(featurename)
                count+=1
                continue
            #df2=pd.read_csv(featurename,index_col=0,header=0)
            df2=df_feature
            #df2=pd.read_pickle(featurename)
            df=pd.merge(df, df2, how='left', on=['ts_code','trade_date'])
            del df2
            gc.collect()
            count+=1

        df=df.replace(np.nan, 0)

        return df

    ######创建每种特征的数据集，返回文件名
    def create_target(self,arges):

        df=self.LoaddfDailydata().copy(deep=True)
        df2=self.LoaddfAdj_factor().copy(deep=True)


        df=pd.merge(df, df2, how='inner', on=['ts_code','trade_date'])

        df['real_price']=df['close']*df['adj_factor']

        df=df[["ts_code","trade_date","real_price"]]

        df=self.PedictDaysRank(df,5)

        df=df[["ts_code","trade_date","tomorrow_chg","tomorrow_chg_rank"]]

        del df2
        gc.collect()

        return df

    def create_target3(self,arges):

        df=self.LoaddfDailydata().copy(deep=True)
        df2=self.LoaddfAdj_factor().copy(deep=True)
        df3=self.LoaddfLongfactor().copy(deep=True)

        df=pd.merge(df, df2, how='inner', on=['ts_code','trade_date'])
        df=pd.merge(df, df3, how='inner', on=['ts_code','trade_date'])

        df['real_price']=df['close']*df['adj_factor']

        df.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df['total_mv_rank']=df.groupby('trade_date')['total_mv'].rank(pct=True)
        #df['total_mv_rank']=df.groupby('ts_code')['total_mv_rank'].shift(1)
        df['total_mv_rank']=df['total_mv_rank']*19.9//1

        df=df[df["total_mv_rank"]>15]

        print(df)

        df=df[["ts_code","trade_date","real_price"]]

        df=self.PedictDaysRank(df,5)

        df=df[["ts_code","trade_date","tomorrow_chg","tomorrow_chg_rank"]]

        del df2
        gc.collect()

        return df

    def create_target2(self,arges):

        df=self.LoaddfDailydata().copy(deep=True)
        df2=self.LoaddfAdj_factor().copy(deep=True)


        df=pd.merge(df, df2, how='inner', on=['ts_code','trade_date'])

        df['real_price']=df['close']*df['adj_factor']
        CSZLUtils.CSZLUtils.showmore()
        print(df)

        df=df[["ts_code","trade_date","real_price","pct_chg"]]

        #df.set_index(['ts_code', 'trade_date'], inplace=True)
        # 计算近5日的方差并赋值给新的一列
        df['variance_20d'] = df.groupby('ts_code')['pct_chg'].rolling(window=20).var().reset_index(level=0, drop=True)
        df['variance_5d'] = df.groupby('ts_code')['pct_chg'].rolling(window=5).var().reset_index(level=0, drop=True)

        #df.to_csv("zzzaaazz.csv")
        # 重置索引
        df.reset_index(inplace=True)

        df=self.PedictDaysRank(df,5)

        df=df[["ts_code","trade_date","tomorrow_chg","tomorrow_chg_rank","variance_5d","variance_20d"]]

        del df2
        gc.collect()

        return df

    def create_dayfeature3(self,arges):

        df=self.LoaddfDailydata().copy(deep=True)
        df2=self.LoaddfAdj_factor().copy(deep=True)

        if arges:

            loadpath="real_buffer.csv"
            df_today=pd.read_csv(loadpath,index_col=0,header=0)
            df_today['trade_date']=arges

            df['ts_code']=df['ts_code'].apply(lambda x : x[:-3])
            df_today['ts_code']=df_today['ts_code'].apply(lambda x:str(x).zfill(6))

            df = df.append(df_today, ignore_index=True)

            lastday=df2['trade_date'].max()
            df2['ts_code']=df2['ts_code'].apply(lambda x : x[:-3])
            copy_df=df2[df2['trade_date']==lastday]
            copy_df.loc[:,'trade_date']=arges
            
            df2 = df2.append(copy_df, ignore_index=True)


        df=pd.merge(df, df2, how='inner', on=['ts_code','trade_date'])

        df['real_price']=df['close']*df['adj_factor']

        df=df[["ts_code","trade_date","real_price","open","high","low","pct_chg","pre_close","close","amount"]]

        df['high_stop']=0
        df.loc[df['pct_chg']>9.4,'high_stop']=1

        df['chg_rank']=df.groupby('trade_date')['pct_chg'].rank(pct=True)
        df['pct_chg_abs']=df['pct_chg'].abs()
        df['pct_chg_abs_rank']=df.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        #计算三种比例rank
        dolist=['open','high','low']

        df['pct_chg_r']=df['pct_chg']

        for curc in dolist:
            buffer=((df[curc]-df['pre_close'])*100)/df['pre_close']
            df[curc]=buffer
            df[curc]=df.groupby('trade_date')[curc].rank(pct=True)

        #print(df)

        df=self.OldFeaturesRank(df,['open','high','low','pct_chg_r'],1)

        df=self.InputChgSumRank(df,6,'pct_chg_abs')
        df=self.InputChgSumRank(df,3,'pct_chg')
        df=self.InputChgSumRank(df,6,'pct_chg')
        df=self.InputChgSumRank(df,12,'pct_chg')
        df=self.InputChgSumRank(df,24,'pct_chg')

        df=self.InputChgSum(df,3,'pct_chg')
        df=self.InputChgSum(df,6,'pct_chg')
        df=self.InputChgSum(df,12,'pct_chg')
        df=self.InputChgSum(df,24,'pct_chg')

        #df['chg_rank_24_diff']=df['chg_rank_24']-df['chg_rank_12']
        #df['chg_rank_12_diff']=df['chg_rank_12']-df['chg_rank_6']
        #df['chg_rank_6_diff']=df['chg_rank_6']-df['chg_rank_3']

        #df['pct_chg_24_diff']=df['pct_chg_24']-df['pct_chg_12']
        #df['pct_chg_12_diff']=df['pct_chg_12']-df['pct_chg_6']
        #df['pct_chg_6_diff']=df['pct_chg_6']-df['pct_chg_3']

        df=self.HighLowRange(df,5)
        df=self.HighLowRange(df,12)
        df=self.HighLowRange(df,25)


        df=self.CloseWithHighLow(df,5)
        df=self.CloseWithHighLow(df,12)
        df=self.CloseWithHighLow(df,25)
        df=self.CloseWithHighLow(df,5,'max')
        df=self.CloseWithHighLow(df,12,'max')
        df=self.CloseWithHighLow(df,25,'max')

        #df['25_pct_rank_min_diff']=df['25_pct_rank_min']-df['12_pct_rank_min']
        #df['12_pct_rank_min_diff']=df['12_pct_rank_min']-df['5_pct_rank_min']

        #df['25_pct_rank_max_diff']=df['25_pct_rank_max']-df['12_pct_rank_max']
        #df['12_pct_rank_max_diff']=df['12_pct_rank_max']-df['5_pct_rank_max']

        #df['25_pct_Rangerank_diff']=df['25_pct_Rangerank']-df['12_pct_Rangerank']
        #df['12_pct_Rangerank_diff']=df['12_pct_Rangerank']-df['5_pct_Rangerank']


        #df.to_csv("dfsdf")

        del df2
        gc.collect()
        
        return df

    def create_dayfeature5(self,arges):

        df=self.LoaddfDailydata().copy(deep=True)
        df2=self.LoaddfAdj_factor().copy(deep=True)

        if arges:
            #when used by real fun it will concat todays data,so that it can product todays result
            #当被real方法调用时，拼接当日的数据，使得模型可以在当日做出预测
            loadpath="real_buffer.csv"
            df_today=pd.read_csv(loadpath,index_col=0,header=0)
            df_today['trade_date']=arges

            df['ts_code']=df['ts_code'].apply(lambda x : x[:-3])
            df_today['ts_code']=df_today['ts_code'].apply(lambda x:str(x).zfill(6))

            df = df.append(df_today, ignore_index=True)

            lastday=df2['trade_date'].max()
            df2['ts_code']=df2['ts_code'].apply(lambda x : x[:-3])
            copy_df=df2[df2['trade_date']==lastday]
            copy_df.loc[:,'trade_date']=arges
            
            df2 = df2.append(copy_df, ignore_index=True)


        df=pd.merge(df, df2, how='inner', on=['ts_code','trade_date'])

        df['real_price']=df['close']*df['adj_factor']

        df=df[["ts_code","trade_date","real_price","open","high","low","pct_chg","pre_close","close","amount"]]

        df=self.InputChgSum(df,3,'amount')
        df=self.InputChgSum(df,6,'amount')
        df=self.InputChgSum(df,12,'amount')
        df=self.InputChgSum(df,24,'amount')

        df['am_pct_1_3']=df['amount_3']/df['amount']
        df['am_pct_3_6']=df['amount_6']/df['amount_3']
        df['am_pct_6_12']=df['amount_12']/df['amount_6']
        df['am_pct_12_24']=df['amount_24']/df['amount_12']

        df['am_rank_1_3']=df.groupby('trade_date')['am_pct_1_3'].rank(pct=True)
        df['am_rank_3_6']=df.groupby('trade_date')['am_pct_3_6'].rank(pct=True)
        df['am_rank_6_12']=df.groupby('trade_date')['am_pct_6_12'].rank(pct=True)
        df['am_rank_12_24']=df.groupby('trade_date')['am_pct_12_24'].rank(pct=True)

        #print(df)

        df['high_stop']=0
        df.loc[df['pct_chg']>9.4,'high_stop']=1

        df['chg_rank']=df.groupby('trade_date')['pct_chg'].rank(pct=True)
        df['pct_chg_abs']=df['pct_chg'].abs()
        df['pct_chg_abs_rank']=df.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        #计算三种比例rank
        dolist=['open','high','low']

        df['pct_chg_r']=df['pct_chg']

        for curc in dolist:
            buffer=((df[curc]-df['pre_close'])*100)/df['pre_close']
            df[curc]=buffer
            df[curc]=df.groupby('trade_date')[curc].rank(pct=True)

        #print(df)

        df=self.OldFeaturesRank(df,['open','high','low','pct_chg_r'],1)

        df=self.InputChgSumRank(df,6,'pct_chg_abs')
        df=self.InputChgSumRank(df,3,'pct_chg')
        df=self.InputChgSumRank(df,6,'pct_chg')
        df=self.InputChgSumRank(df,12,'pct_chg')
        df=self.InputChgSumRank(df,24,'pct_chg')

        df=self.InputChgSum(df,3,'pct_chg')
        df=self.InputChgSum(df,6,'pct_chg')
        df=self.InputChgSum(df,12,'pct_chg')
        df=self.InputChgSum(df,24,'pct_chg')

        #df['chg_rank_24_diff']=df['chg_rank_24']-df['chg_rank_12']
        #df['chg_rank_12_diff']=df['chg_rank_12']-df['chg_rank_6']
        #df['chg_rank_6_diff']=df['chg_rank_6']-df['chg_rank_3']

        #df['pct_chg_24_diff']=df['pct_chg_24']-df['pct_chg_12']
        #df['pct_chg_12_diff']=df['pct_chg_12']-df['pct_chg_6']
        #df['pct_chg_6_diff']=df['pct_chg_6']-df['pct_chg_3']

        df=self.HighLowRange(df,5)
        df=self.HighLowRange(df,12)
        df=self.HighLowRange(df,25)


        df=self.CloseWithHighLow(df,5)
        df=self.CloseWithHighLow(df,12)
        df=self.CloseWithHighLow(df,25)
        df=self.CloseWithHighLow(df,5,'max')
        df=self.CloseWithHighLow(df,12,'max')
        df=self.CloseWithHighLow(df,25,'max')

        #df['25_pct_rank_min_diff']=df['25_pct_rank_min']-df['12_pct_rank_min']
        #df['12_pct_rank_min_diff']=df['12_pct_rank_min']-df['5_pct_rank_min']

        #df['25_pct_rank_max_diff']=df['25_pct_rank_max']-df['12_pct_rank_max']
        #df['12_pct_rank_max_diff']=df['12_pct_rank_max']-df['5_pct_rank_max']

        #df['25_pct_Rangerank_diff']=df['25_pct_Rangerank']-df['12_pct_Rangerank']
        #df['12_pct_Rangerank_diff']=df['12_pct_Rangerank']-df['5_pct_Rangerank']


        #df.to_csv("dfsdf.csv")

        del df2
        gc.collect()
        
        return df

    def create_dayfeature6(self,arges):

        df=self.LoaddfDailydata().copy(deep=True)
        df2=self.LoaddfAdj_factor().copy(deep=True)

        if arges:

            loadpath="real_buffer.csv"
            df_today=pd.read_csv(loadpath,index_col=0,header=0)
            df_today['trade_date']=arges

            df['ts_code']=df['ts_code'].apply(lambda x : x[:-3])
            df_today['ts_code']=df_today['ts_code'].apply(lambda x:str(x).zfill(6))

            df = df.append(df_today, ignore_index=True)

            lastday=df2['trade_date'].max()
            df2['ts_code']=df2['ts_code'].apply(lambda x : x[:-3])
            copy_df=df2[df2['trade_date']==lastday]
            copy_df.loc[:,'trade_date']=arges
            
            df2 = df2.append(copy_df, ignore_index=True)


        df=pd.merge(df, df2, how='inner', on=['ts_code','trade_date'])

        df['real_price']=df['close']*df['adj_factor']

        df=df[["ts_code","trade_date","real_price","open","high","low","pct_chg","pre_close","close","amount"]]

        df=self.InputChgSum(df,3,'amount')
        df=self.InputChgSum(df,6,'amount')
        df=self.InputChgSum(df,12,'amount')
        df=self.InputChgSum(df,24,'amount')

        df['am_pct_1_3']=df['amount_3']/df['amount']
        df['am_pct_3_6']=df['amount_6']/df['amount_3']
        df['am_pct_6_12']=df['amount_12']/df['amount_6']
        df['am_pct_12_24']=df['amount_24']/df['amount_12']

        df['am_rank_1_3']=df.groupby('trade_date')['am_pct_1_3'].rank(pct=True)
        df['am_rank_3_6']=df.groupby('trade_date')['am_pct_3_6'].rank(pct=True)
        df['am_rank_6_12']=df.groupby('trade_date')['am_pct_6_12'].rank(pct=True)
        df['am_rank_12_24']=df.groupby('trade_date')['am_pct_12_24'].rank(pct=True)

        #print(df)

        df['high_stop']=0
        df.loc[df['pct_chg']>9.4,'high_stop']=1

        df['chg_rank']=df.groupby('trade_date')['pct_chg'].rank(pct=True)
        df['amount_rank']=df.groupby('trade_date')['amount'].rank(pct=True)

        df['pct_chg_abs']=df['pct_chg'].abs()
        df['pct_chg_abs_rank']=df.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        #计算三种比例rank
        dolist=['open','high','low']

        df['pct_chg_r']=df['pct_chg']

        for curc in dolist:
            buffer=((df[curc]-df['pre_close'])*100)/df['pre_close']
            df[curc]=buffer
            df[curc]=df.groupby('trade_date')[curc].rank(pct=True)

        #print(df)

        df=self.OldFeaturesRank(df,['open','high','low','pct_chg_r'],1)

        df=self.InputChgSumRank(df,6,'pct_chg_abs')
        df=self.InputChgSumRank(df,3,'pct_chg')
        df=self.InputChgSumRank(df,6,'pct_chg')
        df=self.InputChgSumRank(df,12,'pct_chg')
        df=self.InputChgSumRank(df,24,'pct_chg')

        df=self.InputChgSum(df,3,'pct_chg')
        df=self.InputChgSum(df,6,'pct_chg')
        df=self.InputChgSum(df,12,'pct_chg')
        df=self.InputChgSum(df,24,'pct_chg')

        #df['chg_rank_24_diff']=df['chg_rank_24']-df['chg_rank_12']
        #df['chg_rank_12_diff']=df['chg_rank_12']-df['chg_rank_6']
        #df['chg_rank_6_diff']=df['chg_rank_6']-df['chg_rank_3']

        #df['pct_chg_24_diff']=df['pct_chg_24']-df['pct_chg_12']
        #df['pct_chg_12_diff']=df['pct_chg_12']-df['pct_chg_6']
        #df['pct_chg_6_diff']=df['pct_chg_6']-df['pct_chg_3']

        df=self.HighLowRange(df,5)
        df=self.HighLowRange(df,12)
        df=self.HighLowRange(df,25)


        df=self.CloseWithHighLow(df,5)
        df=self.CloseWithHighLow(df,12)
        df=self.CloseWithHighLow(df,25)
        df=self.CloseWithHighLow(df,5,'max')
        df=self.CloseWithHighLow(df,12,'max')
        df=self.CloseWithHighLow(df,25,'max')

        #df['25_pct_rank_min_diff']=df['25_pct_rank_min']-df['12_pct_rank_min']
        #df['12_pct_rank_min_diff']=df['12_pct_rank_min']-df['5_pct_rank_min']

        #df['25_pct_rank_max_diff']=df['25_pct_rank_max']-df['12_pct_rank_max']
        #df['12_pct_rank_max_diff']=df['12_pct_rank_max']-df['5_pct_rank_max']

        #df['25_pct_Rangerank_diff']=df['25_pct_Rangerank']-df['12_pct_Rangerank']
        #df['12_pct_Rangerank_diff']=df['12_pct_Rangerank']-df['5_pct_Rangerank']


        #df.to_csv("dfsdf.csv")

        del df2
        gc.collect()
        
        return df

    def create_dayfeature8(self,arges):

        df=self.LoaddfDailydata().copy(deep=True)
        df2=self.LoaddfAdj_factor().copy(deep=True)

        if arges:

            loadpath="real_buffer.csv"
            df_today=pd.read_csv(loadpath,index_col=0,header=0)
            df_today['trade_date']=arges

            df['ts_code']=df['ts_code'].apply(lambda x : x[:-3])
            df_today['ts_code']=df_today['ts_code'].apply(lambda x:str(x).zfill(6))

            df = df.append(df_today, ignore_index=True)

            lastday=df2['trade_date'].max()
            df2['ts_code']=df2['ts_code'].apply(lambda x : x[:-3])
            copy_df=df2[df2['trade_date']==lastday]
            copy_df.loc[:,'trade_date']=arges
            
            df2 = df2.append(copy_df, ignore_index=True)


        df=pd.merge(df, df2, how='inner', on=['ts_code','trade_date'])

        df['realhigh_price']=df['high']*df['adj_factor']
        df['reallow_price']=df['low']*df['adj_factor']
        df['real_price']=df['close']*df['adj_factor']

        df=df[["ts_code","trade_date","realhigh_price","reallow_price","real_price","open","high","low","pct_chg","pre_close","close","amount"]]

        seedf=df[df["ts_code"]=="600000.SH"]

        CSZLUtils.CSZLUtils.showmore()

        indexx=0
        baseg=[]
        for name,group in df.groupby('ts_code'):
            
            #group["dif"], group["dea"], group["bar"] = ta.MACD(group['real_price'].values, fastperiod=12, slowperiod=26, signalperiod=9)
            #group['macd']=group["dif"]-group["dea"]
            dif, dea, bar = ta.MACD(group['real_price'].values, fastperiod=12, slowperiod=26, signalperiod=9)
            group['macd']=dif-dea

            group['slowk'], group['slowd'] = ta.STOCH(
			            group['realhigh_price'].values, 
			            group['reallow_price'].values, 
			            group['real_price'].values,
                                    fastk_period=9,
                                    slowk_period=3,
                                    slowk_matype=0,
                                    slowd_period=3,
                                    slowd_matype=0)
            # 求出J值，J = (3*K)-(2*D)
            group['slowj'] = list(map(lambda x,y: 3*x-2*y, group['slowk'], group['slowd']))


            if indexx==0:
                baseg=group
            else:
                baseg=baseg.append(group)

            
            indexx+=1

        #df["zzz"]=df.groupby('trade_date')['real_price'].apply(lambda x: ta.SMA(x,timeperiod=10))
        #print(baseg)
        #baseg.to_csv("xxxx.csv")



        #df["aa"]=df.groupby('trade_date').apply(lambda x: ta.MACD(x['real_price'].values, fastperiod=12, slowperiod=26, signalperiod=9))

        #print(df)


        ##seedf["SMA10"]=ta.SMA(seedf['real_price'],timeperiod=10)
        #seedf["dif"], seedf["dea"], seedf["bar"] = ta.MACD(seedf['real_price'].values, fastperiod=12, slowperiod=26, signalperiod=9)

        #seedf['macd']=seedf["dif"]-seedf["dea"]

        #seedf['slowk'], seedf['slowd'] = ta.STOCH(
			     #   seedf['realhigh_price'].values, 
			     #   seedf['reallow_price'].values, 
			     #   seedf['real_price'].values,
        #                        fastk_period=9,
        #                        slowk_period=3,
        #                        slowk_matype=0,
        #                        slowd_period=3,
        #                        slowd_matype=0)
        ## 求出J值，J = (3*K)-(2*D)
        #seedf['slowj'] = list(map(lambda x,y: 3*x-2*y, seedf['slowk'], seedf['slowd']))


        df=self.InputChgSum(df,3,'amount')
        df=self.InputChgSum(df,6,'amount')
        df=self.InputChgSum(df,12,'amount')
        df=self.InputChgSum(df,24,'amount')

        df['am_pct_1_3']=df['amount_3']/df['amount']
        df['am_pct_3_6']=df['amount_6']/df['amount_3']
        df['am_pct_6_12']=df['amount_12']/df['amount_6']
        df['am_pct_12_24']=df['amount_24']/df['amount_12']

        df['am_rank_1_3']=df.groupby('trade_date')['am_pct_1_3'].rank(pct=True)
        df['am_rank_3_6']=df.groupby('trade_date')['am_pct_3_6'].rank(pct=True)
        df['am_rank_6_12']=df.groupby('trade_date')['am_pct_6_12'].rank(pct=True)
        df['am_rank_12_24']=df.groupby('trade_date')['am_pct_12_24'].rank(pct=True)

        #print(df)

        df['high_stop']=0
        df.loc[df['pct_chg']>9.4,'high_stop']=1

        df['chg_rank']=df.groupby('trade_date')['pct_chg'].rank(pct=True)
        df['amount_rank']=df.groupby('trade_date')['amount'].rank(pct=True)

        df['pct_chg_abs']=df['pct_chg'].abs()
        df['pct_chg_abs_rank']=df.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        #计算三种比例rank
        dolist=['open','high','low']

        df['pct_chg_r']=df['pct_chg']

        for curc in dolist:
            buffer=((df[curc]-df['pre_close'])*100)/df['pre_close']
            df[curc]=buffer
            df[curc]=df.groupby('trade_date')[curc].rank(pct=True)

        #print(df)

        df=self.OldFeaturesRank(df,['open','high','low','pct_chg_r'],1)

        df=self.InputChgSumRank(df,6,'pct_chg_abs')
        df=self.InputChgSumRank(df,3,'pct_chg')
        df=self.InputChgSumRank(df,6,'pct_chg')
        df=self.InputChgSumRank(df,12,'pct_chg')
        df=self.InputChgSumRank(df,24,'pct_chg')

        df=self.InputChgSum(df,3,'pct_chg')
        df=self.InputChgSum(df,6,'pct_chg')
        df=self.InputChgSum(df,12,'pct_chg')
        df=self.InputChgSum(df,24,'pct_chg')

        #df['chg_rank_24_diff']=df['chg_rank_24']-df['chg_rank_12']
        #df['chg_rank_12_diff']=df['chg_rank_12']-df['chg_rank_6']
        #df['chg_rank_6_diff']=df['chg_rank_6']-df['chg_rank_3']

        #df['pct_chg_24_diff']=df['pct_chg_24']-df['pct_chg_12']
        #df['pct_chg_12_diff']=df['pct_chg_12']-df['pct_chg_6']
        #df['pct_chg_6_diff']=df['pct_chg_6']-df['pct_chg_3']

        df=self.HighLowRange(df,5)
        df=self.HighLowRange(df,12)
        df=self.HighLowRange(df,25)


        df=self.CloseWithHighLow(df,5)
        df=self.CloseWithHighLow(df,12)
        df=self.CloseWithHighLow(df,25)
        df=self.CloseWithHighLow(df,5,'max')
        df=self.CloseWithHighLow(df,12,'max')
        df=self.CloseWithHighLow(df,25,'max')

        #df['25_pct_rank_min_diff']=df['25_pct_rank_min']-df['12_pct_rank_min']
        #df['12_pct_rank_min_diff']=df['12_pct_rank_min']-df['5_pct_rank_min']

        #df['25_pct_rank_max_diff']=df['25_pct_rank_max']-df['12_pct_rank_max']
        #df['12_pct_rank_max_diff']=df['12_pct_rank_max']-df['5_pct_rank_max']

        #df['25_pct_Rangerank_diff']=df['25_pct_Rangerank']-df['12_pct_Rangerank']
        #df['12_pct_Rangerank_diff']=df['12_pct_Rangerank']-df['5_pct_Rangerank']


        #df.to_csv("dfsdf.csv")

        del df2
        gc.collect()
        
        return df

    def create_dayfeature25(self,arges):

        df=self.LoaddfDailydata().copy(deep=True)
        df2=self.LoaddfAdj_factor().copy(deep=True)
        df3=self.LoaddfLongfactor().copy(deep=True)

        if arges:
            #when used by real fun it will concat todays data,so that it can product todays result
            #当被real方法调用时，拼接当日的数据，使得模型可以在当日做出预测
            loadpath="real_buffer.csv"
            df_today=pd.read_csv(loadpath,index_col=0,header=0)
            df_today['trade_date']=arges

            df['ts_code']=df['ts_code'].apply(lambda x : x[:-3])
            df_today['ts_code']=df_today['ts_code'].apply(lambda x:str(x).zfill(6))

            df = df.append(df_today, ignore_index=True)

            lastday=df2['trade_date'].max()
            df2['ts_code']=df2['ts_code'].apply(lambda x : x[:-3])
            copy_df=df2[df2['trade_date']==lastday]
            copy_df.loc[:,'trade_date']=arges
            
            df2 = df2.append(copy_df, ignore_index=True)


        df=pd.merge(df, df2, how='inner', on=['ts_code','trade_date'])
        df=pd.merge(df, df3, how='left', on=['ts_code','trade_date'])

        df['real_price']=df['close']*df['adj_factor']

        df['total_mv_rank']=df.groupby('trade_date')['total_mv'].rank(pct=True)
        #df['total_mv_rank']=df.groupby('ts_code')['total_mv_rank'].shift(1)
        df['total_mv_rank']=df['total_mv_rank']*19.9//1

        df=df[df["total_mv_rank"]>15]

        df=df[["ts_code","trade_date","real_price","open","high","low","pct_chg","pre_close","close","amount"]]

        df=self.InputChgSum(df,3,'amount')
        df=self.InputChgSum(df,6,'amount')
        df=self.InputChgSum(df,12,'amount')
        df=self.InputChgSum(df,24,'amount')

        df['am_pct_1_3']=df['amount_3']/df['amount']
        df['am_pct_3_6']=df['amount_6']/df['amount_3']
        df['am_pct_6_12']=df['amount_12']/df['amount_6']
        df['am_pct_12_24']=df['amount_24']/df['amount_12']

        df['am_rank_1_3']=df.groupby('trade_date')['am_pct_1_3'].rank(pct=True)
        df['am_rank_3_6']=df.groupby('trade_date')['am_pct_3_6'].rank(pct=True)
        df['am_rank_6_12']=df.groupby('trade_date')['am_pct_6_12'].rank(pct=True)
        df['am_rank_12_24']=df.groupby('trade_date')['am_pct_12_24'].rank(pct=True)

        #print(df)

        df['high_stop']=0
        df.loc[df['pct_chg']>9.4,'high_stop']=1

        df['chg_rank']=df.groupby('trade_date')['pct_chg'].rank(pct=True)
        df['pct_chg_abs']=df['pct_chg'].abs()
        df['pct_chg_abs_rank']=df.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        #计算三种比例rank
        dolist=['open','high','low']

        df['pct_chg_r']=df['pct_chg']

        for curc in dolist:
            buffer=((df[curc]-df['pre_close'])*100)/df['pre_close']
            df[curc]=buffer
            df[curc]=df.groupby('trade_date')[curc].rank(pct=True)

        #print(df)

        df=self.OldFeaturesRank(df,['open','high','low','pct_chg_r'],1)

        df=self.InputChgSumRank(df,6,'pct_chg_abs')
        df=self.InputChgSumRank(df,3,'pct_chg')
        df=self.InputChgSumRank(df,6,'pct_chg')
        df=self.InputChgSumRank(df,12,'pct_chg')
        df=self.InputChgSumRank(df,24,'pct_chg')

        df=self.InputChgSum(df,3,'pct_chg')
        df=self.InputChgSum(df,6,'pct_chg')
        df=self.InputChgSum(df,12,'pct_chg')
        df=self.InputChgSum(df,24,'pct_chg')

        #df['chg_rank_24_diff']=df['chg_rank_24']-df['chg_rank_12']
        #df['chg_rank_12_diff']=df['chg_rank_12']-df['chg_rank_6']
        #df['chg_rank_6_diff']=df['chg_rank_6']-df['chg_rank_3']

        #df['pct_chg_24_diff']=df['pct_chg_24']-df['pct_chg_12']
        #df['pct_chg_12_diff']=df['pct_chg_12']-df['pct_chg_6']
        #df['pct_chg_6_diff']=df['pct_chg_6']-df['pct_chg_3']

        df=self.HighLowRange(df,5)
        df=self.HighLowRange(df,12)
        df=self.HighLowRange(df,25)


        df=self.CloseWithHighLow(df,5)
        df=self.CloseWithHighLow(df,12)
        df=self.CloseWithHighLow(df,25)
        df=self.CloseWithHighLow(df,5,'max')
        df=self.CloseWithHighLow(df,12,'max')
        df=self.CloseWithHighLow(df,25,'max')

        #df['25_pct_rank_min_diff']=df['25_pct_rank_min']-df['12_pct_rank_min']
        #df['12_pct_rank_min_diff']=df['12_pct_rank_min']-df['5_pct_rank_min']

        #df['25_pct_rank_max_diff']=df['25_pct_rank_max']-df['12_pct_rank_max']
        #df['12_pct_rank_max_diff']=df['12_pct_rank_max']-df['5_pct_rank_max']

        #df['25_pct_Rangerank_diff']=df['25_pct_Rangerank']-df['12_pct_Rangerank']
        #df['12_pct_Rangerank_diff']=df['12_pct_Rangerank']-df['5_pct_Rangerank']


        #df.to_csv("dfsdf.csv")

        del df2
        gc.collect()
        
        return df

    def create_dayfeature_little(self,arges):

        df=self.LoaddfDailydata().copy(deep=True)
        df2=self.LoaddfAdj_factor().copy(deep=True)

        if arges:

            loadpath="real_buffer.csv"
            df_today=pd.read_csv(loadpath,index_col=0,header=0)
            df_today['trade_date']=arges

            df['ts_code']=df['ts_code'].apply(lambda x : x[:-3])
            df_today['ts_code']=df_today['ts_code'].apply(lambda x:str(x).zfill(6))

            df = df.append(df_today, ignore_index=True)

            lastday=df2['trade_date'].max()
            df2['ts_code']=df2['ts_code'].apply(lambda x : x[:-3])
            copy_df=df2[df2['trade_date']==lastday]
            copy_df.loc[:,'trade_date']=arges
            
            df2 = df2.append(copy_df, ignore_index=True)


        df=pd.merge(df, df2, how='inner', on=['ts_code','trade_date'])

        df['real_price']=df['close']*df['adj_factor']

        df=df[["ts_code","trade_date","real_price","open","high","low","pct_chg","pre_close","close","amount"]]

        df=self.InputChgSum(df,3,'amount')


        #print(df)

        df['high_stop']=0
        df.loc[df['pct_chg']>9.4,'high_stop']=1

        df['chg_rank']=df.groupby('trade_date')['pct_chg'].rank(pct=True)
        df['pct_chg_abs']=df['pct_chg'].abs()
        df['pct_chg_abs_rank']=df.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        #计算三种比例rank
        dolist=['open','high','low']

        df=self.InputChgSum(df,3,'pct_chg')
        df=self.InputChgSum(df,6,'pct_chg')

        df['pct_chg_r']=df['pct_chg']

        for curc in dolist:
            buffer=((df[curc]-df['pre_close'])*100)/df['pre_close']
            df[curc]=buffer
            df[curc]=df.groupby('trade_date')[curc].rank(pct=True)

        #print(df)

        df=self.OldFeaturesRank(df,['open','high','low','pct_chg_r'],1)


        #df.to_csv("dfsdf.csv")

        del df2
        gc.collect()
        
        return df

    def create_dayfeature_little2(self,arges):

        df=self.LoaddfDailydata().copy(deep=True)
        df2=self.LoaddfAdj_factor().copy(deep=True)

        if arges:

            loadpath="real_buffer.csv"
            df_today=pd.read_csv(loadpath,index_col=0,header=0)
            df_today['trade_date']=arges

            df['ts_code']=df['ts_code'].apply(lambda x : x[:-3])
            df_today['ts_code']=df_today['ts_code'].apply(lambda x:str(x).zfill(6))

            df = df.append(df_today, ignore_index=True)

            lastday=df2['trade_date'].max()
            df2['ts_code']=df2['ts_code'].apply(lambda x : x[:-3])
            copy_df=df2[df2['trade_date']==lastday]
            copy_df.loc[:,'trade_date']=arges
            
            df2 = df2.append(copy_df, ignore_index=True)


        df=pd.merge(df, df2, how='inner', on=['ts_code','trade_date'])

        df['real_price']=df['close']*df['adj_factor']

        df=df[["ts_code","trade_date","real_price","open","high","low","pct_chg","pre_close","close","amount"]]

        df=self.InputChgSum(df,3,'amount')


        #print(df)

        df['high_stop']=0
        df.loc[df['pct_chg']>9.4,'high_stop']=1

        df['chg_rank']=df.groupby('trade_date')['pct_chg'].rank(pct=True)
        df['pct_chg_abs']=df['pct_chg'].abs()
        df['pct_chg_abs_rank']=df.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        #计算三种比例rank
        dolist=['open','high','low']

        df=self.InputChgSum(df,3,'pct_chg')
        df=self.InputChgSum(df,6,'pct_chg')
        df=self.InputChgSum(df,12,'pct_chg')
        df=self.InputChgSum(df,24,'pct_chg')

        df['pct_chg_r']=df['pct_chg']

        for curc in dolist:
            buffer=((df[curc]-df['pre_close'])*100)/df['pre_close']
            df[curc]=buffer
            df[curc]=df.groupby('trade_date')[curc].rank(pct=True)

        #print(df)

        df=self.OldFeaturesRank(df,['open','high','low','pct_chg_r'],1)


        #df.to_csv("dfsdf.csv")

        del df2
        gc.collect()
        
        return df

    def create_Moneyflowfeature2(self,arges):

        df=self.LoaddfMoneyflow().copy(deep=True)

        df.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        if arges:

            lastday=df['trade_date'].max()
            df['ts_code']=df['ts_code'].apply(lambda x : x[:-3])
            copy_df=df[df['trade_date']==lastday]
            copy_df.loc[:,'trade_date']=arges
            df = df.append(copy_df, ignore_index=True)

        df['sm_amount']=df['buy_sm_amount']-df['sell_sm_amount']
        df['lg_amount']=df['buy_lg_amount']-df['sell_lg_amount']


        df.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)


        #df['sm_amount']=df.groupby('ts_code')['sm_amount'].shift(1)
        #df['lg_amount']=df.groupby('ts_code')['lg_amount'].shift(1)
        #df['net_mf_amount']=df.groupby('ts_code')['net_mf_amount'].shift(1)

        df=self.InputChgSumRank(df,5,'sm_amount')
        df=self.InputChgSumRank(df,5,'lg_amount')
        df=self.InputChgSumRank(df,5,'net_mf_amount')

        df=self.InputChgSumRank(df,12,'sm_amount')
        df=self.InputChgSumRank(df,12,'lg_amount')
        df=self.InputChgSumRank(df,12,'net_mf_amount')

        df=self.InputChgSumRank(df,25,'sm_amount')
        df=self.InputChgSumRank(df,25,'lg_amount')
        df=self.InputChgSumRank(df,25,'net_mf_amount')

        df=self.OldFeaturesRank(df,['sm_amount','lg_amount','net_mf_amount'],1)
        df=self.OldFeaturesRank(df,['sm_amount','lg_amount','net_mf_amount'],2)


        return df

    def create_Moneyflowfeature3(self,arges):

        df=self.LoaddfMoneyflow().copy(deep=True)
        df3=self.LoaddfLongfactor().copy(deep=True)

        #df=pd.merge(df, df3, how='left', on=['ts_code','trade_date'])

        df3['total_mv_rank']=df3.groupby('trade_date')['total_mv'].rank(pct=True)
        #df['total_mv_rank']=df.groupby('ts_code')['total_mv_rank'].shift(1)
        df3['total_mv_rank']=df3['total_mv_rank']*19.9//1

        df3[["ts_code","trade_date","total_mv_rank"]]

        df=pd.merge(df, df3, how='left', on=['ts_code','trade_date'])

        df=df[df["total_mv_rank"]>15]

        df.drop(['buy_sm_vol','sell_sm_vol','buy_md_vol','sell_md_vol','buy_lg_vol','sell_lg_vol','buy_md_vol','sell_md_vol'],axis=1,inplace=True)
        df.drop(['buy_elg_vol','buy_elg_amount','sell_elg_vol','sell_elg_amount','net_mf_vol'],axis=1,inplace=True)
        df.drop(['buy_md_amount','sell_md_amount'],axis=1,inplace=True)

        df.drop(['total_mv_rank'],axis=1,inplace=True)

        if arges:

            lastday=df['trade_date'].max()
            df['ts_code']=df['ts_code'].apply(lambda x : x[:-3])
            copy_df=df[df['trade_date']==lastday]
            copy_df.loc[:,'trade_date']=arges
            df = df.append(copy_df, ignore_index=True)




        df['sm_amount']=df['buy_sm_amount']-df['sell_sm_amount']
        df['lg_amount']=df['buy_lg_amount']-df['sell_lg_amount']


        df.drop(['buy_sm_amount','sell_sm_amount'],axis=1,inplace=True)
        df.drop(['buy_lg_amount','sell_lg_amount'],axis=1,inplace=True)


        #df['sm_amount']=df.groupby('ts_code')['sm_amount'].shift(1)
        #df['lg_amount']=df.groupby('ts_code')['lg_amount'].shift(1)
        #df['net_mf_amount']=df.groupby('ts_code')['net_mf_amount'].shift(1)

        df=self.InputChgSumRank(df,5,'sm_amount')
        df=self.InputChgSumRank(df,5,'lg_amount')
        df=self.InputChgSumRank(df,5,'net_mf_amount')

        df=self.InputChgSumRank(df,12,'sm_amount')
        df=self.InputChgSumRank(df,12,'lg_amount')
        df=self.InputChgSumRank(df,12,'net_mf_amount')

        df=self.InputChgSumRank(df,25,'sm_amount')
        df=self.InputChgSumRank(df,25,'lg_amount')
        df=self.InputChgSumRank(df,25,'net_mf_amount')

        df=self.OldFeaturesRank(df,['sm_amount','lg_amount','net_mf_amount'],1)
        df=self.OldFeaturesRank(df,['sm_amount','lg_amount','net_mf_amount'],2)


        return df

    def create_Limitfeature(self,arges):

        df=self.LoaddfLimit().copy(deep=True)

        if arges:

            lastday=df['trade_date'].max()
            df['ts_code']=df['ts_code'].apply(lambda x : x[:-3])
            copy_df=df[df['trade_date']==lastday]
            copy_df.loc[:,'trade_date']=arges
            df = df.append(copy_df, ignore_index=True)


        df['limit_percent']=df['down_limit']/df['up_limit']

        #是否st或其他
        df['st_or_otherwrong']=0
        df.loc[(df['limit_percent']<0.85) & (0.58<df['limit_percent']),'st_or_otherwrong']=1

        df.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        return df

    def create_Limitfeaturewithdate(self,arges):

        CSZLUtils.CSZLUtils.showmore()
        df=self.LoaddfLimit().copy(deep=True)

        if arges:

            lastday=df['trade_date'].max()
            df['ts_code']=df['ts_code'].apply(lambda x : x[:-3])
            copy_df=df[df['trade_date']==lastday]
            copy_df.loc[:,'trade_date']=arges
            df = df.append(copy_df, ignore_index=True)

        df['date']=df['trade_date'].astype(str)
        df['date'] = df['date'].apply(lambda x: datetime.strptime(x, '%Y%m%d'))
        df['month'] = df['date'].dt.month
        df['day'] = df['date'].dt.day
        df['weekday'] = df['date'].dt.dayofweek + 1

        weekday_mapping = {0: 6, 1: 7, 2: 1, 3: 2, 4: 3, 5: 4, 6: 5}
        df['weekday'] = df['weekday'].replace(weekday_mapping)
        # 将"month"、"day"和"weekday"三列的数据类型转换为整数
        df[['month', 'day', 'weekday']] = df[['month', 'day', 'weekday']].astype(int)

        df.drop('date',axis=1,inplace=True)

        print(df)

        df['limit_percent']=df['down_limit']/df['up_limit']

        #是否st或其他
        df['st_or_otherwrong']=0
        df.loc[(df['limit_percent']<0.85) & (0.58<df['limit_percent']),'st_or_otherwrong']=1

        df.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        return df

    def create_Limitfeatureandindustry(self,arges):

        df=self.LoaddfLimit().copy(deep=True)

        if arges:

            lastday=df['trade_date'].max()
            df['ts_code']=df['ts_code'].apply(lambda x : x[:-3])
            copy_df=df[df['trade_date']==lastday]
            copy_df.loc[:,'trade_date']=arges
            df = df.append(copy_df, ignore_index=True)


        basic_df=pd.read_csv('industryid_tscode.csv')


        df=pd.merge(df, basic_df, how='left', on=['ts_code'])

        df['industry_id'].fillna(0, inplace=True)

        #print(df)

        df['limit_percent']=df['down_limit']/df['up_limit']

        #是否st或其他
        df['st_or_otherwrong']=0
        df.loc[(df['limit_percent']<0.85) & (0.58<df['limit_percent']),'st_or_otherwrong']=1

        df.drop(['up_limit','down_limit','limit_percent'],axis=1,inplace=True)

        return df

    def create_Longfeature(self,arges):

        df=self.LoaddfLongfactor().copy(deep=True)

        if arges:

            lastday=df['trade_date'].max()
            df['ts_code']=df['ts_code'].apply(lambda x : x[:-3])
            copy_df=df[df['trade_date']==lastday]
            copy_df.loc[:,'trade_date']=arges
            df = df.append(copy_df, ignore_index=True)

        df.drop(['turnover_rate','volume_ratio','pe','dv_ttm'],axis=1,inplace=True)

        df['total_mv_rank']=df.groupby('trade_date')['total_mv'].rank(pct=True)
        #df['total_mv_rank']=df.groupby('ts_code')['total_mv_rank'].shift(1)
        df['total_mv_rank']=df['total_mv_rank']*19.9//1

        df['pb_rank']=df.groupby('trade_date')['pb'].rank(pct=True)
        #df['pb_rank']=df.groupby('ts_code')['pb_rank'].shift(1)
        #df['pb_rank']=df['pb_rank']*10//1

        df['circ_mv_pct']=(df['total_mv']-df['circ_mv'])/df['total_mv']
        df['circ_mv_pct']=df.groupby('trade_date')['circ_mv_pct'].rank(pct=True)
        #df['circ_mv_pct']=df.groupby('ts_code')['circ_mv_pct'].shift(1)
        #df['circ_mv_pct']=df['circ_mv_pct']*10//1
        
        df['ps_ttm']=df.groupby('trade_date')['ps_ttm'].rank(pct=True)
        #df['ps_ttm']=df.groupby('ts_code')['ps_ttm'].shift(1)

        df.drop(['total_mv','pb','circ_mv'],axis=1,inplace=True)

        df=df.replace(np.nan, 0)
        
        return df

    def create_CBtarget(self,arges):

        df=self.LoaddfCB_daily().copy(deep=True)

        df['real_price']=df['close']
        df=df[["ts_code","trade_date","real_price"]]

        df=self.PedictDaysRank(df,5)

        df=df[["ts_code","trade_date","tomorrow_chg","tomorrow_chg_rank"]]

        print(df)

        return df

    def create_CBdayfeature3(self,arges):

        df=self.LoaddfCB_daily().copy(deep=True)

        if arges:

            loadpath="real_buffer_CB.csv"
            df_today=pd.read_csv(loadpath,index_col=0,header=0)
            df_today['trade_date']=arges

            df['ts_code']=df['ts_code'].apply(lambda x : x[:-3])
            df_today['ts_code']=df_today['ts_code'].apply(lambda x:str(x).zfill(6))

            df = df.append(df_today, ignore_index=True)

            #lastday=df2['trade_date'].max()
            #df2['ts_code']=df2['ts_code'].apply(lambda x : x[:-3])
            #copy_df=df2[df2['trade_date']==lastday]
            #copy_df.loc[:,'trade_date']=arges
            
            #df2 = df2.append(copy_df, ignore_index=True)

        df['real_price']=df['close']

        df=df[["ts_code","trade_date","real_price","open","high","low","pct_chg","pre_close","close","amount"]]

        df['chg_rank']=df.groupby('trade_date')['pct_chg'].rank(pct=True)
        df['pct_chg_abs']=df['pct_chg'].abs()
        df['pct_chg_abs_rank']=df.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df[curc]-df['pre_close'])*100)/df['pre_close']
            df[curc]=buffer
            df[curc]=df.groupby('trade_date')[curc].rank(pct=True)


        df['pct_chg_r']=df['pct_chg']
        df=self.OldFeaturesRank(df,['open','high','low','pct_chg_r'],1)

        df=self.InputChgSumRank(df,6,'pct_chg_abs')
        df=self.InputChgSumRank(df,3,'pct_chg')
        df=self.InputChgSumRank(df,6,'pct_chg')
        df=self.InputChgSumRank(df,12,'pct_chg')
        df=self.InputChgSumRank(df,24,'pct_chg')

        df=self.InputChgSum(df,3,'pct_chg')
        df=self.InputChgSum(df,6,'pct_chg')
        df=self.InputChgSum(df,12,'pct_chg')
        df=self.InputChgSum(df,24,'pct_chg')

        print(df)
        #df.to_csv("seeeeee.csv")


        return df

    def create_CBdayfeature4(self,arges):

        df=self.LoaddfCB_daily().copy(deep=True)

        if arges:

            loadpath="real_buffer_CB.csv"
            df_today=pd.read_csv(loadpath,index_col=0,header=0)
            df_today['trade_date']=arges

            df['ts_code']=df['ts_code'].apply(lambda x : x[:-3])
            df_today['ts_code']=df_today['ts_code'].apply(lambda x:str(x).zfill(6))

            df_today['amount']=df_today['amount']/10

            df = df.append(df_today, ignore_index=True)

            #lastday=df2['trade_date'].max()
            #df2['ts_code']=df2['ts_code'].apply(lambda x : x[:-3])
            #copy_df=df2[df2['trade_date']==lastday]
            #copy_df.loc[:,'trade_date']=arges
            
            #df2 = df2.append(copy_df, ignore_index=True)

        df['real_price']=df['close']

        df=df[["ts_code","trade_date","real_price","open","high","low","pct_chg","pre_close","close","amount"]]

        df['chg_rank']=df.groupby('trade_date')['pct_chg'].rank(pct=True)
        df['pct_chg_abs']=df['pct_chg'].abs()
        df['pct_chg_abs_rank']=df.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df[curc]-df['pre_close'])*100)/df['pre_close']
            df[curc]=buffer
            df[curc]=df.groupby('trade_date')[curc].rank(pct=True)


        df['pct_chg_r']=df['pct_chg']
        df=self.OldFeaturesRank(df,['open','high','low','pct_chg_r'],1)
        df=self.OldFeaturesRank(df,['open','high','low','pct_chg_r'],2)
        df=self.OldFeaturesRank(df,['open','high','low','pct_chg_r'],3)

        df=self.InputChgSumRank(df,3,'pct_chg_abs')
        df=self.InputChgSumRank(df,6,'pct_chg_abs')
        df=self.InputChgSumRank(df,12,'pct_chg_abs')
        df=self.InputChgSumRank(df,3,'pct_chg')
        df=self.InputChgSumRank(df,6,'pct_chg')
        df=self.InputChgSumRank(df,12,'pct_chg')
        df=self.InputChgSumRank(df,24,'pct_chg')

        df=self.InputChgSum(df,3,'pct_chg')
        df=self.InputChgSum(df,6,'pct_chg')
        df=self.InputChgSum(df,12,'pct_chg')
        df=self.InputChgSum(df,24,'pct_chg')

        print(df)
        #df.to_csv("seeeeee.csv")


        return df

    def create_CBdayfeature5(self,arges):

        df=self.LoaddfCB_daily().copy(deep=True)

        if arges:

            loadpath="real_buffer_CB.csv"
            df_today=pd.read_csv(loadpath,index_col=0,header=0)
            df_today['trade_date']=arges

            df['ts_code']=df['ts_code'].apply(lambda x : x[:-3])
            df_today['ts_code']=df_today['ts_code'].apply(lambda x:str(x).zfill(6))

            df_today['amount']=df_today['amount']/10

            df = df.append(df_today, ignore_index=True)

            #lastday=df2['trade_date'].max()
            #df2['ts_code']=df2['ts_code'].apply(lambda x : x[:-3])
            #copy_df=df2[df2['trade_date']==lastday]
            #copy_df.loc[:,'trade_date']=arges
            
            #df2 = df2.append(copy_df, ignore_index=True)

        df['real_price']=df['close']

        df=df[["ts_code","trade_date","real_price","open","high","low","pct_chg","pre_close","close","amount"]]

        df=self.InputChgSum(df,3,'amount')
        df=self.InputChgSum(df,6,'amount')
        df=self.InputChgSum(df,12,'amount')
        df=self.InputChgSum(df,24,'amount')

        df['am_pct_1_3']=df['amount_3']/df['amount']
        df['am_pct_3_6']=df['amount_6']/df['amount_3']
        df['am_pct_6_12']=df['amount_12']/df['amount_6']
        df['am_pct_12_24']=df['amount_24']/df['amount_12']

        df['am_rank_1_3']=df.groupby('trade_date')['am_pct_1_3'].rank(pct=True)
        df['am_rank_3_6']=df.groupby('trade_date')['am_pct_3_6'].rank(pct=True)
        df['am_rank_6_12']=df.groupby('trade_date')['am_pct_6_12'].rank(pct=True)
        df['am_rank_12_24']=df.groupby('trade_date')['am_pct_12_24'].rank(pct=True)


        df['chg_rank']=df.groupby('trade_date')['pct_chg'].rank(pct=True)
        df['pct_chg_abs']=df['pct_chg'].abs()
        df['pct_chg_abs_rank']=df.groupby('trade_date')['pct_chg_abs'].rank(pct=True)

        #计算三种比例rank
        dolist=['open','high','low']

        for curc in dolist:
            buffer=((df[curc]-df['pre_close'])*100)/df['pre_close']
            df[curc]=buffer
            df[curc]=df.groupby('trade_date')[curc].rank(pct=True)


        df['pct_chg_r']=df['pct_chg']
        df=self.OldFeaturesRank(df,['open','high','low','pct_chg_r'],1)
        df=self.OldFeaturesRank(df,['open','high','low','pct_chg_r'],2)
        df=self.OldFeaturesRank(df,['open','high','low','pct_chg_r'],3)

        df=self.InputChgSumRank(df,3,'pct_chg_abs')
        df=self.InputChgSumRank(df,6,'pct_chg_abs')
        df=self.InputChgSumRank(df,12,'pct_chg_abs')
        df=self.InputChgSumRank(df,3,'pct_chg')
        df=self.InputChgSumRank(df,6,'pct_chg')
        df=self.InputChgSumRank(df,12,'pct_chg')
        df=self.InputChgSumRank(df,24,'pct_chg')

        df=self.InputChgSum(df,3,'pct_chg')
        df=self.InputChgSum(df,6,'pct_chg')
        df=self.InputChgSum(df,12,'pct_chg')
        df=self.InputChgSum(df,24,'pct_chg')

        print(df)
        #df.to_csv("seeeeee.csv")


        return df

    #创建shift特征的方法用于无法获取当日特征的情况
    def create_Shiftfeatures(self,arges):

        shift=arges[0]
        dfloadpath=arges[1]

        #savepath=self.Default_folder_path+"Shift"+shiftname+self.start_date+"to"+self.end_date+"shift"+str(shift)+".pkl"
        #if(os.path.exists(savepath)==True):
        #    print("数据集已创建")
        #    return savepath
        df=[]
        if shift<0:
            shift=-shift
            df=dfloadpath
        else:
            df=CSZLUtils.CSZLUtils.Loaddata(dfloadpath)
        #df=pd.read_pickle(dfloadpath)
        ##df=pd.read_csv(dfloadpath,index_col=0,header=0)

        df.sort_values(["trade_date","ts_code"] , inplace=True, ascending=True) 
        df.reset_index(inplace=True,drop=True)

        colnames=df.columns.values.tolist()
        #删除前两个字段
        colnames.pop(0)
        colnames.pop(0)
        #print(colnames)

        for curfeature in colnames:
            curstring='Shift_'+str(shift)+curfeature
            df[curstring]=df.groupby('ts_code')[curfeature].shift(shift)
            df.drop([curfeature],axis=1,inplace=True)


        df=df.replace(np.nan, 0)
        
        return df

    def create_Groupfeatures(self,arges):

        features=arges[2]
        dayfeaturepath=features[0]
        longfeaturepath=features[1]


        dfday=CSZLUtils.CSZLUtils.Loaddata(dayfeaturepath)
        dflong=CSZLUtils.CSZLUtils.Loaddata(longfeaturepath)

        CSZLUtils.CSZLUtils.showmore()

        dfday=dfday[['ts_code','trade_date','amount','pct_chg','close']]
        dfday['amount_group_rank']=dfday.groupby('trade_date')['amount'].rank(pct=True)
        dfday['amount_group_rank']=dfday['amount_group_rank']*4.99//1

        dfday['close_group_rank']=dfday.groupby('trade_date')['close'].rank(pct=True)
        dfday['close_group_rank']=dfday['close_group_rank']*4.99//1


        dflong=dflong[['ts_code','trade_date','pb_rank']]
        dflong['pb_group_rank']=dflong['pb_rank']*4.99//1

        df_all=pd.merge(dfday,dflong,on=['ts_code','trade_date'])

        df_all['selfrank']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank'])['pct_chg'].rank(pct=True)

        df1=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank']).agg({"pct_chg":"mean"})

        df1.rename(columns={'pct_chg':'group_pct_chg'},inplace=True)
        df1.reset_index(inplace=True)

        df_result=pd.merge(df_all,df1,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank'],how='left')


        df_result['GroupRank']=df_result['group_pct_chg'].rank(method='dense',pct=True)

        #df=pd.read_pickle(dfloadpath)
        ##df=pd.read_csv(dfloadpath,index_col=0,header=0)

        df_result=df_result[['ts_code','trade_date','selfrank','group_pct_chg','GroupRank']]

        df_result.sort_values(["trade_date","ts_code"] , inplace=True, ascending=True) 
        df_result.reset_index(inplace=True,drop=True)
        df_result.to_csv("zzzzzz.csv")
        
        print(df_result)

        return df_result

    def create_Groupfeatures2(self,arges):

        features=arges[2]
        dayfeaturepath=features[0]
        longfeaturepath=features[1]


        dfday=CSZLUtils.CSZLUtils.Loaddata(dayfeaturepath)
        dflong=CSZLUtils.CSZLUtils.Loaddata(longfeaturepath)

        CSZLUtils.CSZLUtils.showmore()

        dfday=dfday[['ts_code','trade_date','amount','pct_chg','close']]
        dfday['amount_group_rank']=dfday.groupby('trade_date')['amount'].rank(pct=True)
        dfday['amount_group_rank']=dfday['amount_group_rank']*2.99//1

        dfday['close_group_rank']=dfday.groupby('trade_date')['close'].rank(pct=True)
        dfday['close_group_rank']=dfday['close_group_rank']*2.99//1


        dflong=dflong[['ts_code','trade_date','pb_rank']]
        dflong['pb_group_rank']=dflong['pb_rank']*2.99//1

        df_all=pd.merge(dfday,dflong,on=['ts_code','trade_date'])

        df_all['selfrank']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank'])['pct_chg'].rank(pct=True)

        df1=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank']).agg({"pct_chg":"mean"})

        df1.rename(columns={'pct_chg':'group_pct_chg'},inplace=True)
        df1.reset_index(inplace=True)

        df_result=pd.merge(df_all,df1,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank'],how='left')


        df_result['GroupRank']=df_result['group_pct_chg'].rank(method='dense',pct=True)

        #df=pd.read_pickle(dfloadpath)
        ##df=pd.read_csv(dfloadpath,index_col=0,header=0)

        df_result=df_result[['ts_code','trade_date','selfrank','group_pct_chg','GroupRank']]

        df_result.sort_values(["trade_date","ts_code"] , inplace=True, ascending=True) 
        df_result.reset_index(inplace=True,drop=True)
        df_result.to_csv("zzzzzz.csv")
        
        print(df_result)

        return df_result

    def create_Groupfeatures3(self,arges):

        features=arges[2]
        dayfeaturepath=features[0]
        longfeaturepath=features[1]


        dfday=CSZLUtils.CSZLUtils.Loaddata(dayfeaturepath)
        dflong=CSZLUtils.CSZLUtils.Loaddata(longfeaturepath)

        CSZLUtils.CSZLUtils.showmore()

        print(dfday)
        dfday=dfday[['ts_code','trade_date','amount','pct_chg','pct_chg_3','pct_chg_6','close']]



        dfday['amount_group_rank']=dfday.groupby('trade_date')['amount'].rank(pct=True)
        dfday['amount_group_rank']=dfday['amount_group_rank']*2.99//1

        dfday['close_group_rank']=dfday.groupby('trade_date')['close'].rank(pct=True)
        dfday['close_group_rank']=dfday['close_group_rank']*2.99//1


        dflong=dflong[['ts_code','trade_date','pb_rank']]
        dflong['pb_group_rank']=dflong['pb_rank']*2.99//1

        df_all=pd.merge(dfday,dflong,on=['ts_code','trade_date'])

        df_all['selfrank']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank'])['pct_chg'].rank(pct=True)
        df_all['selfrank_Rank_3']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank'])['pct_chg_3'].rank(pct=True)
        df_all['selfrank_Rank_6']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank'])['pct_chg_6'].rank(pct=True)

        df1=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank']).agg({"pct_chg":"mean"})
        df2=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank']).agg({"pct_chg_3":"mean"})
        df3=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank']).agg({"pct_chg_6":"mean"})

        df1.rename(columns={'pct_chg':'group_pct_chg'},inplace=True)
        df2.rename(columns={'pct_chg_3':'group_pct_chg_Rank_3'},inplace=True)
        df3.rename(columns={'pct_chg_6':'group_pct_chg_Rank_6'},inplace=True)
        df1.reset_index(inplace=True)
        df2.reset_index(inplace=True)
        df3.reset_index(inplace=True)

        print(df1)

        df_result=pd.merge(df_all,df1,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank'],how='left')
        df_result=pd.merge(df_result,df2,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank'],how='left')
        df_result=pd.merge(df_result,df3,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank'],how='left')

        df_result['GroupRank']=df_result['group_pct_chg'].rank(method='dense',pct=True)
        df_result['GroupRank_3']=df_result['group_pct_chg_Rank_3'].rank(method='dense',pct=True)
        df_result['GroupRank_6']=df_result['group_pct_chg_Rank_6'].rank(method='dense',pct=True)

        #df=pd.read_pickle(dfloadpath)
        ##df=pd.read_csv(dfloadpath,index_col=0,header=0)

        df_result=df_result[['ts_code','trade_date','selfrank','selfrank_Rank_3','selfrank_Rank_6','group_pct_chg','group_pct_chg_Rank_3','group_pct_chg_Rank_6','GroupRank','GroupRank_3','GroupRank_6']]

        df_result.sort_values(["trade_date","ts_code"] , inplace=True, ascending=True) 
        df_result.reset_index(inplace=True,drop=True)
        df_result.to_csv("zzzzzz.csv")
        
        print(df_result)

        return df_result

    def create_Groupfeatures5(self,arges):

        flag=arges[0]
        dfday=[]
        dflong=[]
        if flag<0:
            dfday=arges[1]
            dflong=arges[2]

        else:
            features=arges[2]
            dayfeaturepath=features[0]
            longfeaturepath=features[1]

            dfday=CSZLUtils.CSZLUtils.Loaddata(dayfeaturepath)
            dflong=CSZLUtils.CSZLUtils.Loaddata(longfeaturepath)

        CSZLUtils.CSZLUtils.showmore()

        print(dfday)
        dfday=dfday[['ts_code','trade_date','amount','pct_chg','pct_chg_3','pct_chg_6','close']]

        dfday['code_group']=0

        print(dfday)

        dfday.loc[dfday['ts_code'].str.startswith('3')==True,'code_group']=3
        dfday.loc[dfday['ts_code'].str.startswith('0')==True,'code_group']=0
        dfday.loc[dfday['ts_code'].str.startswith('6')==True,'code_group']=6
        dfday.loc[dfday['ts_code'].str.startswith('68')==True,'code_group']=8

        print(dfday)

        dfday['amount_group_rank']=dfday.groupby('trade_date')['amount'].rank(pct=True)
        dfday['amount_group_rank']=dfday['amount_group_rank']*2.99//1

        dfday['close_group_rank']=dfday.groupby('trade_date')['close'].rank(pct=True)
        dfday['close_group_rank']=dfday['close_group_rank']*2.99//1


        dflong=dflong[['ts_code','trade_date','pb_rank']]
        dflong['pb_group_rank']=dflong['pb_rank']*2.99//1

        df_all=pd.merge(dfday,dflong,on=['ts_code','trade_date'])

        df_all['selfrank']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg'].rank(pct=True)
        df_all['selfrank_Rank_3']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg_3'].rank(pct=True)
        df_all['selfrank_Rank_6']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg_6'].rank(pct=True)

        df1=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg":"mean"})
        df2=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg_3":"mean"})
        df3=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg_6":"mean"})

        df1.rename(columns={'pct_chg':'group_pct_chg'},inplace=True)
        df2.rename(columns={'pct_chg_3':'group_pct_chg_Rank_3'},inplace=True)
        df3.rename(columns={'pct_chg_6':'group_pct_chg_Rank_6'},inplace=True)
        df1.reset_index(inplace=True)
        df2.reset_index(inplace=True)
        df3.reset_index(inplace=True)

        print(df1)

        df_result=pd.merge(df_all,df1,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')
        df_result=pd.merge(df_result,df2,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')
        df_result=pd.merge(df_result,df3,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')

        df_result['GroupRank']=df_result['group_pct_chg'].rank(method='dense',pct=True)
        df_result['GroupRank_3']=df_result['group_pct_chg_Rank_3'].rank(method='dense',pct=True)
        df_result['GroupRank_6']=df_result['group_pct_chg_Rank_6'].rank(method='dense',pct=True)

        #df=pd.read_pickle(dfloadpath)
        ##df=pd.read_csv(dfloadpath,index_col=0,header=0)

        df_result=df_result[['ts_code','trade_date','selfrank','selfrank_Rank_3','selfrank_Rank_6','group_pct_chg','group_pct_chg_Rank_3','group_pct_chg_Rank_6','GroupRank','GroupRank_3','GroupRank_6','code_group']]

        df_result.sort_values(["trade_date","ts_code"] , inplace=True, ascending=True) 
        df_result.reset_index(inplace=True,drop=True)
        df_result.to_csv("zzzzzz.csv")
        
        print(df_result)

        return df_result

    def create_Groupfeatures5b(self,arges):

        flag=arges[0]
        dfday=[]
        dflong=[]
        if flag<0:
            dfday=arges[1]
            dflong=arges[2]

        else:
            features=arges[2]
            dayfeaturepath=features[0]
            longfeaturepath=features[1]

            dfday=CSZLUtils.CSZLUtils.Loaddata(dayfeaturepath)
            dflong=CSZLUtils.CSZLUtils.Loaddata(longfeaturepath)

        CSZLUtils.CSZLUtils.showmore()

        print(dfday)
        dfday=dfday[['ts_code','trade_date','amount','pct_chg','pct_chg_3','pct_chg_6','close']]

        dfday['code_group']=0

        print(dfday)

        dfday.loc[dfday['ts_code'].str.startswith('3')==True,'code_group']=3
        dfday.loc[dfday['ts_code'].str.startswith('0')==True,'code_group']=0
        dfday.loc[dfday['ts_code'].str.startswith('6')==True,'code_group']=6
        dfday.loc[dfday['ts_code'].str.startswith('68')==True,'code_group']=8

        print(dfday)

        dfday['amount_group_rank']=dfday.groupby('trade_date')['amount'].rank(pct=True)
        dfday['amount_group_rank']=dfday['amount_group_rank']*2.99//1

        dfday['close_group_rank']=dfday.groupby('trade_date')['close'].rank(pct=True)
        dfday['close_group_rank']=dfday['close_group_rank']*2.99//1


        dflong=dflong[['ts_code','trade_date','pb_rank']]
        dflong['pb_group_rank']=dflong['pb_rank']*2.99//1

        df_all=pd.merge(dfday,dflong,on=['ts_code','trade_date'])

        df_all['selfrank']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg'].rank(pct=True)
        df_all['selfrank_Rank_3']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg_3'].rank(pct=True)
        df_all['selfrank_Rank_6']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg_6'].rank(pct=True)

        df1=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg":"mean"})
        df2=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg_3":"mean"})
        df3=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg_6":"mean"})

        df1.rename(columns={'pct_chg':'group_pct_chg'},inplace=True)
        df2.rename(columns={'pct_chg_3':'group_pct_chg_Rank_3'},inplace=True)
        df3.rename(columns={'pct_chg_6':'group_pct_chg_Rank_6'},inplace=True)
        df1.reset_index(inplace=True)
        df2.reset_index(inplace=True)
        df3.reset_index(inplace=True)

        print(df1)

        df_result=pd.merge(df_all,df1,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')
        df_result=pd.merge(df_result,df2,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')
        df_result=pd.merge(df_result,df3,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')

        df_result['GroupRank']=df_result['group_pct_chg'].rank(method='dense',pct=True)
        df_result['GroupRank_3']=df_result['group_pct_chg_Rank_3'].rank(method='dense',pct=True)
        df_result['GroupRank_6']=df_result['group_pct_chg_Rank_6'].rank(method='dense',pct=True)

        #df=pd.read_pickle(dfloadpath)
        ##df=pd.read_csv(dfloadpath,index_col=0,header=0)

        df_result=df_result[['ts_code','trade_date','selfrank','selfrank_Rank_3','selfrank_Rank_6','group_pct_chg','group_pct_chg_Rank_3','group_pct_chg_Rank_6','GroupRank','GroupRank_3','GroupRank_6','code_group']]

        df_result.sort_values(["trade_date","ts_code"] , inplace=True, ascending=True) 
        df_result.reset_index(inplace=True,drop=True)
        df_result.to_csv("zzzzzz.csv")
        
        print(df_result)

        return df_result

    def create_Groupfeatures6(self,arges):

        flag=arges[0]
        dfday=[]
        dflong=[]
        if flag<0:
            dfday=arges[1]
            dflong=arges[2]

        else:
            features=arges[2]
            dayfeaturepath=features[0]
            longfeaturepath=features[1]

            dfday=CSZLUtils.CSZLUtils.Loaddata(dayfeaturepath)
            dflong=CSZLUtils.CSZLUtils.Loaddata(longfeaturepath)

        CSZLUtils.CSZLUtils.showmore()

        print(dfday)
        dfday=dfday[['ts_code','trade_date','amount','pct_chg','pct_chg_3','pct_chg_6','pct_chg_12','pct_chg_24','close']]

        dfday['code_group']=0

        print(dfday)

        dfday.loc[dfday['ts_code'].str.startswith('3')==True,'code_group']=3
        dfday.loc[dfday['ts_code'].str.startswith('0')==True,'code_group']=0
        dfday.loc[dfday['ts_code'].str.startswith('6')==True,'code_group']=6
        dfday.loc[dfday['ts_code'].str.startswith('68')==True,'code_group']=8

        print(dfday)

        dfday['amount_group_rank']=dfday.groupby('trade_date')['amount'].rank(pct=True)
        dfday['amount_group_rank']=dfday['amount_group_rank']*2.99//1

        dfday['close_group_rank']=dfday.groupby('trade_date')['close'].rank(pct=True)
        dfday['close_group_rank']=dfday['close_group_rank']*2.99//1


        dflong=dflong[['ts_code','trade_date','pb_rank']]
        dflong['pb_group_rank']=dflong['pb_rank']*2.99//1

        df_all=pd.merge(dfday,dflong,on=['ts_code','trade_date'])
        #这个是判断各自组里面最强的。。
        df_all['selfrank']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg'].rank(pct=True)
        df_all['selfrank_Rank_3']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg_3'].rank(pct=True)
        df_all['selfrank_Rank_6']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg_6'].rank(pct=True)
        df_all['selfrank_Rank_12']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg_12'].rank(pct=True)
        df_all['selfrank_Rank_24']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg_24'].rank(pct=True)

        df1=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg":"mean"})
        df2=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg_3":"mean"})
        df3=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg_6":"mean"})
        df4=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg_12":"mean"})
        df5=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg_24":"mean"})

        df1.rename(columns={'pct_chg':'group_pct_chg'},inplace=True)
        df2.rename(columns={'pct_chg_3':'group_pct_chg_Rank_3'},inplace=True)
        df3.rename(columns={'pct_chg_6':'group_pct_chg_Rank_6'},inplace=True)
        df4.rename(columns={'pct_chg_12':'group_pct_chg_Rank_12'},inplace=True)
        df5.rename(columns={'pct_chg_24':'group_pct_chg_Rank_24'},inplace=True)
        df1.reset_index(inplace=True)
        df2.reset_index(inplace=True)
        df3.reset_index(inplace=True)
        df4.reset_index(inplace=True)
        df5.reset_index(inplace=True)

        print(df1)

        df_result=pd.merge(df_all,df1,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')
        df_result=pd.merge(df_result,df2,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')
        df_result=pd.merge(df_result,df3,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')
        df_result=pd.merge(df_result,df4,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')
        df_result=pd.merge(df_result,df5,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')

        df_result['GroupRank']=df_result['group_pct_chg'].rank(method='dense',pct=True)
        df_result['GroupRank_3']=df_result['group_pct_chg_Rank_3'].rank(method='dense',pct=True)
        df_result['GroupRank_6']=df_result['group_pct_chg_Rank_6'].rank(method='dense',pct=True)
        df_result['GroupRank_12']=df_result['group_pct_chg_Rank_12'].rank(method='dense',pct=True)
        df_result['GroupRank_24']=df_result['group_pct_chg_Rank_24'].rank(method='dense',pct=True)

        #df=pd.read_pickle(dfloadpath)
        ##df=pd.read_csv(dfloadpath,index_col=0,header=0)

        df_result=df_result[['ts_code','trade_date','selfrank','selfrank_Rank_3','selfrank_Rank_6','selfrank_Rank_12','selfrank_Rank_24','group_pct_chg',
                             'group_pct_chg_Rank_3','group_pct_chg_Rank_6','group_pct_chg_Rank_12','group_pct_chg_Rank_24','GroupRank','GroupRank_3',
                             'GroupRank_6','GroupRank_12','GroupRank_24','code_group']]

        df_result.sort_values(["trade_date","ts_code"] , inplace=True, ascending=True) 
        df_result.reset_index(inplace=True,drop=True)
        df_result.to_csv("zzzzzz.csv")
        
        print(df_result)

        return df_result

    def create_Groupfeatures10(self,arges):

        flag=arges[0]
        dfday=[]
        dflong=[]
        if flag<0:
            dfday=arges[1]
            dflong=arges[2]

        else:
            features=arges[2]
            dayfeaturepath=features[0]
            longfeaturepath=features[1]

            dfday=CSZLUtils.CSZLUtils.Loaddata(dayfeaturepath)
            dflong=CSZLUtils.CSZLUtils.Loaddata(longfeaturepath)

        CSZLUtils.CSZLUtils.showmore()

        print(dfday)
        outputcsv=dfday.tail(1000)
        outputcsv.to_csv("se230819.csv")

        dfday=dfday[['ts_code','trade_date','amount','pct_chg','pct_chg_3','pct_chg_6','pct_chg_12','pct_chg_24','close']]

        dfday['code_group']=0

        print(dfday)

        dfday.loc[dfday['ts_code'].str.startswith('3')==True,'code_group']=3
        dfday.loc[dfday['ts_code'].str.startswith('0')==True,'code_group']=0
        dfday.loc[dfday['ts_code'].str.startswith('6')==True,'code_group']=6
        dfday.loc[dfday['ts_code'].str.startswith('68')==True,'code_group']=8

        print(dfday)

        dfday['amount_group_rank']=dfday.groupby('trade_date')['amount'].rank(pct=True)
        dfday['amount_group_rank']=dfday['amount_group_rank']*2.99//1

        dfday['close_group_rank']=dfday.groupby('trade_date')['close'].rank(pct=True)
        dfday['close_group_rank']=dfday['close_group_rank']*2.99//1


        dflong=dflong[['ts_code','trade_date','pb_rank']]
        dflong['pb_group_rank']=dflong['pb_rank']*2.99//1

        df_all=pd.merge(dfday,dflong,on=['ts_code','trade_date'])
        #这个是判断各自组里面最强的。。
        df_all['selfrank']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg'].rank(pct=True)
        df_all['selfrank_Rank_3']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg_3'].rank(pct=True)
        df_all['selfrank_Rank_6']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg_6'].rank(pct=True)
        df_all['selfrank_Rank_12']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg_12'].rank(pct=True)
        df_all['selfrank_Rank_24']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg_24'].rank(pct=True)

        df1=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg":"mean"})
        df2=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg_3":"mean"})
        df3=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg_6":"mean"})
        df4=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg_12":"mean"})
        df5=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg_24":"mean"})

        df1.rename(columns={'pct_chg':'group_pct_chg'},inplace=True)
        df2.rename(columns={'pct_chg_3':'group_pct_chg_Rank_3'},inplace=True)
        df3.rename(columns={'pct_chg_6':'group_pct_chg_Rank_6'},inplace=True)
        df4.rename(columns={'pct_chg_12':'group_pct_chg_Rank_12'},inplace=True)
        df5.rename(columns={'pct_chg_24':'group_pct_chg_Rank_24'},inplace=True)
        df1.reset_index(inplace=True)
        df2.reset_index(inplace=True)
        df3.reset_index(inplace=True)
        df4.reset_index(inplace=True)
        df5.reset_index(inplace=True)

        print(df1)

        df_result=pd.merge(df_all,df1,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')
        df_result=pd.merge(df_result,df2,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')
        df_result=pd.merge(df_result,df3,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')
        df_result=pd.merge(df_result,df4,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')
        df_result=pd.merge(df_result,df5,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')

        df_result['GroupRank']=df_result['group_pct_chg'].rank(method='dense',pct=True)
        df_result['GroupRank_3']=df_result['group_pct_chg_Rank_3'].rank(method='dense',pct=True)
        df_result['GroupRank_6']=df_result['group_pct_chg_Rank_6'].rank(method='dense',pct=True)
        df_result['GroupRank_12']=df_result['group_pct_chg_Rank_12'].rank(method='dense',pct=True)
        df_result['GroupRank_24']=df_result['group_pct_chg_Rank_24'].rank(method='dense',pct=True)

        #df=pd.read_pickle(dfloadpath)
        ##df=pd.read_csv(dfloadpath,index_col=0,header=0)

        df_result=df_result[['ts_code','trade_date','selfrank','selfrank_Rank_3','selfrank_Rank_6','selfrank_Rank_12','selfrank_Rank_24','group_pct_chg',
                             'group_pct_chg_Rank_3','group_pct_chg_Rank_6','group_pct_chg_Rank_12','group_pct_chg_Rank_24','GroupRank','GroupRank_3',
                             'GroupRank_6','GroupRank_12','GroupRank_24','code_group']]

        df_result.sort_values(["trade_date","ts_code"] , inplace=True, ascending=True) 
        df_result.reset_index(inplace=True,drop=True)
        df_result.to_csv("zzzzzz.csv")
        
        print(df_result)

        return df_result

    def create_Groupfeatures5save(self,arges):

        features=arges[2]
        dayfeaturepath=features[0]
        longfeaturepath=features[1]


        dfday=CSZLUtils.CSZLUtils.Loaddata(dayfeaturepath)
        dflong=CSZLUtils.CSZLUtils.Loaddata(longfeaturepath)

        CSZLUtils.CSZLUtils.showmore()

        print(dfday)
        dfday=dfday[['ts_code','trade_date','amount','pct_chg','pct_chg_3','pct_chg_6','close']]

        dfday['code_group']=0

        dfday.loc[dfday['ts_code'].str.startswith('3')==True,'code_group']=3
        dfday.loc[dfday['ts_code'].str.startswith('0')==True,'code_group']=0
        dfday.loc[dfday['ts_code'].str.startswith('6')==True,'code_group']=6
        dfday.loc[dfday['ts_code'].str.startswith('68')==True,'code_group']=8

        print(dfday)

        dfday['amount_group_rank']=dfday.groupby('trade_date')['amount'].rank(pct=True)
        dfday['amount_group_rank']=dfday['amount_group_rank']*2.99//1

        dfday['close_group_rank']=dfday.groupby('trade_date')['close'].rank(pct=True)
        dfday['close_group_rank']=dfday['close_group_rank']*2.99//1


        dflong=dflong[['ts_code','trade_date','pb_rank']]
        dflong['pb_group_rank']=dflong['pb_rank']*2.99//1

        df_all=pd.merge(dfday,dflong,on=['ts_code','trade_date'])

        df_all['selfrank']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg'].rank(pct=True)
        df_all['selfrank_Rank_3']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg_3'].rank(pct=True)
        df_all['selfrank_Rank_6']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'])['pct_chg_6'].rank(pct=True)

        df1=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg":"mean"})
        df2=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg_3":"mean"})
        df3=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group']).agg({"pct_chg_6":"mean"})

        df1.rename(columns={'pct_chg':'group_pct_chg'},inplace=True)
        df2.rename(columns={'pct_chg_3':'group_pct_chg_Rank_3'},inplace=True)
        df3.rename(columns={'pct_chg_6':'group_pct_chg_Rank_6'},inplace=True)
        df1.reset_index(inplace=True)
        df2.reset_index(inplace=True)
        df3.reset_index(inplace=True)

        print(df1)

        df_result=pd.merge(df_all,df1,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')
        df_result=pd.merge(df_result,df2,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')
        df_result=pd.merge(df_result,df3,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank','code_group'],how='left')

        df_result['GroupRank']=df_result['group_pct_chg'].rank(method='dense',pct=True)
        df_result['GroupRank_3']=df_result['group_pct_chg_Rank_3'].rank(method='dense',pct=True)
        df_result['GroupRank_6']=df_result['group_pct_chg_Rank_6'].rank(method='dense',pct=True)

        #df=pd.read_pickle(dfloadpath)
        ##df=pd.read_csv(dfloadpath,index_col=0,header=0)

        df_result=df_result[['ts_code','trade_date','selfrank','selfrank_Rank_3','selfrank_Rank_6','group_pct_chg','group_pct_chg_Rank_3','group_pct_chg_Rank_6','GroupRank','GroupRank_3','GroupRank_6','code_group']]

        df_result.sort_values(["trade_date","ts_code"] , inplace=True, ascending=True) 
        df_result.reset_index(inplace=True,drop=True)
        df_result.to_csv("zzzzzz.csv")
        
        print(df_result)

        return df_result

    def create_Groupfeatures4(self,arges):

        features=arges[2]
        dayfeaturepath=features[0]
        longfeaturepath=features[1]


        dfday=CSZLUtils.CSZLUtils.Loaddata(dayfeaturepath)
        dflong=CSZLUtils.CSZLUtils.Loaddata(longfeaturepath)

        CSZLUtils.CSZLUtils.showmore()

        print(dfday)
        dfday=dfday[['ts_code','trade_date','amount','pct_chg','pct_chg_3','pct_chg_6','close']]



        dfday['amount_group_rank']=dfday.groupby('trade_date')['amount'].rank(pct=True)
        dfday['amount_group_rank']=dfday['amount_group_rank']*2.99//1

        dfday['close_group_rank']=dfday.groupby('trade_date')['close'].rank(pct=True)
        dfday['close_group_rank']=dfday['close_group_rank']*2.99//1


        dflong=dflong[['ts_code','trade_date','pb_rank']]
        dflong['pb_group_rank']=dflong['pb_rank']*2.99//1

        df_all=pd.merge(dfday,dflong,on=['ts_code','trade_date'])

        df_all['selfrank']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank'])['pct_chg'].rank(pct=True)
        df_all['selfrank_Rank_3']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank'])['pct_chg_3'].rank(pct=True)
        df_all['selfrank_Rank_6']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank'])['pct_chg_6'].rank(pct=True)
        df_all['selfrank_Rank_6']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank'])['pct_chg_6'].rank(pct=True)
        df_all['selfrank_Rank_6']=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank'])['pct_chg_6'].rank(pct=True)


        df1=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank']).agg({"pct_chg":"mean"})
        df2=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank']).agg({"pct_chg_3":"mean"})
        df3=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank']).agg({"pct_chg_6":"mean"})
        df3=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank']).agg({"pct_chg_6":"mean"})
        df3=df_all.groupby(['trade_date','amount_group_rank','pb_group_rank','close_group_rank']).agg({"pct_chg_6":"mean"})

        df1.rename(columns={'pct_chg':'group_pct_chg'},inplace=True)
        df2.rename(columns={'pct_chg_3':'group_pct_chg_Rank_3'},inplace=True)
        df3.rename(columns={'pct_chg_6':'group_pct_chg_Rank_6'},inplace=True)
        df3.rename(columns={'pct_chg_6':'group_pct_chg_Rank_6'},inplace=True)
        df3.rename(columns={'pct_chg_6':'group_pct_chg_Rank_6'},inplace=True)

        df1.reset_index(inplace=True)
        df2.reset_index(inplace=True)
        df3.reset_index(inplace=True)
        df3.reset_index(inplace=True)
        df3.reset_index(inplace=True)

        print(df1)

        df_result=pd.merge(df_all,df1,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank'],how='left')
        df_result=pd.merge(df_result,df2,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank'],how='left')
        df_result=pd.merge(df_result,df3,on=['trade_date','amount_group_rank','pb_group_rank','close_group_rank'],how='left')

        df_result['GroupRank']=df_result['group_pct_chg'].rank(method='dense',pct=True)
        df_result['GroupRank_3']=df_result['group_pct_chg_Rank_3'].rank(method='dense',pct=True)
        df_result['GroupRank_6']=df_result['group_pct_chg_Rank_6'].rank(method='dense',pct=True)

        #df=pd.read_pickle(dfloadpath)
        ##df=pd.read_csv(dfloadpath,index_col=0,header=0)

        df_result=df_result[['ts_code','trade_date','selfrank','selfrank_Rank_3','selfrank_Rank_6','group_pct_chg','group_pct_chg_Rank_3','group_pct_chg_Rank_6','GroupRank','GroupRank_3','GroupRank_6']]

        df_result.sort_values(["trade_date","ts_code"] , inplace=True, ascending=True) 
        df_result.reset_index(inplace=True,drop=True)
        df_result.to_csv("zzzzzz.csv")
        
        print(df_result)

        return df_result

    ######创建特征使用的基本方法
    def create_trainingdatasets(self,funname,*args):

        #获取生成文件名
        bufferstr=str(funname)
        filename=re.findall(r"create_(.+?) ",bufferstr)[0]

        if(args):
            base_name=os.path.splitext(args[1])[0]
            #savepath=base_name+"_"+str(args[0])+".csv"
            savepath=base_name+"_"+str(args[0])+".pkl"
        else:
            savepath=self.Default_folder_path+filename+self.start_date+"to"+self.end_date+".pkl"
            #savepath=self.Default_folder_path+filename+self.start_date+"to"+self.end_date+".csv"
        if(os.path.exists(CSZLUtils.CSZLUtils.pathchange(savepath))==True):
            print("数据集已创建")
            return savepath

        
        df=funname(args)

        #print(df)
        CSZLUtils.CSZLUtils.Savedata(df,savepath)
        ##df.to_csv(savepath)
        #df.to_pickle(savepath)


        del df
        gc.collect()
        
        return savepath

    ######下载并读取生成source
    def LoaddfDailydata(self):
        if(self.dfDailydata.empty):
            loadpath=self.CSZLDataLoader.getDataSet(self.Default_folder_path)
            self.dfDailydata=CSZLUtils.CSZLUtils.Loaddata(loadpath)

        return self.dfDailydata

    def LoaddfAdj_factor(self):
        if(self.dfAdj_factor.empty):
            loadpath=self.CSZLDataLoader.getDataSet_adj_factor(self.Default_folder_path)
            self.dfAdj_factor=CSZLUtils.CSZLUtils.Loaddata(loadpath)

        return self.dfAdj_factor

    def LoaddfMoneyflow(self):
        if(self.dfMoneyflow.empty):
            loadpath=self.CSZLDataLoader.getDataSet_moneyflow(self.Default_folder_path)
            self.dfMoneyflow=CSZLUtils.CSZLUtils.Loaddata(loadpath)

        return self.dfMoneyflow

    def LoaddfLimit(self):
        if(self.dfLimit.empty):
            loadpath=self.CSZLDataLoader.getDataSet_stk_limit(self.Default_folder_path)
            self.dfLimit=CSZLUtils.CSZLUtils.Loaddata(loadpath)

        return self.dfLimit

    def LoaddfLongfactor(self):
        if(self.dfLongfactor.empty):
            loadpath=self.CSZLDataLoader.getDataSet_long_factor(self.Default_folder_path)
            self.dfLongfactor=CSZLUtils.CSZLUtils.Loaddata(loadpath)

        return self.dfLongfactor

    def LoaddfCB_daily(self):
        if(self.dfCBDaily.empty):
            loadpath=self.CSZLDataLoader.getDataSet_cbdaily(self.Default_folder_path)
            self.dfCBDaily=CSZLUtils.CSZLUtils.Loaddata(loadpath)

        return self.dfCBDaily

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

    def InputChgSum(self,df_all,days,sumlinename):

        bufferbak='_'+str(days)
        #stringdisplay=sumlinename+'_'+str(days)

        xxx=df_all.groupby('ts_code')[sumlinename].rolling(days).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        #TODO这里可能不用rank会好一点
        #df_all[stringdisplay]=df_all.groupby('trade_date')[stringdisplay].rank(pct=True)

        return df_all

    def InputChgSumRank(self,df_all,days,sumlinename):

        bufferbak='_Rank_'+str(days)
        stringdisplay=sumlinename+'_Rank_'+str(days)

        xxx=df_all.groupby('ts_code')[sumlinename].rolling(days).sum().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)

        df_all=df_all.join(xxx, lsuffix='', rsuffix=bufferbak)

        df_all[stringdisplay]=df_all.groupby('trade_date')[stringdisplay].rank(pct=True)

        return df_all

    def PedictDaysRank(self,df_all,days):

        nextstart=df_all.groupby('ts_code')['real_price'].shift(0)
        nextnstart=df_all.groupby('ts_code')['real_price'].shift(0-days)

        df_all['tomorrow_chg']=((nextnstart-nextstart)/nextstart)*100

        #明日排名
        df_all['tomorrow_chg_rank']=df_all.groupby('trade_date')['tomorrow_chg'].rank(pct=True)
        df_all['tomorrow_chg_rank']=df_all['tomorrow_chg_rank']*19.9//1

        return df_all

    def HighLowRange(self,df_all,days):
        #输入几日和最高或最低返回排名
        #30日最低比值

        stringdisplay=str(days)+'_pct_Rangerank'

        xxx=df_all.groupby('ts_code')['real_price'].rolling(days).min().reset_index()     
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)       

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30min')


        xxx=df_all.groupby('ts_code')['real_price'].rolling(days).max().reset_index()
        xxx.set_index(['level_1'], drop=True, append=False, inplace=True, verify_integrity=False)
        xxx.drop(['ts_code'],axis=1,inplace=True)       

        df_all=df_all.join(xxx, lsuffix='', rsuffix='_30max')
        #bbb=df_all.groupby('ts_code')['real_price'].agg({'all_min':np.min, 'all_max': np.max}).reset_index()
        #ccc=pd.merge(df_all, bbb, how='inner', on=['ts_code'])
        df_all['30_pct']=(100*(df_all['real_price_30max']+0.001-df_all['real_price_30min']))/df_all['real_price_30min']
        df_all[stringdisplay]=df_all.groupby('trade_date')['30_pct'].rank(pct=True)

        df_all.drop(['30_pct','real_price_30min','real_price_30max'],axis=1,inplace=True)

        return df_all

    def OldFeaturesRank(self,df_all,features,daybak):
      
        for curfeature in features:
            curstring='yesterday_'+str(daybak)+curfeature
            df_all[curstring]=df_all.groupby('ts_code')[curfeature].shift(daybak)

        return df_all