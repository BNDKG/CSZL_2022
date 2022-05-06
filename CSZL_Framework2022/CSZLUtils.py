#coding=utf-8

import os
import shutil
import numpy as np
import pandas as pd

class CSZLUtils(object):
    """description of class"""


    def mkdir(path):
        """生成文件夹路径"""
 
        # 去除首位空格
        path=path.strip()
        # 去除尾部 \ 符号
        path=path.rstrip("\\")
 
        # 判断路径是否存在
        # 存在     True
        # 不存在   False
        isExists=os.path.exists(path)
 
        # 判断结果
        if not isExists:

            os.makedirs(path) 
 
            print (path+' 创建成功')
            return True
        else:
            # 如果目录存在则不创建，并提示目录已存在
            print (path+' 目录已存在')
            return False

    def copyfile(srcfile,dstfile):
        """复制文件"""

        if not os.path.isfile(srcfile):
            print ("%s not exist!"%(srcfile))
        else:
            fpath,fname=os.path.split(dstfile)    #分离文件名和路径
            if not os.path.exists(fpath):
                os.makedirs(fpath)                #创建路径
            shutil.copyfile(srcfile,dstfile)      #复制文件
            print ("copy %s -> %s"%( srcfile,dstfile))

    def reduce_mem_usage(df):
        """ iterate through all the columns of a dataframe and modify the data type
            to reduce memory usage.        
        """
        start_mem = df.memory_usage().sum() / 1024**2
        print('Memory usage of dataframe is {:.2f} MB'.format(start_mem))
    
        for col in df.columns:
            col_type = df[col].dtype
        
            if col_type != object:
                c_min = df[col].min()
                c_max = df[col].max()
                if str(col_type)[:3] == 'int':
                    if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                        df[col] = df[col].astype(np.int8)
                    elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                        df[col] = df[col].astype(np.int16)
                    elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                        df[col] = df[col].astype(np.int32)
                    elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                        df[col] = df[col].astype(np.int64)  
                else:
                    if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                        df[col] = df[col].astype(np.float16)
                    elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                        df[col] = df[col].astype(np.float32)
                    else:
                        df[col] = df[col].astype(np.float64)
            else:
                #这里可能会误将大数字定义为cat所以先只指定ts_code
                if(col=='ts_code'):
                    df[col] = df[col].astype('category')

        end_mem = df.memory_usage().sum() / 1024**2
        print('Memory usage after optimization is: {:.2f} MB'.format(end_mem))
        print('Decreased by {:.1f}%'.format(100 * (start_mem - end_mem) / start_mem))
    
        return df

    def csvmode():
        return False

    def Loaddata(path):


        if CSZLUtils.csvmode():
            changepath=CSZLUtils.pathchange(path)
            df=pd.read_csv(changepath,index_col=0,header=0)
        else:
            df=pd.read_pickle(path)

        return df

    def Savedata(df,path):

        if CSZLUtils.csvmode():
            changepath=CSZLUtils.pathchange(path)
            df.to_csv(changepath)
        else:
            df.to_pickle(path)
        
        return

    def pathchange(path):

        if CSZLUtils.csvmode():
            (filepath, tempfilename)=os.path.split(path)
            (filename, extension) = os.path.splitext(tempfilename)
            path=filepath+'/'+filename+'.csv'

        return path

    def copyfile(srcfile,dstfile):
        """复制文件"""

        if not os.path.isfile(srcfile):
            print ("%s not exist!"%(srcfile))
        else:
            fpath,fname=os.path.split(dstfile)    #分离文件名和路径
            if not os.path.exists(fpath):
                os.makedirs(fpath)                #创建路径
            shutil.copyfile(srcfile,dstfile)      #复制文件
            print ("copy %s -> %s"%( srcfile,dstfile))


    def getFlist(path):
        for root, dirs, files in os.walk(path):
            print('root_dir:', root)  #当前路径
            print('sub_dirs:', dirs)   #子文件夹
            print('files:', files)     #文件名称，返回list类型
        return files

    def fun01():
        df=pd.read_csv("qwer.csv",index_col=0,header=0)
        df=df[df['tomorrow_chg']!=0]
        grouped = df.groupby(['tomorrow_chg_rank']) # 对col1列进行分组

        zzzz2=grouped['tomorrow_chg'].mean() # 计算每个组col2列的均值

        print(zzzz2)