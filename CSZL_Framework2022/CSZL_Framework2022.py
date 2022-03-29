#coding=utf-8

import CSZLData
import CSZLWorkflow
import tushare as ts

if __name__ == '__main__':

    #f = open('token.txt')
    #token = f.read()     #将txt文件的所有内容读入到字符串str中
    #f.close()

    #pro = ts.pro_api(token)
    #zzzz2=pro.query('trade_cal', start_date="20210219", end_date="20210319")
    #print(zzzz2)
    #Default_folder_path='./temp/'

    #zzzz=CSZLData.CSZLData("20220101","20220301")

    #zzzz.getDataSet_all(Default_folder_path)

    #zzzz.getDataSet_stk_limit(Default_folder_path)
    #zzzz.getDataSet_moneyflow(Default_folder_path)

    #zzzz.getDataSet_long_factor(Default_folder_path)
    #zzzz.getDataSet(Default_folder_path)
    #zzzz.getDataSet_adj_factor(Default_folder_path)

    zzzz=CSZLWorkflow.CSZLWorkflow()

    zzzz.BackTesting()
    

    print("complete CSZL_2022")
    input()
    end=1