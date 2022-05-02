#coding=utf-8
import sys
import CSZLData
import CSZLWorkflow
import tushare as ts

if __name__ == '__main__':

    
    inputvalues=sys.argv
    if len(inputvalues)>1:
        print("脚本预设参数")
        action=inputvalues[1]
    else:
        print("系统接收参数")
        print("CSZL2022 功能列表：\n 1:BackTesting \n 2:RealTimePredict \n 3:BackTesting_snowball\n"
               " 4:BackTesting_haitong")
        action=input()


    zzzz=CSZLWorkflow.CSZLWorkflow()

    if action=='1':
        zzzz.BackTesting()

    elif action=='2':
        #"Today_result.csv"
        zzzz.RealTimePredict()
        zzzz.HaitongToCSZL()
        zzzz.Todays_action('last_result_real.csv',"Today_result.csv",5,7000)
    elif action=='3':
        zzzz.BackTesting_snowball_0501()
        zzzz.Todays_action('last_result_snowball.csv','Today_NEXT_predict.csv',2,200000)
    elif action=='4':
        zzzz.BackTesting_snowball_0501()
        zzzz.HaitongToCSZL()
        zzzz.Todays_action('last_result_real.csv','Today_NEXT_predict.csv',5,7000)

    elif action=='9':
        zzzz.PKL2CSV()

    #elif action=='5':
    #    zzzz.Todays_action('last_result_real.csv',"Today_result.csv",5,7000)
    #elif action=='6':
    #    zzzz.Todays_action('last_result_real.csv',"Today_result.csv",5,7000)
    else:
        pass
    pass

    print("complete! CSZL_2022")
    input()
    end=1



