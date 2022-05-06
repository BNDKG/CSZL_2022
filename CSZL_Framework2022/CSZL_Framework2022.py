#coding=utf-8
import sys
import CSZLData
import CSZLWorkflow


if __name__ == '__main__':

    
    inputvalues=sys.argv
    if len(inputvalues)>1:
        print("脚本预设参数")
        action=inputvalues[1]
    else:
        print("系统接收参数")
        print("CSZL2022 功能列表：\n 1:BackTesting \n 2:RealTimePredict \n 3:BackTesting_snowball\n"
               " 4:BackTesting_haitong\n 5:ServerRun\n 6:Convertible bond BackTesting\n 7:RealTimePredict_CB\n"
               " 9:PKL2CSV")
        action=input()


    zzzz=CSZLWorkflow.CSZLWorkflow()

    if action=='1':
        zzzz.BackTesting()

    elif action=='2':
        #"Today_result.csv"
        zzzz.RealTimePredict()
        zzzz.Haitong2CSZL()
        zzzz.Todays_action('last_result_real.csv',"Today_result.csv",5,7000)
    elif action=='3':
        zzzz.BackTesting_static_0501()
        zzzz.Todays_action('last_result_snowball.csv','Today_NEXT_predict.csv',2,200000)
    elif action=='4':
        zzzz.BackTesting_static_0501()
        zzzz.Haitong2CSZL()
        zzzz.Todays_action('last_result_real.csv','Today_NEXT_predict.csv',5,7000)
    elif action=='5':
        zzzz.PredictBackRound()

    elif action=='6':

        zzzz.CBBackTesting()

        pass
    elif action=='7':

        #import numpy as np

        #a=[1233,34,67,4]
        #a=np.array(a)
        #np.save('a.npy',a) # 保存为.npy格式

        #a=np.load('a.npy')
        #a=a.tolist()

        #print(a)

        zzzz.RealTimePredict_CB()

        #CSZLData.CSZLDataWithoutDate.get_realtime_quotes_CB()

        #CSZLData.CSZLDataWithoutDate.get_cb_basic()


        pass

    elif action=='8':
        zzzz.Haitong2CSZL()

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



