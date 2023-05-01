#coding=utf-8
import sys
import CSZLData
import CSZLWorkflow
import CSZLUtils

if __name__ == '__main__':

    
    inputvalues=sys.argv
    if len(inputvalues)>1:
        print("脚本预设参数")
        action=inputvalues[1]
    else:
        print("系统接收参数")
        print("CSZL2022 功能列表：\n 1:BackTesting \n 2:RealTimePredict \n 3:BackTesting_snowball\n"
               " 4:BackTesting_haitong\n 5:ServerRun\n 6:Convertible bond BackTesting\n 7:RealTimePredict_CB\n"
               " 8:Haitong2CSZL\n 9:PKL2CSV\n a:CB BackTesting_haitong\n b:changetoqlib\n e:opt_test")
        action=input()


    zzzz=CSZLWorkflow.CSZLWorkflow()

    if action=='1':
        zzzz.BackTesting()

    elif action=='2':
        #"Today_result.csv"
        zzzz.RealTimePredict_FE09c()
        zzzz.Haitong2CSZL()
        zzzz.Todays_action('last_result_real.csv',"Today_result.csv",5,7000)
    elif action=='3':
        zzzz.BackTesting_static_0828()
        zzzz.Todays_action('last_result_snowball.csv','Today_NEXT_predict.csv',2,200000)
    elif action=='4':
        zzzz.BackTesting_static_0828()
        zzzz.Haitong2CSZL()
        zzzz.Todays_action('last_result_real.csv','Today_NEXT_predict.csv',5,7000)
    elif action=='5':
        zzzz.PredictBackRound()

    elif action=='6':
        
        #CSZLUtils.CSZLUtils.fun01()
        zzzz.CBBackTesting()

        pass
    elif action=='7':

        zzzz.RealTimePredict_CB()
        zzzz.Haitong2CSZL_CB()
        zzzz.Todays_action_CB('last_result_real_CB.csv',"Today_result_CB.csv",4,7000)

        pass

    elif action=='8':

        zzzz.Zhaoshang2CSZL_CB()
        zzzz.Zhaoshang2CSZL()
        zzzz.Haitong2CSZL()
        zzzz.Haitong2CSZL_CB()

    elif action=='9':
        zzzz.PKL2CSV()

    elif action=='a':
        zzzz.CBBackTesting_static_0515()
        #zzzz.Haitong2CSZL_CB()
        #zzzz.Todays_action_CB('last_result_real_CB.csv','Today_NEXT_predict_CB.csv',4,200000)
        zzzz.Zhaoshang2CSZL_CB()
        zzzz.Todays_action_CB('last_result_real_CB_ZS.csv',"Today_NEXT_predict_CB.csv",4,200000)

    elif action=='b':
        CSZLUtils.CSZLUtils.changetoqlib()

    elif action=='c':

        zzzzd=CSZLUtils.CSZLUtils.TimeUpper(2118)

        print(zzzzd)

    elif action=='d':
        zzzz.PredictBackRound_CB()

    elif action=='e':


        CSZLDataLoader=CSZLData.CSZLData("20200101","20221111")
        CSZLDataLoader.update_opt()

    elif action=='f':

        zzzz.BackTesting_compare()

        zzzzz=1



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



