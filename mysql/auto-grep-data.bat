@REM @Author: Yansea
@REM @Date:   2023-12-08
@REM @Last Modified by:   Yansea
@REM Modified time: 2023-12-12@echo off  

C:  
cd C:\Users\yanse\Desktop\repo\python-for-backtest
start python ./mysql/tushare-data-to-mysql/TushareDataToMySQL.py
start python ./mysql/tfapi-data-to-mysql/TFAPIDataToMySQL.py
start python ./mysql/tushare-data-to-mysql/FuturesMDToSpread.py
start python ./mysql/tushare-data-to-mysql/UpdateSpreadConfig.py
start python ./mysql/tushare-data-to-mysql/WriteSpreadDataToXlsx.py

start python ./postgre/tfapi-data-to-postgre/TFAPIDataToPostgre.py
exit