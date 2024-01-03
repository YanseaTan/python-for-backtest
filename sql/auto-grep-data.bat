@REM @Author: Yansea
@REM @Date:   2023-12-08
@REM @Last Modified by:   Yansea
@REM Modified time: 2023-12-12@echo off  

C:  
cd C:\Users\yanse\Desktop\repo\python-for-backtest
start python ./sql/tushare-data-to-mysql/TushareDataToMySQL.py
start python ./sql/tfapi-data-to-mysql/TFAPIDataToMySQL.py
start python ./sql/tfapi-data-to-postgre/TFAPIDataToPostgre.py
timeout /t 60 >nul &
start python ./sql/tushare-data-to-mysql/FuturesMDToSpread.py
timeout /t 240 >nul &
start python ./sql/tushare-data-to-mysql/UpdateSpreadConfig.py
@REM timeout /t 600 >nul &
@REM start python ./sql/tushare-data-to-mysql/WriteFundsDataToXlsx.py

exit