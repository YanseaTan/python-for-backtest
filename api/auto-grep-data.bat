@REM @Author: Yansea
@REM @Date:   2023-12-08
@REM @Last Modified by:   Yansea
@REM Modified time: 2023-12-12@echo off  

C:  
cd C:\Users\yanse\Desktop\repo\python-for-backtest
start mysql -u root -p0527
timeout /t 10 >nul &
start python ./api/tushare/TushareDataToSQL.py
start python ./api/tfapi/TFAPIDataToSQL.py
timeout /t 60 >nul &
start python ./proc/FuturesMDToSpread.py
timeout /t 240 >nul &
start python ./proc/UpdateSpreadConfig.py
@REM timeout /t 600 >nul &
@REM start python ./sql/tushare-data-to-mysql/WriteFundsDataToXlsx.py

exit