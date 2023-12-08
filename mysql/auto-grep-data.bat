@echo off  
C:  
cd C:\Users\yanse\Desktop\repo\python-for-backtest
start python ./mysql/tushare-data-to-mysql/TushareDataToMySQL.py
start python ./mysql/tfapi-data-to-mysql/TFAPIDataToMySQL.py
start python ./mysql/tushare-data-to-mysql/FuturesMDToSpread.py
start python ./mysql/tushare-data-to-mysql/UpdateSpreadConfig.py
start python ./mysql/tushare-data-to-mysql/WriteSpreadDataToXlsx.py
exit