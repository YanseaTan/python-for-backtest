# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-03-26
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-27

import sqlite3
import pandas as pd

def cheak_kline_data(data):
    1

def main():
    conn = sqlite3.connect("./doc/KMarketData_20240325.db")
    cursor = conn.cursor()
    result = cursor.execute("select * from '60'").fetchall()
    columnDes = cursor.description #获取连接对象的描述信息
    columnNames = [columnDes[i][0] for i in range(len(columnDes))] #获取列名
    df = pd.DataFrame([list(i) for i in result],columns=columnNames) #得到的data为二维元组，逐行取出，转化为列表，再转化为df
    UpperLimitPrice = df.loc[0]['UpperLimitPrice']
    LowerLimitPrice = df.loc[0]['LowerLimitPrice']
    for i in range(0, len(df)):
        OpenPrice = df.loc[i]['OpenPrice']
        ClosePrice = df.loc[i]['ClosePrice']
        HighestPrice = df.loc[i]['HighestPrice']
        LowestPrice = df.loc[i]['LowestPrice']
        Volume = df.loc[i]['Volume']
        Turnover = df.loc[i]['Turnover']
        if OpenPrice == None or ClosePrice == None or HighestPrice == None or LowestPrice == None or LowestPrice < LowerLimitPrice or HighestPrice > UpperLimitPrice:
            print(df.loc[i],1)
        if OpenPrice == ClosePrice and ClosePrice == HighestPrice and HighestPrice == LowestPrice:
            print(df.loc[i],2)
        if Volume <= 0 or Turnover <= 0:
            print(df.loc[i],3)
            
    cursor.close()
    conn.close()
    
    


if __name__ == "__main__":
    main()
