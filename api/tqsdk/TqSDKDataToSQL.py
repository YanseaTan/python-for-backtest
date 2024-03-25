# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-03-19
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-25

import time
from numpy import NaN
import pandas as pd
import tushare as ts
import os
from sqlalchemy import create_engine
import sys
sys.path.append('.')
from tools.DatabaseTools import *
from tqsdk import TqApi, TqAuth
from contextlib import closing
from tqsdk.tools import DataDownloader
from datetime import datetime

def export_tqsdk_stock_kline_data_to_csv(api):
    sql = "select distinct ts_code from stock.stock_basic order by ts_code"
    ts_code_df = read_postgre_data(sql)
    for i in range(0, len(ts_code_df)):
        ts_code = ts_code_df.loc[i]['ts_code']
        if ts_code[-2:] == 'SH':
            ts_code = 'SSE.' + ts_code[:6]
        elif ts_code[-2:] == 'SZ':
            ts_code = 'SZSE.' + ts_code[:6]
        else:
            continue
    
        kd = DataDownloader(api, symbol_list=ts_code, dur_sec=60,
                        start_dt=datetime(2021, 1, 1, 6, 0 ,0), end_dt=datetime(2024, 3, 18, 16, 0, 0), csv_file_name="./doc/minute-kline/stock/{}.kline.csv".format(ts_code))
        
        while not kd.is_finished():
            api.wait_update()
            print("{} progress: kline: %.2f%%".format(ts_code) % (kd.get_progress()))
        
        print("{} 分钟 k 线导出完毕，进度：{}%".format(ts_code, round((i + 1) / len(ts_code_df) * 100, 2)))

def export_tqsdk_index_kline_data_to_csv(api):
    code_list = ['SSE.000016', 'SSE.000300', 'SSE.000905', 'SSE.000852']
    for i in range(0, len(code_list)):
        code = code_list[i]
        kd = DataDownloader(api, symbol_list=code, dur_sec=60,
                        start_dt=datetime(2018, 1, 1, 6, 0 ,0), end_dt=datetime(2024, 3, 18, 16, 0, 0), csv_file_name="./doc/minute-kline/index/{}.kline.csv".format(code))
        while not kd.is_finished():
            api.wait_update()
            print("{} progress: kline: %.2f%%".format(code) % (kd.get_progress()))

def export_tqsdk_opt_kline_data_to_csv(api, file):
    opt_basic_df = pd.read_csv('./doc/opt-basic/{}'.format(file), encoding='gbk')
    opt_basic_df = opt_basic_df[(opt_basic_df.delist_date >= 20180101) & (opt_basic_df.ts_code > '90000728.SZ')].copy()
    opt_basic_df.sort_values(by='ts_code', ascending=True, inplace=True)
    opt_basic_df.reset_index(drop=True, inplace=True)
    code_list = opt_basic_df['ts_code'].tolist()
    
    for i in range(0, len(code_list)):
        ts_code = code_list[i]
        if ts_code[-2:] == 'SH':
            ts_code = 'SSE.' + ts_code[:8]
        elif ts_code[-2:] == 'SZ':
            ts_code = 'SZSE.' + ts_code[:8]
        else:
            continue
            
        kd = DataDownloader(api, symbol_list=ts_code, dur_sec=60,
                        start_dt=datetime(2018, 1, 1, 6, 0 ,0), end_dt=datetime(2024, 3, 18, 16, 0, 0), csv_file_name="./doc/minute-kline/opt/{}.kline.csv".format(ts_code))
        
        while not kd.is_finished():
            api.wait_update()
            print("{} progress: kline: %.2f%%".format(ts_code) % (kd.get_progress()))
        
        print("{} 分钟 k 线导出完毕，进度：{}%".format(ts_code, round((i + 1) / len(code_list) * 100, 2)))
        
def export_tqsdk_stock_tick_data_to_csv(api, code_list):
    for i in range(0, len(code_list)):
        ts_code = code_list[i]
        if ts_code[-2:] == 'SH':
            ts_code = 'SSE.' + ts_code[:6]
        elif ts_code[-2:] == 'SZ':
            ts_code = 'SZSE.' + ts_code[:6]
        else:
            continue
    
        kd = DataDownloader(api, symbol_list=ts_code, dur_sec=0,
                        start_dt=datetime(2024, 2, 22, 6, 0 ,0), end_dt=datetime(2024, 3, 22, 16, 0, 0), csv_file_name="./doc/tick-data/stock/{}.tick.csv".format(ts_code))
        
        while not kd.is_finished():
            api.wait_update()
            print("{} progress: tick: %.2f%%".format(ts_code) % (kd.get_progress()))
        
        print("{} tick 数据导出完毕，进度：{}%".format(ts_code, round((i + 1) / len(code_list) * 100, 2)))
        
def export_tqsdk_index_tick_data_to_csv(api):
    code_list = ['SSE.000016', 'SSE.000300', 'SSE.000905', 'SSE.000852']
    for i in range(0, len(code_list)):
        code = code_list[i]
        kd = DataDownloader(api, symbol_list=code, dur_sec=0,
                        start_dt=datetime(2024, 2, 22, 6, 0 ,0), end_dt=datetime(2024, 3, 22, 16, 0, 0), csv_file_name="./doc/tick-data/index/{}.tick.csv".format(code))
        while not kd.is_finished():
            api.wait_update()
            print("{} progress: tick: %.2f%%".format(code) % (kd.get_progress()))

def main():
    api = TqApi(web_gui=True, auth=TqAuth("iafos", "1063917351mm"))
    
    # export_tqsdk_stock_kline_data_to_csv(api)
    # export_tqsdk_index_kline_data_to_csv(api)
    
    # path = './doc/opt-basic'
    # files = os.listdir(path)
    # for i in range(0, len(files)):
    #     file = files[i]
    #     export_tqsdk_opt_kline_data_to_csv(api, file)
    
    # code_list = ['123167.SZ', '113672.SH', '127080.SZ', '123210.SZ', '127101.SZ', '127077.SZ', '110045.SH', '110088.SH', '113044.SH']
    # export_tqsdk_stock_tick_data_to_csv(api, code_list)
    
    export_tqsdk_index_tick_data_to_csv(api)
    
    closing(api)

if __name__ == "__main__":
    main()
