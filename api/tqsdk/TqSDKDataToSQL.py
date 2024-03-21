# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-03-19
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-21

import time
from numpy import NaN
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
import sys
sys.path.append('.')
from tools.DatabaseTools import *
from tqsdk import TqApi, TqAuth
from contextlib import closing
from tqsdk.tools import DataDownloader
from datetime import datetime

def export_tqsdk_kline_data_to_csv():
    api = TqApi(web_gui=True, auth=TqAuth("iafos", "1063917351mm"))
    
    sql = "select distinct ts_code from stock.stock_basic order by ts_code"
    ts_code_df = read_postgre_data(sql)
    ts_code_df = ts_code_df[ts_code_df.ts_code > '601238.SH'].copy()
    ts_code_df.reset_index(drop=True, inplace=True)
    for i in range(0, len(ts_code_df)):
        ts_code = ts_code_df.loc[i]['ts_code']
        if ts_code[-2:] == 'SH':
            ts_code = 'SSE.' + ts_code[:6]
        elif ts_code[-2:] == 'SZ':
            ts_code = 'SZSE.' + ts_code[:6]
        else:
            continue
    
        kd = DataDownloader(api, symbol_list=ts_code, dur_sec=60,
                        start_dt=datetime(2021, 1, 1, 6, 0 ,0), end_dt=datetime(2024, 3, 18, 16, 0, 0), csv_file_name="./doc/minute-kline/{}.kline.csv".format(ts_code))
        
        while not kd.is_finished():
            api.wait_update()
            print("{} progress: kline: %.2f%%".format(ts_code) % (kd.get_progress()))
        
        print("{} 分钟 k 线导出完毕，进度：{}%".format(ts_code, round((i + 1) / len(ts_code_df) * 100, 2)))
    
    closing(api)

def main():
    export_tqsdk_kline_data_to_csv()


if __name__ == "__main__":
    main()
