# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-11-16
# @Last Modified by:   Yansea
# @Last Modified time: 2023-12-07

from FYAPI import FY_API
import datetime
import os
import json
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
from DatabaseTools import *

# 初始化天风 api
h5_file_dir = "../output/tfapi/" #您自定义即可，但目录必须存在
h5_file_name = "test.hdf5" # 您自定义即可
h5_file_path = os.path.join(h5_file_dir, h5_file_name)
fy_api = FY_API(fileUrl=h5_file_path)

# 获取天风 api 所有数据类型并存入数据库
def get_index_info():
    indexInfo = fy_api.GetIndexInfo()
    engine_ts = creat_engine_with_database('tfapi')
    write_data(engine_ts, 'index_info', indexInfo)

# 获取指定数据类型在指定日期区间内的所有数据并存入数据库
def get_data(fut_code, index_type, symbol, start_date = '', end_date = ''):
    data = fy_api.Get_Data(symbol, start_date, end_date)
    data.insert(0, 'index_type', index_type)
    data.insert(0, 'fut_code', fut_code)
    for i in range(0, len(data)):
        data.iloc[i, 3] = data.iloc[i, 3][:10].replace('-', '')
        data.iloc[i, 4] = float(data.iloc[i, 4]) / 3.15
    data.columns = ['fut_code', 'index_type', 'index_name', 'update_date', 'value']
    # print(data)
    # exit(1)
    engine_ts = creat_engine_with_database('futures')
    write_data(engine_ts, 'fut_funds', data)
    print('{} 写入成功！数据量：{}'.format(symbol, len(data)))

# 更新所有配置文件中的基本面数据至数据库中，并只更新还未更新的日期的数据
def update_all_data():
    engine_ts = creat_engine_with_database('futures')
    f = open('./config/FundamentalsConfig.json', 'r', encoding='utf-8')
    content = f.read()
    config_json = json.loads(content)
    f.close()
    
    for i in range(0, len(config_json)):
        fut_code = config_json[i]['fut_code']
        for j in range(0, len(config_json[i]['index'])):
            index_type = config_json[i]['index'][j]['index_type']
            index_name = config_json[i]['index'][j]['index_name']
            sql = "select update_date from fut_funds where index_name = '{}' order by update_date desc limit 1;".format(index_name)
            last_update_date_df = read_data(engine_ts, sql)
            if len(last_update_date_df):
                last_update_date = last_update_date_df.loc[0]['update_date']
                last_update_date = datetime.datetime.strptime(last_update_date, "%Y%m%d")
                oneday = datetime.timedelta(days=1)
                start_date = last_update_date + oneday
                start_date = start_date.strftime("%Y-%m-%d")
            else:
                start_date = ''
            get_data(fut_code, index_type, index_name, start_date)

def main():
    # get_index_info()
    
    update_all_data()
    exit(1)
    
    data = fy_api.Get_Data('硅锰：6517：采购价格：河钢集团（月）')
    print(data)


if __name__ == "__main__":
    main()
