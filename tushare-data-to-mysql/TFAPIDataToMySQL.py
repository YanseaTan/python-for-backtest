# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-11-16
# @Last Modified by:   Yansea
# @Last Modified time: 2023-11-16

from FYAPI import FY_API
from datetime import datetime
import os
import pandas as pd
from sqlalchemy import create_engine
from DatabaseTools import *
import matplotlib.pyplot as plt

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
    
def get_data(fut_code, index_type, symbol, start_date = '', end_date = ''):
    data = fy_api.Get_Data(symbol, start_date, end_date)
    data.insert(0, 'type', index_type)
    data.insert(0, 'fut_code', fut_code)
    data.insert(data.shape[1], 'change', 0)
    data.iloc[0, 3] = data.iloc[0, 3][:10].replace('-', '')
    data.iloc[0, 4] = float(data.iloc[0, 4]) / 3.15
    for i in range(1, len(data)):
        data.iloc[i, 3] = data.iloc[i, 3][:10].replace('-', '')
        data.iloc[i, 4] = float(data.iloc[i, 4]) / 3.15
        data.iloc[i, 5] = data.iloc[i, 4] - data.iloc[i - 1, 4]
    data.columns = ['fut_code', 'type', 'index_name', 'update_date', 'value', 'change']
    # print(data)
    # exit(1)
    engine_ts = creat_engine_with_database('futures')
    write_data(engine_ts, 'fut_funds', data)
    print('{} 写入成功！数据量：{}'.format(symbol, len(data)))

def main():
    # get_index_info()
    
    get_data('MA', 'spot_price', '甲醇（江苏低端）')
    exit(1)


if __name__ == "__main__":
    main()
