# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-11-16
# @Last Modified by:   Yansea
# @Last Modified time: 2024-01-26

from FYAPI import FY_API
import datetime
import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import sys
sys.path.append('.')
from tools.DatabaseTools import *

# 初始化天风 api
h5_file_dir = "../output/tfapi/" #您自定义即可，但目录必须存在
h5_file_name = "test.hdf5" # 您自定义即可
h5_file_path = os.path.join(h5_file_dir, h5_file_name)
fy_api = FY_API(fileUrl=h5_file_path)

# 获取天风 api 所有数据类型并存入数据库
def get_index_info():
    indexInfo = fy_api.GetIndexInfo()
    write_data('index_info', 'futures', indexInfo)

# 获取指定数据类型在指定日期区间内的所有数据并存入数据库
def get_data(fut_code, index_type, symbol, start_date = '', end_date = ''):
    data = fy_api.Get_Data(symbol, start_date, end_date)
    data.insert(0, 'index_type', index_type)
    data.insert(0, 'fut_code', fut_code)
    for i in range(0, len(data)):
        data.iloc[i, 3] = data.iloc[i, 3][:10].replace('-', '')
        data.iloc[i, 4] = float(data.iloc[i, 4]) / 3.15
    data.columns = ['fut_code', 'index_type', 'index_name', 'update_date', 'value']
    write_data('fut_funds', 'futures', data)
    print('{} 写入成功！数据量：{}'.format(symbol, len(data)))

# 更新所有配置文件中的基本面数据至数据库中，并只更新还未更新的日期的数据
def update_all_data():
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
            last_update_date_df = read_data('futures', sql)
            if len(last_update_date_df):
                last_update_date = last_update_date_df.loc[0]['update_date']
                last_update_date = datetime.datetime.strptime(last_update_date, "%Y%m%d")
                oneday = datetime.timedelta(days=1)
                start_date = last_update_date + oneday
                start_date = start_date.strftime("%Y-%m-%d")
            else:
                start_date = ''
            get_data(fut_code, index_type, index_name, start_date)

def merge_data(index_name_list, new_index_name):
    sql = "select fut_code, index_type from fut_funds where index_name = '{}' limit 1;".format(index_name_list[0])
    basic_df = read_data('futures', sql)
    fut_code = basic_df.loc[0]['fut_code']
    index_type = basic_df.loc[0]['index_type']
    
    sql = "select update_date from fut_funds where index_name = '{}' order by update_date desc limit 1;".format(new_index_name)
    last_update_date_df = read_data('futures', sql)
    value_dict = {}
    if len(last_update_date_df):
        last_update_date = last_update_date_df.loc[0]['update_date']
        for i in range(0, len(index_name_list)):
            index_name = index_name_list[i]
            sql = "select update_date, value from fut_funds where index_name = '{}' and update_date > '{}' order by update_date".format(index_name, last_update_date)
            value_df = read_data('futures', sql)
            value_dict[i] = value_df
    else:
        for i in range(0, len(index_name_list)):
            index_name = index_name_list[i]
            sql = "select update_date, value from fut_funds where index_name = '{}' order by update_date".format(index_name)
            value_df = read_data('futures', sql)
            value_dict[i] = value_df
    
    index_dict = {}
    index_dict['fut_code'] = []
    index_dict['index_type'] = []
    index_dict['index_name'] = []
    index_dict['update_date'] = []
    index_dict['value'] = []
    date_set = set()
    for v in value_dict.values():
        for i in range(0, len(v)):
            update_date = v.loc[i]['update_date']
            date_set.add(update_date)
    date_list = sorted(date_set)
    for update_date in date_list:
        flag = True
        value = 0
        for i in range(0, len(value_dict)):
            value_df = value_dict[i]
            if update_date in value_df['update_date'].values:
                value_df = value_df[value_df['update_date'] == update_date]
                value_df.reset_index(drop=True, inplace=True)
                value += value_df.loc[0]['value']
                continue
            else:
                flag = False
                break
        if flag:
            index_dict['fut_code'].append(fut_code)
            index_dict['index_type'].append(index_type)
            index_dict['index_name'].append(new_index_name)
            index_dict['update_date'].append(update_date)
            index_dict['value'].append(value)
    
    new_df = pd.DataFrame(index_dict)
    write_data('fut_funds', 'futures', new_df)
    print("{}-{}数据合并生成完毕，已录入数据库！数据量：{}".format(fut_code, new_index_name, len(index_dict['update_date'])))

def main():
    # get_index_info()
    
    update_all_data()
    
    index_name_list = ['库存-电解镍库存-国内社会库存-上期所库存', '库存-电解镍库存-国内社会库存-华东隐性库存', '库存-电解镍库存-国内社会库存-广东南储库存']
    merge_data(index_name_list, '电解镍国内社会库存（吨）')
    
    # data = fy_api.Get_Data('库存-电解镍库存-国内社会库存-上期所库存')
    # print(data)


if __name__ == "__main__":
    main()
