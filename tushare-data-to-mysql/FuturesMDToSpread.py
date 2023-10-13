# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-10-13
# @Last Modified by:   Yansea
# @Last Modified time: 2023-10-13

import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

# 数据库用户配置
user = 'root'
password = '0527'
addr = 'localhost'

# 创建指定数据库操作引擎
def creat_engine_with_database(database):
    engine_ts = create_engine('mysql://{}:{}@/{}?charset=utf8&use_unicode=1'.format(user, password, database))
    return engine_ts

# 获取指定数据库的指定表格内容
def read_data(engine_ts, tableName, sql):
    df = pd.read_sql_query(sql, engine_ts)
    return df

# 将指定内容写入指定数据库的指定表格中
def write_data(engine_ts, tableName, df):
    res = df.to_sql(tableName, engine_ts, index=False, if_exists='append', chunksize=5000)
    print('写入成功！数据量:', res)
    
# 获取指定两个合约在所有重合交易日的价差数据
def get_fut_spread_daily(ins_1 = '', ins_2 = ''):
    engine_ts = creat_engine_with_database('futures')
    sql = '''select trade_date from fut_daily where ts_code = '{}';'''.format(ins_1)
    date_1 = read_data(engine_ts, 'fut_daily', sql)
    sql = '''select trade_date from fut_daily where ts_code = '{}';'''.format(ins_2)
    date_2 = read_data(engine_ts, 'fut_daily', sql)
    date = pd.merge(date_1, date_2)
    sql = '''select trade_date, close from fut_daily where ts_code = '{}';'''.format(ins_1)
    close_1 = read_data(engine_ts, 'fut_daily', sql)
    sql = '''select trade_date, close from fut_daily where ts_code = '{}';'''.format(ins_2)
    close_2 = read_data(engine_ts, 'fut_daily', sql)
    
    df = pd.DataFrame()
    ts_code = ins_1[:ins_1.index('.')] + '-' + ins_2[:ins_2.index('.')]
    ts_code_list = [ts_code] * len(date)
    trade_date_list = []
    close_list = []
    
    for i in range(0, len(date)):
        trade_date = date.loc[i]['trade_date']
        spread = close_1[close_1['trade_date'] == trade_date].iat[0, 1] - close_2[close_2['trade_date'] == trade_date].iat[0, 1]
        trade_date_list.append(trade_date)
        close_list.append(spread)
        
    df['ts_code'] = ts_code_list
    df['trade_date'] = trade_date_list
    df['close'] = close_list
    
    # 写入数据库
    # write_data(engine_ts, 'fut_spread_daily', df)
    
    # 绘制图像
    # figure,axes=plt.subplots(nrows=1,ncols=2,figsize=(20,5))
    # df.plot(ax=axes[0])         # 折线图
    # df.plot.kde(ax=axes[1])     # 概率分布图
    # plt.show()                  # 保持图像显示

def main():
    get_fut_spread_daily('HC2204.SHF', 'HC2205.SHF')


if __name__ == "__main__":
    main()
