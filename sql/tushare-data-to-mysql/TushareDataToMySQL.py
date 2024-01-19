# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-10-10
# @Last Modified by:   Yansea
# @Last Modified time: 2024-01-19

import time
import datetime
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
from DatabaseTools import *

# Tushare 账户 token
token = 'e59d203345b5dac84a150b2abb7b49dcb06b6c2abefa7bc49c06bea1'

# 获取所有日行情数据库中缺少的交易日期合集
def get_trade_date_set():
    engine_ts = creat_engine_with_database('bond')
    sql = 'select distinct trade_date from cb_daily order by trade_date desc limit 1'
    last_trade_date_df = read_data(engine_ts, sql)
    last_trade_date = last_trade_date_df.loc[0]['trade_date']
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    i = 0
    trade_date_set = set()
    while True:
        date = today - i * oneday
        i += 1
        dateStr = date.strftime('%Y%m%d')
        df = pro.trade_cal(**{"cal_date":dateStr}, fields=["pretrade_date"])
        if df.loc[0]['pretrade_date'] != last_trade_date:
            trade_date_set.add(df.loc[0]['pretrade_date'])
        else :
            break
    return trade_date_set

# 获取所有股票基本信息
def get_all_stock_basic_data():
    engine_ts = creat_engine_with_database('stock')
    df = pro.stock_basic()
    write_data(engine_ts, 'stock_basic', df)
    
# 更新所有股票基本信息
def update_stock_basic_data():
    engine_ts = creat_engine_with_database('stock')
    sql = 'select distinct list_date from stock_basic order by list_date desc limit 1'
    last_list_date_df = read_data(engine_ts, sql)
    last_list_date = last_list_date_df.loc[0]['list_date']
    df = pro.stock_basic()
    df.drop(df[(df.list_date <= last_list_date)].index, inplace=True)
    write_data(engine_ts, 'stock_basic', df)
    print('更新所有股票基本信息成功！数据量:', len(df))

# 获取所有可转债基本信息
def get_all_cb_basic_data():
    engine_ts = creat_engine_with_database('bond')
    df = pro.cb_basic()
    df.to_sql('cb_basic', engine_ts, index=False, if_exists='replace', chunksize=5000)
    print('更新所有可转债基本信息成功！数据量:', len(df))
    
# 获取所有可转债的所有历史日行情数据
def get_all_cb_md_data():
    engine_ts = creat_engine_with_database('bond')
    sql = 'SELECT ts_code FROM cb_basic'
    ts_code = read_data(engine_ts, sql)
    for i in range(0, len(ts_code)):
        # 若调用次数达到限制，则在一分钟内反复尝试
        for _ in range(60):
            try:
                df = pro.cb_daily(**{'ts_code': ts_code.loc[i]['ts_code']},
                                  fields=["ts_code","trade_date","pre_close","open","high","low","close","change",
                                          "pct_chg","vol","amount","bond_value","bond_over_rate","cb_value","cb_over_rate"])
                if len(df):
                    write_data(engine_ts, 'cb_daily', df)
                else:
                    print('回调数据为空！可转债代码:', ts_code.loc[i]['ts_code'])
            except:
                time.sleep(1)
            else:
                break
        print("{} 日行情数据导入成功，总进度 {}%".format(ts_code.loc[i]['ts_code'], round((i + 1) / len(ts_code) * 100, 2)))
            
# 获取指定交易日所有可转债的日行情数据
def update_cb_md_data(trade_date = ''):
    engine_ts = creat_engine_with_database('bond')
    df = pro.cb_daily(**{"trade_date": trade_date},
                      fields=["ts_code","trade_date","pre_close","open","high","low","close","change",
                              "pct_chg","vol","amount","bond_value","bond_over_rate","cb_value","cb_over_rate"])
    write_data(engine_ts, 'cb_daily', df)
    print('新增可转债日行情 {} 条！'.format(len(df)))

# 获取期货合约基本信息
def get_all_fut_basic_data():
    engine_ts = creat_engine_with_database('futures')
    futuresExchanges = ['CFFEX', 'DCE', 'CZCE', 'SHFE', 'INE', 'GFEX']
    for exchange in futuresExchanges:
        df = pro.fut_basic(**{"exchange": exchange}, fields=["symbol","exchange","ts_code","name","fut_code","trade_unit","per_unit","list_date","delist_date","d_month","last_ddate"])
        write_data(engine_ts, 'fut_basic', df)
        
def update_fut_basic_data():
    engine_ts = creat_engine_with_database('futures')
    sql = 'select distinct list_date from fut_basic order by list_date desc limit 1'
    last_list_date_df = read_data(engine_ts, sql)
    last_list_date = last_list_date_df.loc[0]['list_date']
    futuresExchanges = ['CFFEX', 'DCE', 'CZCE', 'SHFE', 'INE', 'GFEX']
    for exchange in futuresExchanges:
        df = pro.fut_basic(**{"exchange": exchange}, fields=["symbol","exchange","ts_code","name","fut_code","trade_unit","per_unit","list_date","delist_date","d_month","last_ddate"])
        drop_list = []
        for i in range(0, len(df)):
            if df.loc[i]['list_date'] == None or df.loc[i]['list_date'] <= last_list_date:
                drop_list.append(i)
        df.drop(drop_list, inplace=True)
        write_data(engine_ts, 'fut_basic', df)
        print('更新 {} 期货合约基本信息成功！数据量：{}'.format(exchange, len(df)))

# 获取所有期货合约的所有历史日行情数据
def get_all_fut_md_data():
    engine_ts = creat_engine_with_database('futures')
    # 去除主力/连续合约
    sql = 'SELECT ts_code FROM fut_basic WHERE per_unit is not NULL'
    ts_code = read_data(engine_ts, sql)
    for i in range(0, len(ts_code)):
        # 若调用次数达到限制，则在一分钟内反复尝试
        for _ in range(60):
            try:
                df = pro.fut_daily(**{'ts_code': ts_code.loc[i]['ts_code']})
                if len(df):
                    write_data(engine_ts, 'fut_daily', df)
                else:
                    print('回调数据为空！期货合约代码:', ts_code.loc[i]['ts_code'])
            except:
                time.sleep(1)
            else:
                break
            
# 获取指定交易日所有期货合约的日行情数据
def update_fut_md_data(trade_date = ''):
    engine_ts = creat_engine_with_database('futures')
    # 去除主力/连续合约
    sql = 'SELECT ts_code FROM fut_basic WHERE per_unit is NULL'
    ts_code = read_data(engine_ts, sql)
    code_list = ts_code['ts_code'].tolist()
    df = pro.fut_daily(**{"trade_date": trade_date})
    del_index = []
    for i in range(0, len(df)):
        if str(df.iloc[i].iat[0]) in code_list:
            del_index.append(i)
    df = df.drop(df.index[del_index])
    write_data(engine_ts, 'fut_daily', df)
    print('新增期货日行情 {} 条！'.format(len(df)))
        
# 每日将新增的各类昨日行情自动导入对应的表中
def update_daily_data():
    update_stock_basic_data()
    get_all_cb_basic_data()
    update_fut_basic_data()
    
    trade_date_set = get_trade_date_set()
    for trade_date in trade_date_set:
        update_cb_md_data(trade_date)
        update_fut_md_data(trade_date)

if __name__ == '__main__':
    # 登录 Tushare 接口
    pro = ts.pro_api(token)
    
    update_daily_data()
