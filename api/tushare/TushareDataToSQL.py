# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-10-10
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-12

import time
import datetime
from numpy import NaN
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
import sys
sys.path.append('.')
from tools.DatabaseTools import *

# Tushare 账户 token
token = 'e59d203345b5dac84a150b2abb7b49dcb06b6c2abefa7bc49c06bea1'

# 获取所有日行情数据库中缺少的交易日期合集
def get_trade_date_set():
    sql = 'select distinct trade_date from bond.cb_daily order by trade_date desc limit 1'
    last_trade_date_df = read_postgre_data(sql)
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
    df = pro.stock_basic()
    write_data('stock_basic', 'stock', df)
    
# 更新所有股票基本信息
def update_stock_basic_data():
    sql = 'select distinct list_date from stock.stock_basic order by list_date desc limit 1'
    last_list_date_df = read_postgre_data(sql)
    last_list_date = last_list_date_df.loc[0]['list_date']
    df = pro.stock_basic()
    df.drop(df[(df.list_date <= last_list_date)].index, inplace=True)
    write_data('stock_basic', 'stock', df)
    
    print('更新所有股票基本信息成功！数据量:', len(df))

# 获取所有可转债基本信息
def get_all_cb_basic_data():
    df = pro.cb_basic()
    engine_ts = create_engine('mysql://{}:{}@/{}?charset=utf8&use_unicode=1'.format(mysql_user, mysql_password, 'bond'))
    df.to_sql('cb_basic', engine_ts, index=False, if_exists='replace', chunksize=5000)
    df.to_sql('cb_basic', postgre_engine_ts, index=False, if_exists='replace', chunksize=5000)
    print('更新所有可转债基本信息成功！数据量:', len(df))
    
# 获取所有可转债的所有历史日行情数据
def get_all_cb_md_data():
    sql = 'SELECT ts_code FROM bond.cb_basic'
    ts_code = read_postgre_data(sql)
    for i in range(0, len(ts_code)):
        # 若调用次数达到限制，则在一分钟内反复尝试
        for _ in range(60):
            try:
                df = pro.cb_daily(**{'ts_code': ts_code.loc[i]['ts_code']},
                                  fields=["ts_code","trade_date","pre_close","open","high","low","close","change",
                                          "pct_chg","vol","amount","bond_value","bond_over_rate","cb_value","cb_over_rate"])
                if len(df):
                    write_data('cb_daily', 'bond', df)
                else:
                    print('回调数据为空！可转债代码:', ts_code.loc[i]['ts_code'])
            except:
                time.sleep(1)
            else:
                break
        print("{} 日行情数据导入成功，总进度 {}%".format(ts_code.loc[i]['ts_code'], round((i + 1) / len(ts_code) * 100, 2)))
            
# 获取指定交易日所有可转债的日行情数据
def update_cb_md_data(trade_date = ''):
    df = pro.cb_daily(**{"trade_date": trade_date},
                      fields=["ts_code","trade_date","pre_close","open","high","low","close","change",
                              "pct_chg","vol","amount","bond_value","bond_over_rate","cb_value","cb_over_rate"])
    write_data('cb_daily', 'bond', df)
    print('新增 {} 可转债日行情 {} 条！'.format(trade_date, len(df)))

# 获取期货合约基本信息
def get_all_fut_basic_data():
    futuresExchanges = ['CFFEX', 'DCE', 'CZCE', 'SHFE', 'INE', 'GFEX']
    for exchange in futuresExchanges:
        df = pro.fut_basic(**{"exchange": exchange}, fields=["symbol","exchange","ts_code","name","fut_code","trade_unit","per_unit","list_date","delist_date","d_month","last_ddate"])
        write_data('fut_basic', 'futures', df)

# 更新期货合约基本信息
def update_fut_basic_data():
    sql = 'select distinct list_date from future.fut_basic order by list_date desc limit 1'
    last_list_date_df = read_postgre_data(sql)
    last_list_date = last_list_date_df.loc[0]['list_date']
    futuresExchanges = ['CFFEX', 'DCE', 'CZCE', 'SHFE', 'INE', 'GFEX']
    for exchange in futuresExchanges:
        df = pro.fut_basic(**{"exchange": exchange}, fields=["symbol","exchange","ts_code","name","fut_code","trade_unit","per_unit","list_date","delist_date","d_month","last_ddate"])
        drop_list = []
        for i in range(0, len(df)):
            if df.loc[i]['list_date'] == None or df.loc[i]['list_date'] <= last_list_date:
                drop_list.append(i)
        df.drop(drop_list, inplace=True)
        write_data('fut_basic', 'futures', df)
        print('更新 {} 期货合约基本信息成功！数据量：{}'.format(exchange, len(df)))

# 获取所有期货合约的所有历史日行情数据
def get_all_fut_md_data():
    # 去除主力/连续合约
    sql = 'SELECT ts_code FROM future.fut_basic WHERE per_unit is not NULL'
    ts_code = read_postgre_data(sql)
    for i in range(0, len(ts_code)):
        # 若调用次数达到限制，则在一分钟内反复尝试
        for _ in range(60):
            try:
                df = pro.fut_daily(**{'ts_code': ts_code.loc[i]['ts_code']})
                if len(df):
                    write_data('fut_daily', 'futures', df)
                else:
                    print('回调数据为空！期货合约代码:', ts_code.loc[i]['ts_code'])
            except:
                time.sleep(1)
            else:
                break
            
# 获取指定交易日所有期货合约的日行情数据
def update_fut_md_data(trade_date = ''):
    # 去除主力/连续合约
    sql = 'SELECT ts_code FROM future.fut_basic WHERE per_unit is NULL'
    ts_code = read_postgre_data(sql)
    code_list = ts_code['ts_code'].tolist()
    df = pro.fut_daily(**{"trade_date": trade_date})
    del_index = []
    for i in range(0, len(df)):
        if str(df.iloc[i].iat[0]) in code_list:
            del_index.append(i)
    df = df.drop(df.index[del_index])
    write_data('fut_daily', 'futures', df)
    print('新增 {} 期货日行情 {} 条！'.format(trade_date, len(df)))
    
# 获取所有期货品种的所有历史仓单数据
def get_all_fut_warehouse_data():
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    dateStr = (today - oneday).strftime('%Y%m%d')
    date_df = pro.trade_cal(**{"start_date":"20200101","end_date":dateStr,"is_open":"1"}, fields=["cal_date"])
    for i in range(0, len(date_df)):
        dateStr = date_df.loc[i]['cal_date']
        # 若调用次数达到限制，则在一分钟内反复尝试
        for _ in range(60):
            try:
                warehouse_df = pro.fut_wsr(**{"trade_date":dateStr}, fields=["trade_date","symbol","fut_name","pre_vol","vol","vol_chg","unit","warehouse","exchange"])
                if len(warehouse_df):
                    write_data('fut_warehouse', 'futures', warehouse_df)
                else:
                    print('回调数据为空！查询日期:', dateStr)
            except:
                time.sleep(1)
            else:
                break
        print("交易日 {} 仓单数据导入成功！进度：{}%".format(dateStr, round((i + 1) / len(date_df) * 100, 2)))
        
# 获取指定交易日所有期货品种的仓单数据
def update_fut_warehouse_data(trade_date = ''):
    warehouse_df = pro.fut_wsr(**{"trade_date":trade_date}, fields=["trade_date","symbol","fut_name","pre_vol","vol","vol_chg","unit","warehouse","exchange"])
    write_data('fut_warehouse', 'futures', warehouse_df)
    print('新增 {} 期货仓单数据 {} 条！'.format(trade_date, len(warehouse_df)))
    
# 获取所有期货品种的所有历史仓单汇总数据
def get_all_fut_warehouse_data_sum():
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    dateStr = (today - oneday).strftime('%Y%m%d')
    date_df = pro.trade_cal(**{"start_date":"20200101","end_date":dateStr,"is_open":"1"}, fields=["cal_date"])
    for i in range(0, len(date_df)):
        dateStr = date_df.loc[i]['cal_date']
        # 若调用次数达到限制，则在一分钟内反复尝试
        for _ in range(60):
            try:
                warehouse_df = pro.fut_wsr(**{"trade_date":dateStr}, fields=["trade_date","symbol","vol"])
                if len(warehouse_df):
                    vol_dict = {}
                    for j in range(0, len(warehouse_df)):
                        if warehouse_df.loc[j]['vol'] == NaN:
                            print('NaN')
                            continue
                        key = warehouse_df.loc[j]['trade_date'] + warehouse_df.loc[j]['symbol']
                        if key in vol_dict.keys():
                            vol_dict[key] += warehouse_df.loc[j]['vol']
                        else:
                            vol_dict[key] = warehouse_df.loc[j]['vol']
                    warehouse_sum_dict = {}
                    warehouse_sum_dict['trade_date'] = []
                    warehouse_sum_dict['symbol'] = []
                    warehouse_sum_dict['vol'] = []
                    for k, v in vol_dict.items():
                        warehouse_sum_dict["trade_date"].append(k[:8])
                        warehouse_sum_dict["symbol"].append(k[8:])
                        warehouse_sum_dict["vol"].append(v)
                    warehouse_sum_df = pd.DataFrame(warehouse_sum_dict)
                    write_data('fut_warehouse_sum', 'futures', warehouse_sum_df)
                else:
                    print('回调数据为空！查询日期:', dateStr)
            except:
                time.sleep(1)
            else:
                break
        print("交易日 {} 仓单汇总数据导入成功！进度：{}%".format(dateStr, round((i + 1) / len(date_df) * 100, 2)))
        
# 获取指定交易日所有期货品种的仓单汇总数据
def update_fut_warehouse_data_sum(trade_date = ''):
    warehouse_df = pro.fut_wsr(**{"trade_date":trade_date}, fields=["trade_date","symbol","vol"])
    vol_dict = {}
    for j in range(0, len(warehouse_df)):
        if warehouse_df.loc[j]['vol'] == NaN:
            continue
        key = warehouse_df.loc[j]['trade_date'] + warehouse_df.loc[j]['symbol']
        if key in vol_dict.keys():
            vol_dict[key] += warehouse_df.loc[j]['vol']
        else:
            vol_dict[key] = warehouse_df.loc[j]['vol']
    warehouse_sum_dict = {}
    warehouse_sum_dict['trade_date'] = []
    warehouse_sum_dict['symbol'] = []
    warehouse_sum_dict['vol'] = []
    for k, v in vol_dict.items():
        warehouse_sum_dict["trade_date"].append(k[:8])
        warehouse_sum_dict["symbol"].append(k[8:])
        warehouse_sum_dict["vol"].append(v)
    warehouse_sum_df = pd.DataFrame(warehouse_sum_dict)
    write_data('fut_warehouse_sum', 'futures', warehouse_sum_df)
    print('新增 {} 期货仓单汇总数据 {} 条！'.format(trade_date, len(warehouse_sum_df)))

# 获取最新的交易日历，一年更新一次就好
def get_all_fut_cal_date():
    cal_date_df = pro.trade_cal(**{"start_date":'19950101', "end_date":'20500101', "is_open":1}, fields=["cal_date"])
    write_data('fut_cal_date', 'futures', cal_date_df)
    print('新增交易日历数据 {} 条！'.format(len(cal_date_df)))

# 每日将新增的各类昨日行情自动导入对应的表中
def update_daily_data():
    update_stock_basic_data()
    get_all_cb_basic_data()
    update_fut_basic_data()
    
    trade_date_set = get_trade_date_set()
    for trade_date in trade_date_set:
        update_cb_md_data(trade_date)
        update_fut_md_data(trade_date)
        update_fut_warehouse_data(trade_date)
        update_fut_warehouse_data_sum(trade_date)

def bond_daily_md_proc(start_date, end_date, ts_code):
    remain_size_df = pro.cb_share(**{"ts_code": ts_code}, fields=["ts_code","end_date","remain_size"])
    
    sql = "select issue_size, stk_code from bond.cb_basic where ts_code = '{}'".format(ts_code)
    cb_basic_df = read_postgre_data(sql)
    issue_size = cb_basic_df.loc[0]['issue_size']
    stk_code = cb_basic_df.loc[0]['stk_code']
    
    sql = "SELECT ts_code, trade_date, open, high, low, close, change, pct_chg, vol, amount FROM bond.cb_daily\
        WHERE ts_code = '{}' and trade_date >= '{}' and trade_date <= '{}' order by trade_date".format(ts_code, start_date, end_date)
    cb_md_df = read_postgre_data(sql)
    cb_md_df.insert(1, 'stk_code', stk_code)
    cb_md_df.insert(len(cb_md_df.columns), 'remain_size', 0)
    cb_md_df.insert(len(cb_md_df.columns), 'turn_over', 0)
    
    for i in range(0, len(cb_md_df)):
        trade_date = cb_md_df.loc[i]['trade_date']
        vol = cb_md_df.loc[i]['vol']
        trade_date_format = trade_date[:4] + '-' + trade_date[4:6] + '-' + trade_date[6:8]
        current_remain_size_df = remain_size_df[remain_size_df.end_date <= trade_date_format].copy()
        current_remain_size_df.sort_values(by='end_date', ascending=False, inplace=True)
        current_remain_size_df.reset_index(drop=True, inplace=True)
        if len(current_remain_size_df) == 0:
            remain_size = issue_size
        else:
            remain_size = current_remain_size_df.loc[0]['remain_size']
        cb_md_df.loc[cb_md_df.trade_date == trade_date, 'remain_size'] = remain_size
        turn_over = vol * 10000 / remain_size
        cb_md_df.loc[cb_md_df.trade_date == trade_date, 'turn_over'] = turn_over
    
    stk_md_df = pro.bak_daily(**{"ts_code": stk_code,"start_date": start_date,"end_date": end_date},
                       fields=["ts_code","trade_date","open","high","low","close","change","pct_change","vol","amount","turn_over"])
    
    print(cb_md_df)
    print(stk_md_df)
    write_data('cb_md', 'bond', cb_md_df)
    write_data('stk_md', 'bond', stk_md_df)
        
def write_bond_daily_md_to_csv(ts_code):
    sql = "select * from bond.cb_md where ts_code = '{}' order by trade_date".format(ts_code)
    cb_md_df = read_postgre_data(sql)
    cb_md_df.columns = ['ts_code','stk_code','trade_date','cb_open','cb_high','cb_low','cb_close','cb_change','cb_pct_chg','cb_vol','cb_amount','cb_remain_size','cb_turn_over']
    stk_code = cb_md_df.loc[0]['stk_code']
    
    sql = "select * from bond.stk_md where ts_code = '{}'".format(stk_code)
    stk_md_df = read_postgre_data(sql)
    
    cb_md_df.insert(len(cb_md_df.columns), 'stk_open', 0)
    cb_md_df.insert(len(cb_md_df.columns), 'stk_high', 0)
    cb_md_df.insert(len(cb_md_df.columns), 'stk_low', 0)
    cb_md_df.insert(len(cb_md_df.columns), 'stk_close', 0)
    cb_md_df.insert(len(cb_md_df.columns), 'stk_change', 0)
    cb_md_df.insert(len(cb_md_df.columns), 'stk_pct_chg', 0)
    cb_md_df.insert(len(cb_md_df.columns), 'stk_vol', 0)
    cb_md_df.insert(len(cb_md_df.columns), 'stk_amount', 0)
    cb_md_df.insert(len(cb_md_df.columns), 'stk_turn_over', 0)
    for i in range(0, len(cb_md_df)):
        trade_date = cb_md_df.loc[i]['trade_date']
        stk_data_df = stk_md_df[stk_md_df.trade_date == trade_date].copy()
        stk_data_df.reset_index(drop=True, inplace=True)
        cb_md_df.loc[i, 'stk_open'] = stk_data_df.loc[0]['open']
        cb_md_df.loc[i, 'stk_high'] = stk_data_df.loc[0]['high']
        cb_md_df.loc[i, 'stk_low'] = stk_data_df.loc[0]['low']
        cb_md_df.loc[i, 'stk_close'] = stk_data_df.loc[0]['close']
        cb_md_df.loc[i, 'stk_change'] = stk_data_df.loc[0]['change']
        cb_md_df.loc[i, 'stk_pct_chg'] = stk_data_df.loc[0]['pct_change']
        cb_md_df.loc[i, 'stk_vol'] = stk_data_df.loc[0]['vol']
        cb_md_df.loc[i, 'stk_amount'] = stk_data_df.loc[0]['amount']
        cb_md_df.loc[i, 'stk_turn_over'] = stk_data_df.loc[0]['turn_over']
    
    cb_md_df.to_csv("./temp/{}.csv".format(ts_code), index=False)

def read_csv_test():
    reader = pd.read_csv("./temp/Level1MD20240308.csv", chunksize=5000)
    for chunk in reader:
        print(chunk)
        chunk.to_sql('md_test', postgre_engine_ts, 'bond', index=False, if_exists='append', chunksize=5000)

if __name__ == '__main__':
    # 登录 Tushare 接口
    pro = ts.pro_api(token)
    
    update_daily_data()
    # get_all_fut_cal_date()
    
    # start_date = '20220101'
    # end_date = '20240308'
    # bond_list = ['127056.SZ', '123218.SZ', '123025.SZ', '127033.SZ', '123205.SZ', '113052.SH',
    #              '123018.SZ', '110044.SH', '128041.SZ', '113044.SH', '123230.SZ', '113678.SH',
    #              '127081.SZ', '127098.SZ', '113594.SH', '113672.SH']
    # for ts_code in bond_list:
    #     bond_daily_md_proc(start_date, end_date, ts_code)
    
    # bond_list = ['127056.SZ', '123218.SZ', '123025.SZ', '127033.SZ', '123205.SZ', '113052.SH',
    #              '123018.SZ', '110044.SH', '128041.SZ', '113044.SH', '123230.SZ', '113678.SH',
    #              '127081.SZ', '127098.SZ', '113594.SH', '113672.SH']
    # for ts_code in bond_list:
    #     write_bond_daily_md_to_csv(ts_code)