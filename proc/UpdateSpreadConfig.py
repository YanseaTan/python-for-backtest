# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-10-18
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-15

import datetime
import tushare as ts
import pandas as pd
from sqlalchemy import create_engine
import json
import sys
sys.path.append('.')
from tools.DatabaseTools import *
import numpy as np
import matplotlib.pyplot as plt

# Tushare 账户 token
token = 'e59d203345b5dac84a150b2abb7b49dcb06b6c2abefa7bc49c06bea1'

# 从 postgre 中获取上一日所有合约组合的无风险价差
def get_safe_spread():
    today = datetime.date.today()
    strToday = today.strftime('%Y%m%d')
    last_trade_date_df = pro.trade_cal(**{"cal_date":strToday}, fields=["pretrade_date"])
    last_trade_date = last_trade_date_df.loc[0]['pretrade_date']
    last_trade_date = datetime.datetime.strptime(last_trade_date, '%Y%m%d').strftime('%Y-%m-%d')
    engine = create_engine('postgresql://postgres:shan3353@10.10.20.189:5432/future?sslmode=disable')
    safe_spread_df = pd.read_sql("SELECT ticker_n, ticker_f, product, safe_spread from future.safe_spread('{}', '{}')".format(last_trade_date, last_trade_date), con=engine)
    spread_type_list = []
    num = len(safe_spread_df)
    for i in range(0, num):
        spread_type = safe_spread_df.loc[i]['ticker_n'][-2:] + '-' + safe_spread_df.loc[i]['ticker_f'][-2:]
        spread_type_list.append(spread_type)
    safe_spread_df['spread_type'] = spread_type_list
    return safe_spread_df

# 更新价差配置文件
def update_spread_config():
    f = open('./config/productOps.json', 'r')
    content = f.read()
    ops_json = json.loads(content)
    f.close()
    
    safe_spread_df = get_safe_spread()
    
    for i in range(0, len(ops_json)):
        fut_code = ops_json[i]['ProductID']
        logout_month = ops_json[i]['WarrantsLogoutMonth']
        # fut_code = 'CJ'
        sql = "select distinct spread_type from fut_spread_daily where fut_code = '{}' order by spread_type".format(fut_code)
        spread_type_df = read_data('futures', sql)
        spread_type_list = []
        spread_price_list = []
        safe_spread_list = []
        for j in range(0, len(spread_type_df)):
            spread_type = spread_type_df.loc[j]['spread_type']
            # spread_type = '03-05'
            sql = "select distinct ts_code from fut_spread_daily where fut_code = '{}' and spread_type = '{}' order by ts_code".format(fut_code, spread_type)
            ts_code_df = read_data('futures', sql)
            num_of_trade_date = 0
            close_df = pd.DataFrame()
            for k in range(0, len(ts_code_df)):
                ts_code = ts_code_df.loc[k]['ts_code']
                sql = "select trade_date, close, vol from fut_spread_daily where ts_code = '{}' order by ts_code".format(ts_code)
                df = read_data('futures', sql)
                if k == 0:
                    num_of_trade_date = len(df)
                    if num_of_trade_date < 66:
                        break
                # 去除交割前一个月的数据（如果未达到日期则不去除）
                df.drop(df[df.index >= (num_of_trade_date - 22)].index, inplace=True)
                # 通过中位数剔除毛刺数据（当日成交量小于 1000 手则认为是毛刺）
                me = np.median(df['close'])
                mad = np.median(abs(df['close'] - me))
                up = me + (2*mad)
                down = me - (2*mad)
                df.drop(df[(((df.close < down) & (df.vol < 1000)) | ((df.close > up) & (df.vol < 1000)))].index, inplace=True)
                close_df = pd.concat([close_df, df])
            num = len(close_df)
            if num == 0:
                continue
            
            close_df.sort_values(by='close', ascending=True, inplace=True)
            close_df.reset_index(drop=True, inplace=True)
            # 可转抛计算底部 10% 区间的价差阈值，不可转抛计算底部 5% 区间的价差阈值
            high = close_df.loc[num - 1]['close']
            low = close_df.loc[0]['close']
            ins1 = int(spread_type[:2])
            ins2 = int(spread_type[3:])
            sub_month = set()
            while ins1 != ins2:
                sub_month.add(ins1)
                ins1 = (ins1 % 12) + 1
            if len(sub_month & set(logout_month)):
                rec_spread = round((low + (high - low) * 0.05), 1)
            else:
                rec_spread = round((low + (high - low) * 0.1), 1)
                
            df = safe_spread_df[(safe_spread_df['product'] == fut_code.upper()) & (safe_spread_df['spread_type'] == spread_type)]
            safe_spread = -999
            if (len(df)):
                df.reset_index(drop=True, inplace=True)
                if not pd.isna(df.loc[0]['safe_spread']):
                    safe_spread = round(df.loc[0]['safe_spread'], 2) * -1
                
            # print(close_df)
            # print(rec_spread)
            # exit(1)
            # print(close_df.loc[max(round(num * 0.1), 1) - 1]['close'])
            # close_df['vol'].plot()
            # plt.axhline(y=np.nanmean(close_df['vol'])/2)
            # plt.show()
            # exit(1)
            spread_type_list.append(spread_type_df.loc[j]['spread_type'])
            spread_price_list.append(rec_spread)
            safe_spread_list.append(safe_spread)
        ops_json[i]['SpreadType'] = spread_type_list
        ops_json[i]['RecPrice'] = spread_price_list
        ops_json[i]['SafeSpread'] = safe_spread_list
        print('{} 价差配置写入成功，文件更新进度：{}%'.format(fut_code, format((i + 1) / len(ops_json) * 100, '.2f')))
    
    f = open('./output/productOps.json', 'w')
    content = json.dumps(ops_json, indent=2)
    f.write(content)
    f.close()
    print('价差配置文件更新完毕！')

# 更新期货单腿合约价格配置文件
def update_single_leg_config():
    # 获取所有合约基本信息
    sql = "select ts_code, fut_code, list_date from future.fut_basic where list_date >= '20190101'"
    fut_basic_df = read_postgre_data(sql)
    fut_code_list = fut_basic_df['fut_code'].tolist()
    fut_code_list = list(set(fut_code_list))
    fut_code_list.sort()
    
    # 获取所有合约行情信息
    sql = "select ts_code, trade_date, close, vol from future.fut_daily where trade_date >= '20190101' and oi_chg is not NULL and close is not NULL order by trade_date"
    fut_md_df = read_postgre_data(sql)
    
    config_list = []
    
    for i in range(0, len(fut_code_list)):
        fut_code = fut_code_list[i]
        fut_config_dict = {}
        fut_config_dict['fut_code'] = fut_code
        ts_code_df = fut_basic_df[fut_basic_df.fut_code == fut_code].copy()
        ts_code_df.insert(len(ts_code_df.columns), 'month', '')
        ts_code_df.reset_index(drop=True, inplace=True)
        month_list = []
        # 为合约基础数据增加合约月份信息
        for j in range(0, len(ts_code_df)):
            ts_code = ts_code_df.loc[j]['ts_code']
            month = ts_code[:ts_code.index('.')][-2:]
            ts_code_df.loc[j, 'month'] = month
            month_list.append(month)
        ts_code_df.sort_values(by='month', ascending=True, inplace=True)
        ts_code_df.reset_index(drop=True, inplace=True)
        month_list = list(set(month_list))
        month_list.sort()
        
        # 按合约月份进行分类计算
        rec_price_dict = {}
        fut_config_dict['rec_price'] = rec_price_dict
        for j in range(0, len(month_list)):
            month = month_list[j]
            month_ts_code_df = ts_code_df[ts_code_df.month == month].copy()
            month_ts_code_list = month_ts_code_df['ts_code'].tolist()
            close_df = pd.DataFrame()
            for k in range(0, len(month_ts_code_list)):
                ts_code = month_ts_code_list[k]
                md_df = fut_md_df[fut_md_df.ts_code == ts_code].copy()
                if len(md_df) < 66:
                    continue
                md_df.reset_index(drop=True, inplace=True)
                # 去除交割前一个月的数据（如果未达到日期则不去除）
                md_df.drop(md_df[md_df.index >= (len(md_df) - 22)].index, inplace=True)
                # 通过中位数剔除毛刺数据（当日成交量小于 1000 手则认为是毛刺）
                me = np.median(md_df['close'])
                mad = np.median(abs(md_df['close'] - me))
                up = me + (2*mad)
                down = me - (2*mad)
                md_df.drop(md_df[(((md_df.close < down) | (md_df.close > up)) & (md_df.vol < 1000))].index, inplace=True)
                close_df = pd.concat([close_df, md_df])
            
            num = len(close_df)
            if num == 0:
                continue
            close_df.sort_values(by='close', ascending=True, inplace=True)
            close_df.reset_index(drop=True, inplace=True)
            # 可转抛计算底部 10% 区间的价差阈值，不可转抛计算底部 5% 区间的价差阈值
            high = close_df.loc[num - 1]['close']
            low = close_df.loc[0]['close']
            rec_price = round((low + (high - low) * 0.1), 1)
            rec_price_dict[month] = rec_price
        
        config_list.append(fut_config_dict)
        print('{} 推荐价格配置写入成功，文件更新进度：{}%'.format(fut_code, format((i + 1) / len(fut_code_list) * 100, '.2f')))
    
    f = open('./output/recPrice.json', 'w')
    content = json.dumps(config_list, indent=2)
    f.write(content)
    f.close()
    print('推荐价格配置文件更新完毕！')

def main():
    # update_spread_config()
    update_single_leg_config()

if __name__ == "__main__":
    # 登录 Tushare 接口
    pro = ts.pro_api(token)
    main()
