# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-02-22
# @Last Modified by:   Yansea
# @Last Modified time: 2024-02-24

import pandas as pd
import xlwings as xw
import datetime
import time
import os
from copy import deepcopy
from sqlalchemy import column, create_engine

OPEN_CLOSE_NONE = 0
OPEN_CLOSE_OPEN = 1
OPEN_CLOSE_CLOSE = 2

DIRECTION_BUY = 0
DIRECTION_SELL = 1

# 账户资金记录
FundData = pd.DataFrame(columns=['acct_id', 'trade_date', 'available', 'asset', 'close_profit', 'position_profit'])
CurrentFund = {"acct_id":'', "trade_date":'', "available":0, "asset":0, "close_profit":0, "position_profit":0}

# 账户成交记录
TradeData = pd.DataFrame(columns=['acct_id', 'trade_date', 'ts_code', 'vol', 'direction', 'open_close', 'price'])

# 账户持仓记录
PositionData = pd.DataFrame(columns=['acct_id', 'trade_date', 'ts_code', 'vol', 'direction', 'open_price', 'position_profit'])

# 服务器 postgre 数据库用户配置
postgre_user = 'postgres'
postgre_password = 'shan3353'
postgre_addr = '10.10.20.189:5432'
postgre_database = 'future'

# 创建 postgre 数据库操作引擎
postgre_engine_ts = create_engine('postgresql://{}:{}@{}/{}?sslmode=disable'.format(postgre_user, postgre_password, postgre_addr, postgre_database))

# 读取服务器数据库
def read_postgre_data(sql):
    df = pd.read_sql_query(sql, postgre_engine_ts)
    return df

# 获取交易日历
def get_cal_date_list(start_date, end_date):
    print("获取交易日历...")
    sql = "select distinct trade_date from bond.cb_daily_test where trade_date >= '{}' and trade_date <= '{}' order by trade_date".format(start_date, end_date)
    date_df = read_postgre_data(sql)
    cal_date_list = date_df['trade_date'].tolist()
    return cal_date_list

# 获取日行情数据
def get_daily_md_data(database_name, table_name, param_list, start_date, end_date):
    print("获取 {} 日行情数据...".format(database_name))
    sql = "select {} from {}.{} where trade_date >= '{}' and trade_date <= '{}'".format(param_list, database_name, table_name, start_date, end_date)
    daily_md_df = read_postgre_data(sql)
    return daily_md_df

# 账户资金相关
def set_init_fund(acct_id, trade_date, asset):
    CurrentFund['acct_id'] = acct_id
    CurrentFund['trade_date'] = trade_date
    CurrentFund['available'] = asset
    CurrentFund['asset'] = asset
    FundData.loc[0] = list(CurrentFund.values())
    
def add_fund_data(fund_list):
    FundData.loc[len(FundData)] = fund_list

def get_fund_data(acct_id, trade_date = ''):
    if trade_date == '':
        return FundData[FundData.acct_id == acct_id]
    else:
        return FundData[((FundData.acct_id == acct_id) & (FundData.trade_date == trade_date))]

# 账户成交相关
def add_trade_data(acct_id, trade_date, ts_code, vol, direction, open_close, price):
    TradeData.loc[len(TradeData)] = [acct_id, trade_date, ts_code, vol, direction, open_close, price]
    
def get_trade_data(acct_id, trade_date = ''):
    if trade_date == '':
        return TradeData[TradeData.acct_id == acct_id]
    else:
        return TradeData[((TradeData.acct_id == acct_id) & (TradeData.trade_date == trade_date))]

# 账户持仓相关
def add_position_data(acct_id, trade_date, ts_code, vol, direction, open_price, position_profit):
    PositionData.loc[len(PositionData)] = [acct_id, trade_date, ts_code, vol, direction, open_price, position_profit]

def get_position_data(acct_id, trade_date = ''):
    if trade_date == '':
        return PositionData[PositionData.acct_id == acct_id]
    else:
        return PositionData[((PositionData.acct_id == acct_id) & (PositionData.trade_date == trade_date))]


        


# 交易指令相关
# def buy(acct_id, trade_date, ts_code, vol, price, open_close)

def sell(acct_id, trade_date, ts_code, vol, price, open_close):
    add_trade_data(acct_id, trade_date, ts_code, vol, 0, open_close, price)

def place_order(acct_id, trade_date, order):
    ts_code = order[0]
    vol = order[1]
    direction = order[2]
    open_close = order[3]
    price = order[4]
    add_trade_data(acct_id, trade_date, ts_code, vol, direction, open_close, price)
    
    if open_close == OPEN_CLOSE_NONE:
        if direction == DIRECTION_BUY:
            CurrentFund['available'] -= price * vol
        elif direction == DIRECTION_SELL:
            CurrentFund['available'] += price * vol