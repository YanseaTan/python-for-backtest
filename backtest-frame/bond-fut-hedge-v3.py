# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-02-22
# @Last Modified by:   Yansea
# @Last Modified time: 2024-02-22

from turtle import position
from numpy import NaN
import pandas as pd
import xlwings as xw
import datetime
import time
import os
from copy import deepcopy
from sqlalchemy import create_engine
import sys
sys.path.append('./backtest-frame/api/')
from api.BackTestApi import *

# 策略参数
DEFAULT_VALUE = 9999999

setting_data = pd.DataFrame()

acct_id = 'default'
init_fund = 0
start_date = '20190101'
end_date = '20240101'
alter_period = 0

fut_name = ''
fut_code = ''
fut_multiplier = 0
margin_rate = 0
margin_redundancy = 0

yield_low = -DEFAULT_VALUE
yield_high = DEFAULT_VALUE
close_low = -DEFAULT_VALUE
close_high = DEFAULT_VALUE
vol_low = -DEFAULT_VALUE
vol_high = DEFAULT_VALUE

hedge_over_rate_1 = DEFAULT_VALUE
hedge_over_rate_2 = DEFAULT_VALUE
hedge_rate_1 = DEFAULT_VALUE
hedge_rate_2 = DEFAULT_VALUE

# 过程参数
total_days = 0
cal_date_list = []
bond_daily_md_df = pd.DataFrame()
fut_daily_md_df = pd.DataFrame()

# 读取策略参数
def read_config(file_path):
    print("读取设置文件...")
    global acct_id
    global init_fund
    global start_date
    global end_date
    global alter_period
    global fut_name
    global fut_code
    global fut_multiplier
    global margin_rate
    global margin_redundancy
    global yield_low
    global yield_high
    global close_low
    global close_high
    global vol_low
    global vol_high
    global hedge_over_rate_1
    global hedge_over_rate_2
    global hedge_rate_1
    global hedge_rate_2
    
    global setting_data
    setting_data = pd.read_excel(file_path)
    setting_data = pd.DataFrame(setting_data)
    
    app = xw.App(visible = False, add_book = False)
    app.display_alerts = False
    app.screen_updating = False
    workbook = app.books.open(file_path)
    ws = workbook.sheets.active

    acct_id = str(ws.range('A3').value)
    init_fund = ws.range('B3').value * 10000
    start_date = str(ws.range('C3').value)[:8]
    end_date = str(ws.range('D3').value)[:8]
    if len(start_date) < 8 or len(end_date) < 8 or start_date >= end_date:
        return -1
    alter_period = int(ws.range('E3').value)
    if init_fund <= 0 or alter_period <= 0:
        return -1
    
    fut_name = str(ws.range('A7').value)
    fut_code = str(ws.range('B7').value)
    if fut_name == '' or fut_code == '':
        return -1
    fut_multiplier = ws.range('C7').value
    margin_rate = ws.range('D7').value
    margin_redundancy = 1 - ws.range('E7').value
    if fut_multiplier == 0 or margin_rate == 0 or margin_redundancy == 0:
        return -1

    if ws.range('B11').value != None:
        yield_low = max(ws.range('B11').value, yield_low)
    if ws.range('B12').value != None:
        yield_high = min(ws.range('B12').value, yield_high)
    if ws.range('C11').value != None:
        close_low = max(ws.range('C11').value, close_low)
    if ws.range('C12').value != None:
        close_high = min(ws.range('C12').value, close_high)
    if ws.range('D11').value != None:
        vol_low = max(ws.range('D11').value, vol_low)
    if ws.range('D12').value != None:
        vol_high = min(ws.range('D12').value, vol_high)
    if yield_low >= yield_high or close_low >= close_high or vol_low >= vol_high:
        return -1

    hedge_over_rate_1 = ws.range('A16').value
    hedge_over_rate_2 = ws.range('A17').value
    hedge_rate_1 = ws.range('B16').value
    hedge_rate_2 = ws.range('B17').value
    if hedge_over_rate_1 == DEFAULT_VALUE or hedge_over_rate_2 == DEFAULT_VALUE or hedge_rate_1 == DEFAULT_VALUE or hedge_rate_2 == DEFAULT_VALUE:
        return -1
    
    workbook.close()
    app.quit()
    return 0

# 更具筛选条件获取指定交易日的代码列表，列表末位为股指期货合约
def filter_code_list(trade_date, position_df):
    global total_days
    total_days += 1
    code_list = []
    
    global bond_daily_md_df
    global fut_daily_md_df
    bond_md_df = bond_daily_md_df[(bond_daily_md_df.trade_date == trade_date)].copy()
    fut_md_df = fut_daily_md_df[(fut_daily_md_df.trade_date == trade_date)].copy()
    
    if alter_period == 1 or (total_days % alter_period) == 1:
        bond_code_df = bond_md_df[((bond_md_df.yield_to_maturity >= yield_low) & (bond_md_df.yield_to_maturity <= yield_high) &
                                   (bond_md_df.close >= close_low) & (bond_md_df.close <= close_high) &
                                   (bond_md_df.vol >= vol_low) & (bond_md_df.vol <= vol_high))]
        code_list = bond_code_df['ts_code'].tolist()
    else:
        code_list = position_df['ts_code'].tolist()
        code_list.pop(len(code_list) - 1)
    
    fut_md_df.sort_values(by='vol', ascending=False, inplace=True)
    fut_md_df.reset_index(drop=True, inplace=True)
    code_list.append(fut_md_df.loc[0]['ts_code'])
        
    return code_list

# 根据资金情况以及所选合约计算详细仓位
def calculate_position_dict(last_trade_date, trade_date, code_list):
    fund_df = get_fund_data(acct_id, last_trade_date)
    asset = fund_df.loc[0]['asset']
    
    global bond_daily_md_df
    global fut_daily_md_df
    bond_md_df = bond_daily_md_df[(bond_daily_md_df.trade_date == trade_date)].copy()
    fut_md_df = fut_daily_md_df[(fut_daily_md_df.trade_date == trade_date)].copy()
    
    # 根据全市场平均转股溢价率计算对冲比例
    cb_over_list = bond_md_df['cb_over_rate'].tolist()
    cb_over_mean = sum(cb_over_list) / len(cb_over_list)
    if cb_over_mean <= hedge_over_rate_1:
        hedge_rate = hedge_rate_1
    elif cb_over_mean >= hedge_over_rate_2:
        hedge_rate = hedge_rate_2
    else:
        hedge_rate = hedge_rate_1 + (hedge_rate_2 - hedge_rate_1) / (hedge_over_rate_2 - hedge_over_rate_1) * cb_over_mean
    
    # 排除当前交易日已经到期的代码（因为筛选是依靠上一交易日的数据）
    for code in code_list:
        if bond_md_df['ts_code'].value_counts(code)[0] == 0:
            code_list.remove(code)
    
    bond_fund = asset / (1 + margin_rate * hedge_rate)
    per_fund = bond_fund / (len(code_list) - 1)
    fut_fund = asset - bond_fund
    
    position_dict = {}
    for i in range(0, len(code_list) - 1):
        code = code_list[i]
        code_df = bond_md_df[bond_md_df.ts_code == code].copy()
        code_df.reset_index(drop=True, inplace=True)
        price = code_df.loc[0]['amount'] * 1000 / code_df.loc[0]['vol']
        vol = int(per_fund / price)
        value_list = [vol, round(price, 2)]
        position_dict[code] = value_list
    
    code = code_list[len(code_list) - 1]
    code_df = fut_md_df[fut_md_df.ts_code == code].copy()
    code_df.reset_index(drop=True, inplace=True)
    price = code_df.loc[0]['amount'] * 10000 / code_df.loc[0]['vol'] / fut_multiplier
    vol = int(fut_fund * margin_redundancy / margin_rate / fut_multiplier / price)
    value_list = [vol, round(price, 2)]
    position_dict[code] = value_list
    
    return position_dict

# 计算持仓变化，生成交易指令
def calculate_order_list(trade_date, position_dict, position_df):
    order_list = []
    
    # 股指期货交易指令
    fut_ts_code = list(position_dict.keys())[len(position_dict) - 1]
    fut_value_list = position_dict.pop(fut_ts_code)
    fut_position_df = position_df[position_df.direction != 0].copy()
    fut_position_df.reset_index(drop=True, inplace=True)
    
    if len(fut_position_df) == 0:
        order = [fut_ts_code, fut_value_list[0] * fut_multiplier, DIRECTION_SELL, OPEN_CLOSE_OPEN, fut_value_list[1]]
        order_list.append(order)
    elif fut_position_df.loc[0]['ts_code'] != fut_ts_code:
        last_fut_ts_code = fut_position_df.loc[0]['ts_code']
        last_fut_vol = fut_position_df.loc[0]['vol']
        global fut_daily_md_df
        code_df = fut_daily_md_df[((fut_daily_md_df.trade_date == trade_date) & (fut_daily_md_df.ts_code == last_fut_ts_code))].copy()
        code_df.reset_index(drop=True, inplace=True)
        price = round(code_df.loc[0]['amount'] * 10000 / code_df.loc[0]['vol'] / fut_multiplier, 2)
        order = [last_fut_ts_code, last_fut_vol * fut_multiplier, DIRECTION_BUY, OPEN_CLOSE_CLOSE, price]
        order_list.append(order)
        order = [fut_ts_code, fut_value_list[0] * fut_multiplier, DIRECTION_SELL, OPEN_CLOSE_OPEN, fut_value_list[1]]
        order_list.append(order)
    else:
        fut_vol = fut_value_list[0] - fut_position_df.loc[0]['vol']
        if fut_vol > 0:
            order = [fut_ts_code, fut_vol * fut_multiplier, DIRECTION_SELL, OPEN_CLOSE_OPEN, fut_value_list[1]]
            order_list.append(order)
        elif fut_vol < 0:
            order = [fut_ts_code, -fut_vol * fut_multiplier, DIRECTION_BUY, OPEN_CLOSE_CLOSE, fut_value_list[1]]
            order_list.append(order)
            
    # 可转债多头交易指令
    for code, value_list in position_dict.items():
        if len(position_df) == 0 or position_df['ts_code'].value_counts(code)[0] == 0:
            order = [code, value_list[0], DIRECTION_BUY, OPEN_CLOSE_NONE, value_list[1]]
            order_list.append(order)
        else:
            bond_position_df = position_df[position_df.ts_code == code].copy()
            bond_position_df.reset_index(drop=True, inplace=True)
            vol = value_list[0] - bond_position_df.loc[0]['vol']
            if vol > 0:
                order = [code, vol, DIRECTION_BUY, OPEN_CLOSE_NONE, value_list[1]]
                order_list.append(order)
            elif vol < 0:
                order = [code, -vol, DIRECTION_SELL, OPEN_CLOSE_NONE, value_list[1]]
                order_list.append(order)
        
    for i in range(0, len(position_df)):
        code = position_df.loc[0]['ts_code']
        if code not in position_dict.keys():
            bond_position_df = position_df[position_df.ts_code == code].copy()
            bond_position_df.reset_index(drop=True, inplace=True)
            global bond_daily_md_df
            code_df = bond_daily_md_df[((bond_daily_md_df.trade_date == trade_date) & (bond_daily_md_df.ts_code == code))].copy()
            code_df.reset_index(drop=True, inplace=True)
            price = round(code_df.loc[0]['amount'] * 1000 / code_df.loc[0]['vol'], 2)
            order = [code, bond_position_df.loc[0]['vol'], DIRECTION_SELL, OPEN_CLOSE_NONE, price]
            order_list.append(order)
    
    return order_list
                

# 策略主线程
def main():    
    ret = read_config('./可转债-股指期货对冲回测框架设置-v3.xlsx')
    if ret != 0:
        print("设置读取错误，请检查设置文件！")
        exit(1)
    
    # 设置初始资金
    set_init_fund(acct_id, start_date, init_fund)
    
    # 获取交易日历以及行情数据
    global cal_date_list
    global bond_daily_md_df
    global fut_daily_md_df
    cal_date_list = get_cal_date_list(start_date, end_date)
    bond_daily_md_df = get_daily_md_data('bond', 'cb_daily_test', 'ts_code, trade_date, close, vol, amount, yield_to_maturity, cb_over_rate', start_date, end_date)
    fut_daily_md_df = get_daily_md_data('future', 'fut_daily', 'ts_code, trade_date, vol, amount, oi_chg', start_date, end_date)
    fut_daily_md_df = fut_daily_md_df[((fut_daily_md_df.ts_code.str.startswith(fut_code)) & (~fut_daily_md_df.oi_chg.isnull()))]
    
    # 时间驱动策略
    for i in range(0, len(cal_date_list)):
        last_trade_date = cal_date_list[i]
        trade_date = cal_date_list[i + 1]
        
        position_df = get_position_data(acct_id, last_trade_date)
        position_df.reset_index(drop=True, inplace=True)
        
        code_list = filter_code_list(last_trade_date, position_df)
        
        position_dict = calculate_position_dict(last_trade_date, trade_date, code_list)
        
        order_list = calculate_order_list(trade_date, position_dict, position_df)
        
        for order in order_list:
            place_order(acct_id, trade_date, order)
            
        add_fund_data(list(CurrentFund.values()))
        
        fund = get_fund_data(acct_id)
        print(fund)
        exit(1)
            
        # trade_data = get_trade_data(acct_id)
        # print(trade_data)
        # exit(1)
        
        
        
        
        
        
        
        
        
    
    


if __name__ == "__main__":
    main()
