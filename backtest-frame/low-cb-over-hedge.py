# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-03-07
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-07

import pandas as pd
import xlwings as xw
import datetime
import time
import json
import os
import sys
sys.path.append('./backtest-frame/api/')
from api.BackTestApi import *

acct_id = 'hailong'
init_fund = 20000000
start_date = '20190101'
end_date = '20240229'
per_fund = 150000

fut_name = '中证500'
fut_code = 'IC'
fut_multiplier = 200
margin_rate = 0.2

cb_over_mean_1 = 20
cb_over_mean_2 = 60
hedge_rate_1 = 0.7
hedge_rate_2 = 0.3

black_list_dict = {}
black_list = []

# 过程参数
cal_date_list = []
bond_daily_md_df = pd.DataFrame()
fut_daily_md_df = pd.DataFrame()
index_daily_md_df = pd.DataFrame()
fut_diff_rate_dict = {}

# 测试参数
issue_size_level_1 = 1000000000
issue_size_level_2 = 5000000000
issue_size_level_3 = 10000000000
buy_cb_over_level_1 = 15
buy_cb_over_level_2 = 10
buy_cb_over_level_3 = 5
buy_cb_over_level_4 = 2
sell_cb_over_level_1 = 30
sell_cb_over_level_2 = 25
sell_cb_over_level_3 = 20
sell_cb_over_level_4 = 10
buy_cb_over_dict = {}
sell_cb_over_dict = {}
issue_size_code_set_1 = set()
issue_size_code_set_2 = set()
issue_size_code_set_3 = set()
issue_size_code_set_4 = set()
max_buy_price = 150
max_len_of_single_code_set = 50
highest_price_dict = {}
max_drawdown = 0.15

setting_data = pd.DataFrame(columns=['init_fund', 'start_date', 'end_date', 'max_buy_price', 'fut_code', 'fut_multiplier', 'margin_rate', 'cb_over_mean_1', 'hedge_rate_1', 'cb_over_mean_2', 'hedge_rate_2'])
setting_data.loc[0] = [init_fund, start_date, end_date, max_buy_price, fut_code, fut_multiplier, margin_rate, cb_over_mean_1, hedge_rate_1, cb_over_mean_2, hedge_rate_2]

# 计算股指期货季连年华升贴水率
def calculate_fut_diff_rate_dict():
    print('计算股指期货季连年化升贴水率...')
    global cal_date_list
    global fut_daily_md_df
    global index_daily_md_df
    fut_diff_rate_dict = {}
    for i in range(0, len(cal_date_list) - 2):
        last_trade_date = cal_date_list[i]
        trade_date = cal_date_list[i + 1]
        
        fut_md_df = fut_daily_md_df[(fut_daily_md_df.trade_date == last_trade_date)].copy()
        
        fut_md_df.sort_values(by='ts_code', ascending=True, inplace=True)
        fut_md_df.reset_index(drop=True, inplace=True)
        fut_ts_code = fut_md_df.loc[2]['ts_code']
        fut_md_df = fut_daily_md_df[((fut_daily_md_df.trade_date == trade_date) & (fut_daily_md_df.ts_code == fut_ts_code))].copy()
        fut_md_df.reset_index(drop=True, inplace=True)
        fut_clsoe = fut_md_df.loc[0]['close']
            
        days = calculate_remain_days(fut_ts_code, trade_date)
            
        index_md_df = index_daily_md_df[index_daily_md_df.update_date == trade_date].copy()
        index_md_df.reset_index(drop=True, inplace=True)
        index_close = index_md_df.loc[0]['value']
        
        fut_diff = index_close - fut_clsoe
        value = round(fut_diff * 250 * 100 / index_close / days, 2)
        fut_diff_rate_dict[trade_date] = value
        
    return fut_diff_rate_dict

# 根据可转债发行规模定制不同可转债的开平仓条件
def calculate_limit_by_issue_size():
    print('根据可转债规模定制开平仓规则...')
    global buy_cb_over_dict
    global sell_cb_over_dict
    global issue_size_code_set_1
    global issue_size_code_set_2
    global issue_size_code_set_3
    global issue_size_code_set_4
    sql = "select ts_code, issue_size from bond.cb_basic"
    issue_df = read_postgre_data(sql)
    
    global bond_daily_md_df
    bond_daily_md_df.insert(len(bond_daily_md_df.columns), 'issue_size', 0)
    
    for i in range(0, len(issue_df)):
        code = issue_df.loc[i]['ts_code']
        issue_size = issue_df.loc[i]['issue_size']
        bond_daily_md_df.loc[bond_daily_md_df.ts_code == code, 'issue_size'] = issue_size
        if issue_size <= issue_size_level_1:
            buy_cb_over_dict[code] = buy_cb_over_level_1
            sell_cb_over_dict[code] = sell_cb_over_level_1
            issue_size_code_set_1.add(code)
        elif issue_size > issue_size_level_1 and issue_size <= issue_size_level_2:
            buy_cb_over_dict[code] = buy_cb_over_level_2
            sell_cb_over_dict[code] = sell_cb_over_level_2
            issue_size_code_set_2.add(code)
        elif issue_size > issue_size_level_2 and issue_size <= issue_size_level_3:
            buy_cb_over_dict[code] = buy_cb_over_level_3
            sell_cb_over_dict[code] = sell_cb_over_level_3
            issue_size_code_set_3.add(code)
        else:
            buy_cb_over_dict[code] = buy_cb_over_level_4
            sell_cb_over_dict[code] = sell_cb_over_level_4
            issue_size_code_set_4.add(code)

# 更具筛选条件获取指定交易日的代码列表，列表末位为股指期货合约
def filter_code_list(last_trade_date, trade_date, next_trade_date, position_df):
    code_list = []
    remove_code_set = set()
    sub_code_set_1 = set()
    sub_code_set_2 = set()
    sub_code_set_3 = set()
    sub_code_set_4 = set()
    
    global bond_daily_md_df
    global fut_daily_md_df
    bond_md_df = bond_daily_md_df[(bond_daily_md_df.trade_date == last_trade_date)].copy()
    now_bond_md_df = bond_daily_md_df[(bond_daily_md_df.trade_date == trade_date)].copy()
    next_bond_md_df = bond_daily_md_df[(bond_daily_md_df.trade_date == next_trade_date)].copy()
    fut_md_df = fut_daily_md_df[(fut_daily_md_df.trade_date == last_trade_date)].copy()
    
    for i in range(1, len(position_df)):
        code = position_df.loc[i]['ts_code']
        code_df = bond_md_df[bond_md_df.ts_code == code].copy()
        code_df.reset_index(drop=True, inplace=True)
        now_code_df = now_bond_md_df[now_bond_md_df.ts_code == code].copy()
        now_code_df.reset_index(drop=True, inplace=True)
        next_code_df = next_bond_md_df[next_bond_md_df.ts_code == code].copy()
        next_code_df.reset_index(drop=True, inplace=True)
        highest_price = highest_price_dict[code]
        close = code_df.loc[0]['close']
        cb_over_rate = code_df.loc[0]['cb_over_rate']
        if len(now_code_df) == 0 or now_code_df.loc[0]['vol'] == 0 or len(next_code_df) == 0 or next_code_df.loc[0]['vol'] == 0 or\
        cb_over_rate > sell_cb_over_dict[code] or (highest_price - close) / highest_price > max_drawdown:
            remove_code_set.add(code)
        else:
            if code in issue_size_code_set_1:
                sub_code_set_1.add(code)
            elif code in issue_size_code_set_2:
                sub_code_set_2.add(code)
            elif code in issue_size_code_set_3:
                sub_code_set_3.add(code)
            else:
                sub_code_set_4.add(code)
    
    bond_code_df = bond_md_df[((bond_md_df.close <= max_buy_price) & (bond_md_df.cb_over_rate <= buy_cb_over_level_1) & (bond_md_df.vol >= 0) & (bond_md_df.issue_size <= issue_size_level_1))].copy()
    sub_code_list_1 = bond_code_df['ts_code'].tolist()
    union_set = (sub_code_set_1 | (set(sub_code_list_1) - remove_code_set))
    if len(union_set) > max_len_of_single_code_set:
        bond_code_df.sort_values(by='cb_over_rate', ascending=True, inplace=True)
        sub_code_list_1 = bond_code_df['ts_code'].tolist()
        for code in sub_code_list_1:
            if len(sub_code_set_1) >= max_len_of_single_code_set:
                break
            if code not in remove_code_set:
                sub_code_set_1.add(code)
        sub_code_list_1 = list(sub_code_set_1)
    else:
        sub_code_list_1 = list(union_set)
    
    bond_code_df = bond_md_df[((bond_md_df.close <= max_buy_price) & (bond_md_df.cb_over_rate <= buy_cb_over_level_2) & (bond_md_df.vol >= 0) & (bond_md_df.issue_size > issue_size_level_1) & (bond_md_df.issue_size <= issue_size_level_2))].copy()
    sub_code_list_2 = bond_code_df['ts_code'].tolist()
    union_set = (sub_code_set_2 | (set(sub_code_list_2) - remove_code_set))
    if len(union_set) > max_len_of_single_code_set:
        bond_code_df.sort_values(by='cb_over_rate', ascending=True, inplace=True)
        sub_code_list_2 = bond_code_df['ts_code'].tolist()
        for code in sub_code_list_2:
            if len(sub_code_set_2) >= max_len_of_single_code_set:
                break
            if code not in remove_code_set:
                sub_code_set_2.add(code)
        sub_code_list_2 = list(sub_code_set_2)
    else:
        sub_code_list_2 = list(union_set)
    
    bond_code_df = bond_md_df[((bond_md_df.close <= max_buy_price) & (bond_md_df.cb_over_rate <= buy_cb_over_level_3) & (bond_md_df.vol >= 0) & (bond_md_df.issue_size > issue_size_level_2) & (bond_md_df.issue_size <= issue_size_level_3))].copy()
    sub_code_list_3 = bond_code_df['ts_code'].tolist()
    union_set = (sub_code_set_3 | (set(sub_code_list_3) - remove_code_set))
    if len(union_set) > max_len_of_single_code_set:
        bond_code_df.sort_values(by='cb_over_rate', ascending=True, inplace=True)
        sub_code_list_3 = bond_code_df['ts_code'].tolist()
        for code in sub_code_list_3:
            if len(sub_code_set_3) >= max_len_of_single_code_set:
                break
            if code not in remove_code_set:
                sub_code_set_3.add(code)
        sub_code_list_3 = list(sub_code_set_3)
    else:
        sub_code_list_3 = list(union_set)
        
    bond_code_df = bond_md_df[((bond_md_df.close <= max_buy_price) & (bond_md_df.cb_over_rate <= buy_cb_over_level_4) & (bond_md_df.vol >= 0) & (bond_md_df.issue_size > issue_size_level_3))].copy()
    sub_code_list_4 = bond_code_df['ts_code'].tolist()
    union_set = (sub_code_set_4 | (set(sub_code_list_4) - remove_code_set))
    if len(union_set) > max_len_of_single_code_set:
        bond_code_df.sort_values(by='cb_over_rate', ascending=True, inplace=True)
        sub_code_list_4 = bond_code_df['ts_code'].tolist()
        for code in sub_code_list_4:
            if len(sub_code_set_4) >= max_len_of_single_code_set:
                break
            if code not in remove_code_set:
                sub_code_set_4.add(code)
        sub_code_list_4 = list(sub_code_set_4)
    else:
        sub_code_list_4 = list(union_set)
    
        
    code_list = sub_code_list_1 + sub_code_list_2 + sub_code_list_3 + sub_code_list_4
    
    # 排除当前以及下一个交易日已经到期或无交易量的代码，以及当日收益率不满足要求和黑名单中的代码
    for i in range(0, len(code_list)):
        code = code_list[i]
        code_df = bond_md_df[bond_md_df.ts_code == code].copy()
        code_df.reset_index(drop=True, inplace=True)
        now_code_df = now_bond_md_df[now_bond_md_df.ts_code == code].copy()
        now_code_df.reset_index(drop=True, inplace=True)
        next_code_df = next_bond_md_df[next_bond_md_df.ts_code == code].copy()
        next_code_df.reset_index(drop=True, inplace=True)
        if len(now_code_df) == 0 or now_code_df.loc[0]['vol'] == 0 or len(next_code_df) == 0 or next_code_df.loc[0]['vol'] == 0 or code in black_list:
            remove_code_set.add(code)
    code_list = list(set(code_list) - remove_code_set)
    
    # 筛选股指期货
    fut_md_df.sort_values(by='oi', ascending=False, inplace=True)
    fut_md_df.reset_index(drop=True, inplace=True)
    fut_ts_code = fut_md_df.loc[0]['ts_code']
    # 检查合约代码在当前以及下一个交易日是否存在交易
    new_fut_md_df = fut_daily_md_df[(fut_daily_md_df.trade_date == trade_date)].copy()
    next_fut_md_df = fut_daily_md_df[(fut_daily_md_df.trade_date == next_trade_date)].copy()
    code_df = new_fut_md_df[new_fut_md_df.ts_code == fut_ts_code].copy()
    code_df.reset_index(drop=True, inplace=True)
    next_code_df = next_fut_md_df[next_fut_md_df.ts_code == fut_ts_code].copy()
    next_code_df.reset_index(drop=True, inplace=True)
    if len(code_df) == 0 or code_df.loc[0]['vol'] == 0 or len(next_code_df) == 0 or next_code_df.loc[0]['vol'] == 0:
        fut_ts_code = fut_md_df.loc[1]['ts_code']
    code_list.append(fut_ts_code)
    
    return code_list

# 更新持仓浮盈情况
def update_position_profit(trade_date, position_df, remove_code_list):
    if len(position_df) == 0:
        return
    
    global bond_daily_md_df
    global fut_daily_md_df
    bond_md_df = bond_daily_md_df[(bond_daily_md_df.trade_date == trade_date)].copy()
    fut_md_df = fut_daily_md_df[(fut_daily_md_df.trade_date == trade_date)].copy()
    
    # 股指期货部分
    last_fut_ts_code = position_df.loc[0]['ts_code']
    if last_fut_ts_code not in remove_code_list:
        last_fut_vol = position_df.loc[0]['vol']
        last_fut_price = position_df.loc[0]['open_price']
        code_df = fut_md_df[fut_md_df.ts_code == last_fut_ts_code].copy()
        code_df.reset_index(drop=True, inplace=True)
        fut_price = code_df.loc[0]['amount'] * 10000 / code_df.loc[0]['vol'] / fut_multiplier
        position_profit = -round((fut_price - last_fut_price) * last_fut_vol * fut_multiplier, 2)
        add_position_data(acct_id, trade_date, last_fut_ts_code, last_fut_vol, DIRECTION_SELL, last_fut_price, position_profit)
        CurrentFund['position_profit'] += position_profit
    
    # 可转债部分
    for i in range(1, len(position_df)):
        last_ts_code = position_df.loc[i]['ts_code']
        if last_ts_code not in remove_code_list:
            last_vol = position_df.loc[i]['vol']
            last_price = position_df.loc[i]['open_price']
            code_df = bond_md_df[bond_md_df.ts_code == last_ts_code].copy()
            code_df.reset_index(drop=True, inplace=True)
            price = round(code_df.loc[0]['amount'] * 1000 / code_df.loc[0]['vol'], 2)
            position_profit = round((price - last_price) * last_vol, 2)
            add_position_data(acct_id, trade_date, last_ts_code, last_vol, DIRECTION_BUY, last_price, position_profit)
            CurrentFund['position_profit'] += position_profit
            highest_price_dict[last_ts_code] = max(price, highest_price_dict[last_ts_code])

# 更新可转债平仓盈亏情况（周期内仅有部分合约发生变动时不调整其他持仓，仅进行可转债平仓操作）
def calculate_bond_sell_order_list(trade_date, position_df, remove_code_list):
    global bond_daily_md_df
    bond_md_df = bond_daily_md_df[(bond_daily_md_df.trade_date == trade_date)].copy()
    
    order_list = []
    for code in remove_code_list:
        bond_position_df = position_df[position_df.ts_code == code].copy()
        bond_position_df.reset_index(drop=True, inplace=True)
        last_vol = bond_position_df.loc[0]['vol']
        last_price = bond_position_df.loc[0]['open_price']
        code_df = bond_md_df[bond_md_df.ts_code == code].copy()
        code_df.reset_index(drop=True, inplace=True)
        price = round(code_df.loc[0]['amount'] * 1000 / code_df.loc[0]['vol'], 2)
        close_profit = round((price - last_price) * last_vol, 2)
        CurrentFund['close_profit'] += close_profit
        order = [code, last_vol, DIRECTION_SELL, OPEN_CLOSE_NONE, price, close_profit]
        order_list.append(order)
        
    return order_list

# 根据资金情况以及所选合约计算详细仓位
def calculate_position_dict(last_trade_date, trade_date, code_list):
    fund_df = get_fund_data(acct_id, last_trade_date)
    fund_df.reset_index(drop=True, inplace=True)
    asset = fund_df.loc[0]['asset']
    available = fund_df.loc[0]['available']
    
    global bond_daily_md_df
    global fut_daily_md_df
    bond_md_df = bond_daily_md_df[(bond_daily_md_df.trade_date == trade_date)].copy()
    fut_md_df = fut_daily_md_df[(fut_daily_md_df.trade_date == trade_date)].copy()
    
    # 根据全市场平均转股溢价率计算对冲比例
    cb_over_list = bond_md_df['cb_over_rate'].tolist()
    cb_over_mean = sum(cb_over_list) / len(cb_over_list)
    if cb_over_mean <= cb_over_mean_1:
        hedge_rate = hedge_rate_1
    elif cb_over_mean >= cb_over_mean_2:
        hedge_rate = hedge_rate_2
    else:
        hedge_rate = hedge_rate_1 + (hedge_rate_2 - hedge_rate_1) * (cb_over_mean - cb_over_mean_1) / (cb_over_mean_2 - cb_over_mean_1)
    
    # 根据股指期货季连合约年化升贴水率修正对冲比例
    global fut_diff_rate_dict
    fut_diff_rate = fut_diff_rate_dict[trade_date]
    
    # 按比例计算
    # if fut_diff_rate <= fut_diff_1:
    #     hedge_rate += hedge_rate_diff_1
    # elif fut_diff_rate >= fut_diff_2:
    #     hedge_rate += hedge_rate_diff_2
    # else:
    #     hedge_rate += hedge_rate_diff_1 + (hedge_rate_diff_2 - hedge_rate_diff_1) * (fut_diff_rate - fut_diff_1) / (fut_diff_2 - fut_diff_1)
    
    # 按极值计算
    if fut_diff_rate >= 10:
        hedge_rate -= 0.1
    
    global per_fund
    bond_fund = asset - available + per_fund * (len(code_list) - 1)
    fut_fund = bond_fund * hedge_rate
    
    position_dict = {}
    for i in range(0, len(code_list) - 1):
        code = code_list[i]
        code_df = bond_md_df[bond_md_df.ts_code == code].copy()
        code_df.reset_index(drop=True, inplace=True)
        price = code_df.loc[0]['amount'] * 1000 / code_df.loc[0]['vol']
        vol = int(per_fund / price)
        vol -= vol % 10
        value_list = [vol, round(price, 2)]
        position_dict[code] = value_list
    
    code = code_list[len(code_list) - 1]
    code_df = fut_md_df[fut_md_df.ts_code == code].copy()
    code_df.reset_index(drop=True, inplace=True)
    price = code_df.loc[0]['amount'] * 10000 / code_df.loc[0]['vol'] / fut_multiplier
    vol = int(fut_fund / fut_multiplier / price)
    if vol == 0:
        vol = 1
    value_list = [vol, round(price, 2)]
    position_dict[code] = value_list
    
    return position_dict

# 计算持仓变化，生成交易指令
def calculate_order_list(trade_date, position_dict, position_df):
    order_list = []
    
    # 股指期货交易指令
    fut_ts_code = list(position_dict.keys())[len(position_dict) - 1]
    fut_value_list = position_dict.pop(fut_ts_code)
    fut_vol = fut_value_list[0]
    fut_price = fut_value_list[1]
    fut_position_df = position_df[position_df.direction != 0].copy()
    fut_position_df.reset_index(drop=True, inplace=True)
    if len(fut_position_df) != 0:
        last_fut_ts_code = fut_position_df.loc[0]['ts_code']
        last_fut_vol = fut_position_df.loc[0]['vol']
        last_fut_price = fut_position_df.loc[0]['open_price']
        if last_fut_ts_code != fut_ts_code:
            global fut_daily_md_df
            code_df = fut_daily_md_df[((fut_daily_md_df.trade_date == trade_date) & (fut_daily_md_df.ts_code == last_fut_ts_code))].copy()
            code_df.reset_index(drop=True, inplace=True)
            price = round(code_df.loc[0]['amount'] * 10000 / code_df.loc[0]['vol'] / fut_multiplier, 2)
            close_profit = -(price - last_fut_price) * last_fut_vol * fut_multiplier
            CurrentFund['close_profit'] += close_profit
            CurrentFund['available'] += close_profit
            order = [last_fut_ts_code, last_fut_vol, DIRECTION_BUY, OPEN_CLOSE_CLOSE, price, close_profit]
            order_list.append(order)
            order = [fut_ts_code, fut_vol, DIRECTION_SELL, OPEN_CLOSE_OPEN, fut_price, 0]
            order_list.append(order)
            add_position_data(acct_id, trade_date, fut_ts_code, fut_vol, DIRECTION_SELL, fut_price, 0)
        else:
            fut_vol_diff = fut_vol - last_fut_vol
            if fut_vol_diff > 0:
                order = [fut_ts_code, fut_vol_diff, DIRECTION_SELL, OPEN_CLOSE_OPEN, fut_price, 0]
                order_list.append(order)
                open_price = round(((last_fut_price * last_fut_vol) + (fut_price * fut_vol_diff)) / fut_vol, 2)
                position_profit = -round((fut_price - open_price) * fut_vol * fut_multiplier, 2)
                add_position_data(acct_id, trade_date, fut_ts_code, fut_vol, DIRECTION_SELL, open_price, position_profit)
                CurrentFund['position_profit'] += position_profit
            elif fut_vol_diff < 0:
                close_profit = (fut_price - last_fut_price) * fut_vol_diff * fut_multiplier
                CurrentFund['close_profit'] += close_profit
                CurrentFund['available'] += close_profit
                order = [fut_ts_code, -fut_vol_diff, DIRECTION_BUY, OPEN_CLOSE_CLOSE, fut_price, close_profit]
                order_list.append(order)
                position_profit = -round((fut_price - last_fut_price) * fut_vol * fut_multiplier, 2)
                add_position_data(acct_id, trade_date, fut_ts_code, fut_vol, DIRECTION_SELL, last_fut_price, position_profit)
                CurrentFund['position_profit'] += position_profit
            else:
                position_profit = -round((fut_price - last_fut_price) * fut_vol * fut_multiplier, 2)
                add_position_data(acct_id, trade_date, fut_ts_code, fut_vol, DIRECTION_SELL, last_fut_price, position_profit)
                CurrentFund['position_profit'] += position_profit
    else:
        order = [fut_ts_code, fut_vol, DIRECTION_SELL, OPEN_CLOSE_OPEN, fut_price, 0]
        order_list.append(order)
        add_position_data(acct_id, trade_date, fut_ts_code, fut_vol, DIRECTION_SELL, fut_price, 0)
    
    # 可转债多头交易指令
    for code, value_list in position_dict.items():
        vol = value_list[0]
        price = value_list[1]
        order = [code, vol, DIRECTION_BUY, OPEN_CLOSE_NONE, price, 0]
        order_list.append(order)
        add_position_data(acct_id, trade_date, code, vol, DIRECTION_BUY, price, 0)
        highest_price_dict[code] = price
    
    return order_list

def main():
    global black_list_dict
    f = open('./backtest-frame/black-list.json', 'r', encoding='utf-8')
    content = f.read()
    black_list_dict = json.loads(content)
    f.close()
    
    # 获取交易日历以及行情数据
    global cal_date_list
    global bond_daily_md_df
    global fut_daily_md_df
    global index_daily_md_df
    cal_date_list = get_cal_date_list(start_date, end_date)
    bond_daily_md_df = get_daily_md_data('bond', 'cb_daily_test', 'ts_code, trade_date, close, vol, amount, yield_to_maturity, cb_over_rate', start_date, end_date)
    fut_daily_md_df = get_daily_md_data('future', 'fut_daily', 'ts_code, trade_date, close, vol, amount, oi, oi_chg', start_date, end_date)
    fut_daily_md_df = fut_daily_md_df[((fut_daily_md_df.ts_code.str.startswith(fut_code)) & (fut_daily_md_df.ts_code.str.len() > 6))]
    sql = "select update_date, value from future.fut_funds where index_name = '{}' and update_date >= '{}' and update_date <= '{}'".format(fut_name, start_date, end_date)
    index_daily_md_df = read_postgre_data(sql)
    
    # 计算股指期货季连年华升贴水率
    global fut_diff_rate_dict
    fut_diff_rate_dict = calculate_fut_diff_rate_dict()
    
    # 根据可转债规模定制开平仓规则
    calculate_limit_by_issue_size()
    
    # 设置初始资金
    set_init_fund(acct_id, cal_date_list[0], init_fund)
    
    # 时间驱动策略
    last_code_list = []
    for i in range(0, len(cal_date_list) - 2):
        last_trade_date = cal_date_list[i]
        trade_date = cal_date_list[i + 1]
        next_trade_date = cal_date_list[i + 2]
        
        # 更新黑名单
        global black_list
        if trade_date in black_list_dict.keys():
            black_list += black_list_dict[trade_date]
        
        # 获取最新昨日持仓
        position_df = get_position_data(acct_id, last_trade_date)
        position_df.reset_index(drop=True, inplace=True)
        
        # 根据昨日市场数据以及昨日持仓，筛选今日可转债和期货合约，并根据当前以及下一交易日这些合约是否存在，若不存在进行剔除
        code_list = filter_code_list(last_trade_date, trade_date, next_trade_date, position_df)
        remove_code_list = []
        
        # 若筛选出的代码列表与上一交易日相同，则仅进行持仓盈亏的更新，不进行交易操作
        if code_list == last_code_list:
            update_position_profit(trade_date, position_df, [])
        else:
            # 将代码列表中减少的代码进行平仓操作
            remove_code_list = list(set(last_code_list[:-1]) - set(code_list[:-1]))
            order_list = calculate_bond_sell_order_list(trade_date, position_df, remove_code_list)
            
            # 根据今日市场数据确定今日新增可转债和期货合约的具体仓位
            add_code_list = list(set(code_list[:-1]) - set(last_code_list[:-1])) + code_list[-1:]
            position_dict = calculate_position_dict(last_trade_date, trade_date, add_code_list)
            fut_ts_code = list(position_dict.keys())[len(position_dict) - 1]
            fut_vol = position_dict[fut_ts_code][0]
            
            # 根据昨日持仓以及今日持仓计算得到今日的交易指令列表
            buy_order_list = calculate_order_list(trade_date, position_dict, position_df)
            order_list += buy_order_list
            
            # 将代码列表中保持的可转债代码进行持仓盈亏的更新
            update_position_profit(trade_date, position_df, remove_code_list + last_code_list[-1:])
            
            # 根据交易指令列表向柜台发出交易指令，更新【成交数据】，【资金数据】
            for order in order_list:
                place_order(acct_id, trade_date, order)
                
        # 更新账户资金数据
        CurrentFund['trade_date'] = trade_date
        last_fund = get_fund_data(acct_id, last_trade_date)
        last_fund.reset_index(drop=True, inplace=True)
        CurrentFund['asset'] = CurrentFund['asset'] + CurrentFund['close_profit'] + (CurrentFund['position_profit'] - last_fund.loc[0]['position_profit'])
        add_fund_data(list(CurrentFund.values()))
        
        print("交易日：{} | 总资金：{} | 可用资金：{} | 可转债标的数量：{} | 股指期货持仓手数：{} | 平仓盈亏：{} | 持仓盈亏：{} | 回测进度：{}%".format(trade_date, round(CurrentFund['asset'], 2),
              round(CurrentFund['available'], 2), len(code_list) - 1, fut_vol, round(CurrentFund['close_profit'], 2), round(CurrentFund['position_profit'], 2), round((i + 1) / (len(cal_date_list) - 2) * 100, 2)))
        CurrentFund['close_profit'] = 0
        CurrentFund['position_profit'] = 0
        last_code_list = code_list
    
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    timeStr = time.strftime('%H-%M-%S')
    if not os.path.exists('output/{}/'.format(todayStr)):
        os.makedirs('output/{}/'.format(todayStr))
    book_name = './output/{}/{}-{}-低溢价进攻选债-{}股指期货对冲净值回测-{}.xlsx'.format(todayStr, start_date, end_date, fut_name, timeStr)
    write_data_to_xlsx(book_name, setting_data)


if __name__ == "__main__":
    main()
