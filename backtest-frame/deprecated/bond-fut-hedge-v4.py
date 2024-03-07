# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-02-22
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-04

import pandas as pd
import xlwings as xw
import datetime
import time
import json
import os
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

filter_yield_low = -DEFAULT_VALUE
filter_yield_high = DEFAULT_VALUE
filter_close_low = -DEFAULT_VALUE
filter_close_high = DEFAULT_VALUE
filter_vol_low = -DEFAULT_VALUE
filter_vol_high = DEFAULT_VALUE

cb_over_mean_1 = DEFAULT_VALUE
cb_over_mean_2 = DEFAULT_VALUE
hedge_rate_1 = DEFAULT_VALUE
hedge_rate_2 = DEFAULT_VALUE
fut_diff_1 = DEFAULT_VALUE
fut_diff_2 = DEFAULT_VALUE
hedge_rate_diff_1 = DEFAULT_VALUE
hedge_rate_diff_2 = DEFAULT_VALUE

close_close_low = -DEFAULT_VALUE
close_close_high = DEFAULT_VALUE

black_list_dict = {}
black_list = []

# 过程参数
total_days = 0
cal_date_list = []
bond_daily_md_df = pd.DataFrame()
fut_daily_md_df = pd.DataFrame()
index_daily_md_df = pd.DataFrame()
fut_diff_rate_dict = {}

# 测试参数
yield_to_maturity_dict = {}
close_level_dict = {}
highest_price_dict = {}

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
    global filter_yield_low
    global filter_yield_high
    global filter_close_low
    global filter_close_high
    global filter_vol_low
    global filter_vol_high
    global close_close_low
    global close_close_high
    global cb_over_mean_1
    global cb_over_mean_2
    global hedge_rate_1
    global hedge_rate_2
    global fut_diff_1
    global fut_diff_2
    global hedge_rate_diff_1
    global hedge_rate_diff_2
    global black_list
    
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
        filter_yield_low = max(ws.range('B11').value, filter_yield_low)
    if ws.range('B12').value != None:
        filter_yield_high = min(ws.range('B12').value, filter_yield_high)
    if ws.range('C11').value != None:
        filter_close_low = max(ws.range('C11').value, filter_close_low)
    if ws.range('C12').value != None:
        filter_close_high = min(ws.range('C12').value, filter_close_high)
    if ws.range('D11').value != None:
        filter_vol_low = max(ws.range('D11').value, filter_vol_low)
    if ws.range('D12').value != None:
        filter_vol_high = min(ws.range('D12').value, filter_vol_high)
    if filter_yield_low >= filter_yield_high or filter_close_low >= filter_close_high or filter_vol_low >= filter_vol_high:
        return -1

    cb_over_mean_1 = ws.range('A16').value
    cb_over_mean_2 = ws.range('A17').value
    hedge_rate_1 = ws.range('B16').value
    hedge_rate_2 = ws.range('B17').value
    fut_diff_1 = ws.range('C16').value
    fut_diff_2 = ws.range('C17').value
    hedge_rate_diff_1 = ws.range('D16').value
    hedge_rate_diff_2 = ws.range('D17').value
    
    if ws.range('B21').value != None:
        close_close_low = max(ws.range('B21').value, close_close_low)
    if ws.range('B22').value != None:
        close_close_high = min(ws.range('B22').value, close_close_high)
        
    workbook.close()
    app.quit()
    
    global black_list_dict
    f = open('./backtest-frame/black-list.json', 'r', encoding='utf-8')
    content = f.read()
    black_list_dict = json.loads(content)
    f.close()
    
    return 0

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
    global yield_to_maturity_dict
    global close_level_dict
    sql = "select ts_code, issue_size from bond.cb_basic"
    issue_df = read_postgre_data(sql)
    for i in range(0, len(issue_df)):
        code = issue_df.loc[i]['ts_code']
        issue_size = issue_df.loc[i]['issue_size']
        if issue_size <= 500000000:
            yield_to_maturity_dict[code] = 0.5
            close_level_dict[code] = 130
        elif issue_size > 500000000 and issue_size <= 1000000000:
            yield_to_maturity_dict[code] = 1.5
            close_level_dict[code] = 125
        else:
            yield_to_maturity_dict[code] = 2
            close_level_dict[code] = 120

# 更具筛选条件获取指定交易日的代码列表，列表末位为股指期货合约
def filter_code_list(last_trade_date, trade_date, next_trade_date, position_df):
    global total_days
    total_days += 1
    code_list = []
    remove_code_set = set()
    
    global bond_daily_md_df
    global fut_daily_md_df
    bond_md_df = bond_daily_md_df[(bond_daily_md_df.trade_date == last_trade_date)].copy()
    fut_md_df = fut_daily_md_df[(fut_daily_md_df.trade_date == last_trade_date)].copy()

    if alter_period == 1 or (total_days % alter_period) == 1:
        bond_code_df = bond_md_df[((bond_md_df.yield_to_maturity >= filter_yield_low) & (bond_md_df.yield_to_maturity <= filter_yield_high) &
                                    (bond_md_df.close >= filter_close_low) & (bond_md_df.close <= filter_close_high) &
                                    (bond_md_df.vol >= filter_vol_low) & (bond_md_df.vol <= filter_vol_high))]
        code_list = bond_code_df['ts_code'].tolist()
    else:
        code_list = position_df['ts_code'].tolist()
        code_list.pop(0)
        # 排除周期内触碰平仓设置的代码
        # for i in range(0, len(code_list)):
        #     code = code_list[i]
        #     code_df = bond_md_df[bond_md_df.ts_code == code].copy()
        #     code_df.reset_index(drop=True, inplace=True)
        #     if code_df.loc[0]['close'] <= close_close_low or code_df.loc[0]['close'] >= close_close_high:
        #         remove_code_set.add(code)
        
    # 检查合约代码在当前以及下一个交易日是否存在交易
    now_bond_md_df = bond_daily_md_df[(bond_daily_md_df.trade_date == trade_date)].copy()
    next_bond_md_df = bond_daily_md_df[(bond_daily_md_df.trade_date == next_trade_date)].copy()
    
    # 排除当前以及下一个交易日已经到期或无交易量的代码
    for i in range(0, len(code_list)):
        code = code_list[i]
        code_df = bond_md_df[bond_md_df.ts_code == code].copy()
        code_df.reset_index(drop=True, inplace=True)
        now_code_df = now_bond_md_df[now_bond_md_df.ts_code == code].copy()
        now_code_df.reset_index(drop=True, inplace=True)
        next_code_df = next_bond_md_df[next_bond_md_df.ts_code == code].copy()
        next_code_df.reset_index(drop=True, inplace=True)
        if len(now_code_df) == 0 or now_code_df.loc[0]['vol'] == 0 or len(next_code_df) == 0 or next_code_df.loc[0]['vol'] == 0 or\
            code_df.loc[0]['yield_to_maturity'] <= yield_to_maturity_dict[code] or code_df.loc[0]['close'] >= close_level_dict[code] or\
            now_code_df.loc[0]['yield_to_maturity'] <= filter_yield_low or code in black_list:
        # if len(now_code_df) == 0 or now_code_df.loc[0]['vol'] == 0 or len(next_code_df) == 0 or next_code_df.loc[0]['vol'] == 0 or\
        #     now_code_df.loc[0]['yield_to_maturity'] <= filter_yield_low or code in black_list:
            remove_code_set.add(code)
        if code in highest_price_dict.keys() and len(now_code_df) != 0:
            highest_price = highest_price_dict[code]
            close = now_code_df.loc[0]['close']
            if (highest_price - close) / highest_price >= 0.1:
                remove_code_set.add(code)
                highest_price_dict.pop(code)
                print(trade_date, code)
            
    # 入池后今日也没筛出来，但尚未满足平仓条件且距离最高位还没有跌破15%，那么不进行平仓
    # for i in range(0, len(position_df) - 1):
    #     code = position_df.loc[i]['ts_code']
    #     if code in remove_code_set:
    #         now_code_df = now_bond_md_df[now_bond_md_df.ts_code == code].copy()
    #         now_code_df.reset_index(drop=True, inplace=True)
    #         next_code_df = next_bond_md_df[next_bond_md_df.ts_code == code].copy()
    #         next_code_df.reset_index(drop=True, inplace=True)
    #         highest_price = highest_price_dict[code]
    #         close = now_code_df.loc[0]['close']
    #         if len(now_code_df) != 0 and now_code_df.loc[0]['vol'] != 0 and len(next_code_df) != 0 and next_code_df.loc[0]['vol'] != 0 and\
    #             close < close_level_dict[code] and (highest_price - close) / highest_price < 0.15:
    #             remove_code_set.remove(code)
    #         if (highest_price - close) / highest_price >= 0.15:
    #             print(trade_date, code)
    
    for code in remove_code_set:
        code_list.remove(code)
    
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

# 更新股指期货换仓盈亏情况
def calculate_fut_close_order_list(trade_date, position_df, fut_ts_code, last_fut_ts_code):
    global fut_daily_md_df
    fut_md_df = fut_daily_md_df[(fut_daily_md_df.trade_date == trade_date)].copy()
    
    order_list = []
    # 平旧仓
    fut_position_df = position_df[position_df.ts_code == last_fut_ts_code].copy()
    fut_position_df.reset_index(drop=True, inplace=True)
    last_vol = fut_position_df.loc[0]['vol']
    last_price = fut_position_df.loc[0]['open_price']
    code_df = fut_md_df[fut_md_df.ts_code == last_fut_ts_code].copy()
    code_df.reset_index(drop=True, inplace=True)
    price = code_df.loc[0]['amount'] * 10000 / code_df.loc[0]['vol'] / fut_multiplier
    close_profit = -round((price - last_price) * last_vol * fut_multiplier, 2)
    CurrentFund['close_profit'] += close_profit
    CurrentFund['available'] += close_profit
    order = [last_fut_ts_code, last_vol, DIRECTION_BUY, OPEN_CLOSE_CLOSE, price, close_profit]
    order_list.append(order)
    # 开新仓
    code_df = fut_md_df[fut_md_df.ts_code == fut_ts_code].copy()
    code_df.reset_index(drop=True, inplace=True)
    price = code_df.loc[0]['amount'] * 10000 / code_df.loc[0]['vol'] / fut_multiplier
    order = [fut_ts_code, last_vol, DIRECTION_SELL, OPEN_CLOSE_OPEN, price, 0]
    order_list.append(order)
    add_position_data(acct_id, trade_date, fut_ts_code, last_vol, DIRECTION_SELL, price, 0)
    
    return order_list

# 根据资金情况以及所选合约计算详细仓位
def calculate_position_dict(last_trade_date, trade_date, code_list):
    fund_df = get_fund_data(acct_id, last_trade_date)
    fund_df.reset_index(drop=True, inplace=True)
    asset = fund_df.loc[0]['asset']
    
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
    # if fut_diff_rate <= fut_diff_1:
    #     hedge_rate += hedge_rate_diff_1
    # elif fut_diff_rate >= fut_diff_2:
    #     hedge_rate += hedge_rate_diff_2
    # else:
    #     hedge_rate += hedge_rate_diff_1 + (hedge_rate_diff_2 - hedge_rate_diff_1) * (fut_diff_rate - fut_diff_1) / (fut_diff_2 - fut_diff_1)
    if fut_diff_rate >= 10 and fut_diff_rate <= 20:
        hedge_rate -= 0.1
    # if fut_diff_rate <= 5:
    #     hedge_rate += 0.1
    
    # 根据全市场平均收益率调整仓位 bata
    yield_to_maturity_list = bond_md_df['yield_to_maturity'].tolist()
    yield_to_maturity_mean = sum(yield_to_maturity_list) / len(yield_to_maturity_list)
    if yield_to_maturity_mean <= 2 and yield_to_maturity_mean >= -2:
        cut_rate_1 = (2 - yield_to_maturity_mean) / 16
    elif yield_to_maturity_mean < -2:
        cut_rate_1 = 0.25
    else:
        cut_rate_1 = 0
        
    # 根据全市场平均溢价率调整仓位 bata
    if cb_over_mean <= 70 and cb_over_mean >= 15:
        cut_rate_2 = (70 - cb_over_mean) / 220
    elif cb_over_mean < 15:
        cut_rate_2 = 0.25
    else:
        cut_rate_2 = 0
        
    asset *= (1 - cut_rate_1 - cut_rate_2)
    
    if len(code_list) <= 1:
        per_fund = 0
        fut_fund = 0
    else:
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
        vol -= vol % 10
        value_list = [vol, round(price, 2)]
        position_dict[code] = value_list
    
    code = code_list[len(code_list) - 1]
    code_df = fut_md_df[fut_md_df.ts_code == code].copy()
    code_df.reset_index(drop=True, inplace=True)
    price = code_df.loc[0]['amount'] * 10000 / code_df.loc[0]['vol'] / fut_multiplier
    vol = int(fut_fund * margin_redundancy / margin_rate / fut_multiplier / price)
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
        bond_position_df = position_df[position_df.ts_code == code].copy()
        bond_position_df.reset_index(drop=True, inplace=True)
        if len(bond_position_df) == 0:
            order = [code, vol, DIRECTION_BUY, OPEN_CLOSE_NONE, price, 0]
            order_list.append(order)
            add_position_data(acct_id, trade_date, code, vol, DIRECTION_BUY, price, 0)
            highest_price_dict[code] = price
        else:
            last_vol = bond_position_df.loc[0]['vol']
            last_price = bond_position_df.loc[0]['open_price']
            vol_diff = vol - last_vol
            if vol_diff > 0:
                order = [code, vol_diff, DIRECTION_BUY, OPEN_CLOSE_NONE, price, 0]
                order_list.append(order)
                open_price = round(((last_price * last_vol) + (price * vol_diff)) / vol, 2)
                position_profit = round((price - open_price) * vol, 2)
                add_position_data(acct_id, trade_date, code, vol, DIRECTION_BUY, open_price, position_profit)
                CurrentFund['position_profit'] += position_profit
            elif vol_diff < 0:
                close_profit = -(price - last_price) * vol_diff
                CurrentFund['close_profit'] += close_profit
                order = [code, -vol_diff, DIRECTION_SELL, OPEN_CLOSE_NONE, price, close_profit]
                order_list.append(order)
                position_profit = round((price - last_price) * vol, 2)
                add_position_data(acct_id, trade_date, code, vol, DIRECTION_BUY, last_price, position_profit)
                CurrentFund['position_profit'] += position_profit
            else:
                position_profit = round((price - last_price) * vol)
                add_position_data(acct_id, trade_date, code, vol, DIRECTION_BUY, last_price, position_profit)
                CurrentFund['position_profit'] += position_profit
            highest_price_dict[code] = max(price, highest_price_dict[code])
    
    return order_list
                

# 策略主线程
def main():
    ret = read_config('./可转债-股指期货对冲回测框架设置-v4.xlsx')
    if ret != 0:
        print("设置读取错误，请检查设置文件！")
        exit(1)
    
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
            
            # 若进入了新的周期，则进行仓位的重新分配，进行新开仓和补仓操作
            if alter_period == 1 or (total_days % alter_period) == 1:
                # 根据今日市场数据确定最终的今日所有可转债和期货合约的具体仓位
                position_dict = calculate_position_dict(last_trade_date, trade_date, code_list)
                
                # 根据昨日持仓以及今日持仓计算得到今日的交易指令列表
                buy_order_list = calculate_order_list(trade_date, position_dict, position_df)
                order_list += buy_order_list
            else:
                # 在周期内若主力股指期货合约发生变化，则进行换仓操作
                fut_ts_code = code_list[len(code_list) - 1]
                last_fut_ts_code = last_code_list[len(last_code_list) - 1]
                if fut_ts_code != last_fut_ts_code:
                    fut_order_list = calculate_fut_close_order_list(trade_date, position_df, fut_ts_code, last_fut_ts_code)
                    order_list += fut_order_list
                    remove_code_list.append(last_fut_ts_code)
                
                update_position_profit(trade_date, position_df, remove_code_list)
            
            # 根据交易指令列表向柜台发出交易指令，更新【成交数据】，【资金数据】
            for order in order_list:
                place_order(acct_id, trade_date, order)
        
        # 更新账户资金数据
        CurrentFund['trade_date'] = trade_date
        last_fund = get_fund_data(acct_id, last_trade_date)
        last_fund.reset_index(drop=True, inplace=True)
        CurrentFund['asset'] = CurrentFund['asset'] + CurrentFund['close_profit'] + (CurrentFund['position_profit'] - last_fund.loc[0]['position_profit'])
        add_fund_data(list(CurrentFund.values()))
        
        print("交易日：{} | 总资金：{} | 平仓盈亏：{} | 持仓盈亏：{} | 回测进度：{}%".format(trade_date,
              round(CurrentFund['asset'], 2), round(CurrentFund['close_profit'], 2), round(CurrentFund['position_profit'], 2), round((i + 1) / (len(cal_date_list) - 2) * 100, 2)))
        CurrentFund['close_profit'] = 0
        CurrentFund['position_profit'] = 0
        last_code_list = code_list

    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    timeStr = time.strftime('%H-%M-%S')
    if not os.path.exists('output/{}/'.format(todayStr)):
        os.makedirs('output/{}/'.format(todayStr))
    book_name = './output/{}/{}-{}-转债轮换-{}股指期货对冲净值回测-{}.xlsx'.format(todayStr, start_date, end_date, fut_name, timeStr)
    write_data_to_xlsx(book_name, setting_data)


if __name__ == "__main__":
    main()
    
    # full_data = get_daily_md_data('future', 'fut_daily', 'ts_code, trade_date, close', '20190101', '20240201')
    # code_list = ['IC1901.CFX', 'IC1902.CFX', 'IC1903.CFX', 'IC1904.CFX', 'IC1905.CFX', 'IC1906.CFX', 'IC1907.CFX', 'IC1908.CFX', 'IC1909.CFX', 'IC1910.CFX',
    #              'IC1911.CFX', 'IC1912.CFX', 'IC2001.CFX', 'IC2002.CFX', 'IC2003.CFX', 'IC2004.CFX', 'IC2005.CFX', 'IC2006.CFX', 'IC2007.CFX', 'IC2008.CFX',
    #              'IC2009.CFX', 'IC2010.CFX', 'IC2011.CFX', 'IC2012.CFX', 'IC2101.CFX', 'IC2102.CFX', 'IC2103.CFX', 'IC2104.CFX', 'IC2105.CFX', 'IC2106.CFX',
    #              'IC2107.CFX', 'IC2108.CFX', 'IC2109.CFX', 'IC2110.CFX', 'IC2111.CFX', 'IC2112.CFX', 'IC2201.CFX', 'IC2202.CFX', 'IC2203.CFX', 'IC2204.CFX',
    #              'IC2205.CFX', 'IC2206.CFX', 'IC2207.CFX', 'IC2208.CFX', 'IC2209.CFX', 'IC2210.CFX', 'IC2211.CFX', 'IC2212.CFX', 'IC2301.CFX', 'IC2302.CFX',
    #              'IC2303.CFX', 'IC2304.CFX', 'IC2305.CFX', 'IC2306.CFX', 'IC2307.CFX', 'IC2308.CFX', 'IC2309.CFX', 'IC2310.CFX', 'IC2311.CFX', 'IC2312.CFX',
    #              'IC2401.CFX', 'IC2402.CFX', 'IC2403.CFX', 'IC2404.CFX', 'IC2406.CFX', 'IC2409.CFX']
    # with pd.ExcelWriter('中证500期货数据.xlsx') as writer:
    #     for i in range(0, len(code_list)):
    #         code = code_list[i]
    #         data = full_data[full_data.ts_code == code].copy()
    #         data.sort_values(by='trade_date', ascending=True, inplace=True)
    #         data.reset_index(drop=True, inplace=True)
    #         print(data)
    #         data.to_excel(writer, sheet_name='Sheet{}'.format(i), index=False)
    # exit(1)
