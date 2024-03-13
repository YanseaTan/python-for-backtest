# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-03-06
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-13

import pandas as pd
import xlwings as xw
import datetime
import time
import json
import os
import sys
sys.path.append('./backtest-frame/api/')
from api.BackTestApi import *

# 参数设置
test_year = '2023'
fut_code = 'M'
spread_type = '05-07'
index_name = '豆粕库存_中国'    # 后续如果确定唯一有效的库存数据，则此参数可以去除
fut_multiplier = 10
margin_rate = 0.2

acct_id = 'hailong'
init_fund = 2000000
total_vol = 40
per_vol = 1
num_of_years = 3

MAX_VALUE = 9999999999

setting_data = pd.DataFrame(columns=['init_fund', 'test_year', 'fut_code', 'fut_multiplier', 'margin_rate', 'spread_type', 'total_vol', 'per_vol'])
setting_data.loc[0] = [init_fund, test_year, fut_code, fut_multiplier, margin_rate, spread_type, total_vol, per_vol]

# 公共变量
spread_md_df = pd.DataFrame()
first_md_df = pd.DataFrame()
second_md_df = pd.DataFrame()
inventory_df = pd.DataFrame()
first_ts_code = ''
second_ts_code = ''

# 根据库存走势以及库存历史位置分位数进行打分
def calculate_inventory_score(last_trade_date):
    score = 0
    
    last_inventory_df = inventory_df[inventory_df.update_date <= last_trade_date].copy()
    last_inventory_df.sort_values(by='update_date', ascending=False, inplace=True)
    last_inventory_df.reset_index(drop=True, inplace=True)
    last_inventory = last_inventory_df.loc[0]['value']
    previous_inventory = last_inventory_df.loc[1]['value']
    # 根据库存走势打分
    if last_inventory < previous_inventory:
        score += 1
    else:
        score -= 1
    
    max_inventory = -MAX_VALUE
    mini_inventory = MAX_VALUE
    for i in range(1, num_of_years + 1):
        date = str(int(last_trade_date[:4]) - i) + last_trade_date[-4:]
        
        left_inventory_df = inventory_df[inventory_df.update_date <= date].copy()
        left_inventory_df.sort_values(by='update_date', ascending=False, inplace=True)
        left_inventory_df.reset_index(drop=True, inplace=True)
        inventory = left_inventory_df.loc[0]['value']
        max_inventory = max(max_inventory, inventory)
        mini_inventory = min(mini_inventory, inventory)
        
        right_inventory_df = inventory_df[inventory_df.update_date >= date].copy()
        right_inventory_df.sort_values(by='update_date', ascending=True, inplace=True)
        right_inventory_df.reset_index(drop=True, inplace=True)
        inventory = right_inventory_df.loc[0]['value']
        max_inventory = max(max_inventory, inventory)
        mini_inventory = min(mini_inventory, inventory)
    # 根据库存历史位置打分
    loc_percent = (last_inventory - mini_inventory) / (max_inventory - mini_inventory)
    if loc_percent < 0.5:
        score += 1
    elif loc_percent > 1:
        score -= 1
        
    return score

# 根据价差历史位置分位数进行打分
def calculate_spread_score(last_trade_date):
    score = 0
    
    last_spread_df = spread_md_df[spread_md_df.trade_date == last_trade_date].copy()
    last_spread_df.reset_index(drop=True, inplace=True)
    last_spread = last_spread_df.loc[0]['close']
    
    max_spread = -MAX_VALUE
    mini_spread = MAX_VALUE
    for i in range(1, num_of_years + 1):
        date = str(int(last_trade_date[:4]) - i) + last_trade_date[-4:]
        
        left_spread_df = spread_md_df[spread_md_df.trade_date <= date].copy()
        left_spread_df.sort_values(by='trade_date', ascending=False, inplace=True)
        left_spread_df.reset_index(drop=True, inplace=True)
        spread = left_spread_df.loc[0]['close']
        max_spread = max(max_spread, spread)
        mini_spread = min(mini_spread, spread)
        
        right_spread_df = spread_md_df[spread_md_df.trade_date >= date].copy()
        right_spread_df.sort_values(by='trade_date', ascending=True, inplace=True)
        right_spread_df.reset_index(drop=True, inplace=True)
        spread = right_spread_df.loc[0]['close']
        max_spread = max(max_spread, spread)
        mini_spread = min(mini_spread, spread)
    # 根据价差历史位置打分
    loc_percent = (last_spread - mini_spread) / (max_spread - mini_spread)
    if loc_percent < 0.5:
        score += 1
    elif loc_percent > 1:
        score -= 1
    
    return score

# 计算今日交易指令
def calculate_order(last_trade_date):
    score = 0
    
    score += calculate_inventory_score(last_trade_date)
    score += calculate_spread_score(last_trade_date)
    
    if score >= 2:
        order_plan = [OPEN_CLOSE_OPEN, per_vol]
    elif score <= -2:
        order_plan = [OPEN_CLOSE_CLOSE, per_vol]
    else:
        order_plan = [OPEN_CLOSE_NONE, 0]
        
    return order_plan

# 更新持仓浮盈情况
def update_position_profit(trade_date, position_df):
    if len(position_df) == 0:
        return
    
    open_price = position_df.loc[0]['open_price']
    vol = position_df.loc[0]['vol']
    md_df = first_md_df[first_md_df.trade_date == trade_date].copy()
    md_df.reset_index(drop=True, inplace=True)
    price = md_df.loc[0]['close']
    position_profit = round((price - open_price) * vol * fut_multiplier, 2)
    add_position_data(acct_id, trade_date, first_ts_code, vol, DIRECTION_BUY, open_price, position_profit)
    CurrentFund['position_profit'] += position_profit
    
    open_price = position_df.loc[1]['open_price']
    md_df = second_md_df[second_md_df.trade_date == trade_date].copy()
    md_df.reset_index(drop=True, inplace=True)
    price = md_df.loc[0]['close']
    position_profit = -round((price - open_price) * vol * fut_multiplier, 2)
    add_position_data(acct_id, trade_date, second_ts_code, vol, DIRECTION_SELL, open_price, position_profit)
    CurrentFund['position_profit'] += position_profit

# 根据交易指令以及持仓情况进行下单并更新持仓情况
def make_order(trade_date, position_df, order_plan):
    if order_plan[0] == OPEN_CLOSE_NONE:
        update_position_profit(trade_date, position_df)
    else:
        md_df = first_md_df[first_md_df.trade_date == trade_date].copy()
        md_df.reset_index(drop=True, inplace=True)
        first_price = md_df.loc[0]['close']
        
        md_df = second_md_df[second_md_df.trade_date == trade_date].copy()
        md_df.reset_index(drop=True, inplace=True)
        second_price = md_df.loc[0]['close']
        
        if len(position_df) == 0:
            last_vol = 0
        else:
            last_vol = position_df.loc[0]['vol']
        vol = order_plan[1]
        
        if (last_vol + vol) > total_vol:
            update_position_profit(trade_date, position_df)
            return
        
        if order_plan[0] == OPEN_CLOSE_OPEN:
            # 一腿买开
            place_order(acct_id, trade_date, [first_ts_code, vol, DIRECTION_BUY, OPEN_CLOSE_OPEN, first_price], position_df, fut_multiplier)
            # 二腿卖开
            place_order(acct_id, trade_date, [second_ts_code, vol, DIRECTION_SELL, OPEN_CLOSE_OPEN, second_price], position_df, fut_multiplier)
        elif order_plan[0] == OPEN_CLOSE_CLOSE:
            # 一腿卖平
            place_order(acct_id, trade_date, [first_ts_code, vol, DIRECTION_SELL, OPEN_CLOSE_CLOSE, first_price], position_df, fut_multiplier)
            # 二腿买平
            place_order(acct_id, trade_date, [second_ts_code, vol, DIRECTION_BUY, OPEN_CLOSE_CLOSE, second_price], position_df, fut_multiplier)

def main():
    # 计算起止日期
    first_leg = spread_type[:2]
    second_leg = spread_type[-2:]
    start_date = test_year + second_leg + '01'
    end_date = str(int(test_year) + 1 - (int(first_leg) > int(second_leg))) + first_leg + '28'
    
    # 获取价差行情以及交易日历
    sql = "select trade_date, close, vol from future.fut_spread_daily where fut_code = '{}' and spread_type = '{}' and\
        trade_date <= '{}' order by trade_date".format(fut_code, spread_type, end_date)
    global spread_md_df
    spread_md_df = read_postgre_data(sql)
    sub_spread_md_df = spread_md_df[spread_md_df.trade_date >= start_date].copy()
    sub_spread_md_df.reset_index(drop=True, inplace=True)
    start_date = sub_spread_md_df.loc[0]['trade_date']
    end_date = spread_md_df.loc[len(spread_md_df) - 1]['trade_date']
    cal_date_list = get_cal_date_list(start_date, end_date)
    
    # 获取两腿行情及合约代码
    fut_daily_md_df = get_daily_md_data('future', 'fut_daily', 'ts_code, trade_date, close', start_date, end_date)
    global first_md_df
    first_md_df = fut_daily_md_df[(fut_daily_md_df.ts_code.str.startswith(fut_code)) & (fut_daily_md_df.ts_code.str.contains('{}.'.format(first_leg)))].copy()
    first_md_df.reset_index(drop=True, inplace=True)
    global first_ts_code
    first_ts_code = first_md_df.loc[0]['ts_code']
    global second_md_df
    second_md_df = fut_daily_md_df[(fut_daily_md_df.ts_code.str.startswith(fut_code)) & (fut_daily_md_df.ts_code.str.contains('{}.'.format(second_leg)))].copy()
    second_md_df.reset_index(drop=True, inplace=True)
    global second_ts_code
    second_ts_code = second_md_df.loc[0]['ts_code']
    
    # 获取库存数据
    sql = "select update_date, value from future.fut_funds where fut_code = '{}' and index_name = '{}' order by update_date".format(fut_code, index_name, end_date)
    global inventory_df
    inventory_df = read_postgre_data(sql)
    
    # 第一天设置初始资金
    set_init_fund(acct_id, start_date, init_fund)
    
    # 日期驱动策略
    for i in range(1, len(cal_date_list)):
        last_trade_date = cal_date_list[i - 1]
        trade_date = cal_date_list[i]
        
        # 获取上一日持仓
        position_df = get_position_data(acct_id, last_trade_date)
        position_df.reset_index(drop=True, inplace=True)
        
        # 计算今日交易指令
        order_plan = calculate_order(last_trade_date)
        
        # 根据交易指令以及持仓情况进行下单并更新持仓情况
        make_order(trade_date, position_df, order_plan)
        
        # 更新总资金情况
        CurrentFund['trade_date'] = trade_date
        last_fund = get_fund_data(acct_id, last_trade_date)
        last_fund.reset_index(drop=True, inplace=True)
        CurrentFund['asset'] = CurrentFund['asset'] + CurrentFund['close_profit'] + (CurrentFund['position_profit'] - last_fund.loc[0]['position_profit'])
        # 更新可用资金情况
        position_df = get_position_data(acct_id, trade_date)
        if len(position_df) == 0:
            vol = 0
        else:
            position_df.reset_index(drop=True, inplace=True)
            vol = position_df.loc[0]['vol']
        md_df = first_md_df[first_md_df.trade_date == trade_date].copy()
        md_df.reset_index(drop=True, inplace=True)
        price = md_df.loc[0]['close']
        CurrentFund['available'] = CurrentFund['asset'] - vol * fut_multiplier * price * margin_rate
        add_fund_data(list(CurrentFund.values()))
        
        print("交易日：{} | 总资金：{} | 可用资金：{} | 期货持仓手数：{} | 持仓盈亏：{} | 回测进度：{}%".format(trade_date, round(CurrentFund['asset'], 2),
              round(CurrentFund['available'], 2), vol * 2, round(CurrentFund['position_profit'], 2), round((i - 1) / (len(cal_date_list) - 2) * 100, 2)))
        CurrentFund['close_profit'] = 0
        CurrentFund['position_profit'] = 0
        
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    timeStr = time.strftime('%H-%M-%S')
    if not os.path.exists('output/{}/'.format(todayStr)):
        os.makedirs('output/{}/'.format(todayStr))
    book_name = './output/{}/{}-{}-{}{}基于基本面季节性走势回测-{}.xlsx'.format(todayStr, start_date, end_date, fut_code, spread_type, timeStr)
    write_data_to_xlsx(book_name, setting_data)

if __name__ == "__main__":
    main()
