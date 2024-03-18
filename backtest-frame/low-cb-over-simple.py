# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-03-07
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-15

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
init_fund = 300000000
start_date = '20190101'
end_date = '20240229'
per_fund = 1000000

black_list_dict = {}
black_list = []

# 过程参数
cal_date_list = []
bond_daily_md_df = pd.DataFrame()

# 测试参数
buy_cb_over_level_1 = 15
sell_cb_over_level_1 = 50
max_buy_price = 140
max_len_of_single_code_set = 300
highest_price_dict = {}
max_drawdown = 0.15

setting_data = pd.DataFrame(columns=['init_fund', 'start_date', 'end_date', 'max_buy_price'])
setting_data.loc[0] = [init_fund, start_date, end_date, max_buy_price]

# 更具筛选条件获取指定交易日的代码列表，列表末位为股指期货合约
def filter_code_list(last_trade_date, trade_date, next_trade_date, position_df):
    code_list = []
    remove_code_set = set()
    sub_code_set = set()
    
    global bond_daily_md_df
    bond_md_df = bond_daily_md_df[(bond_daily_md_df.trade_date == last_trade_date)].copy()
    now_bond_md_df = bond_daily_md_df[(bond_daily_md_df.trade_date == trade_date)].copy()
    next_bond_md_df = bond_daily_md_df[(bond_daily_md_df.trade_date == next_trade_date)].copy()
    
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
        cb_over_rate > sell_cb_over_level_1 or (highest_price - close) / highest_price > max_drawdown:
            remove_code_set.add(code)
        else:
            sub_code_set.add(code)
    
    bond_code_df = bond_md_df[((bond_md_df.close <= max_buy_price) & (bond_md_df.cb_over_rate <= buy_cb_over_level_1) & (bond_md_df.vol >= 0))].copy()
    sub_code_list_1 = bond_code_df['ts_code'].tolist()
    union_set = (sub_code_set | (set(sub_code_list_1) - remove_code_set))
    if len(union_set) > max_len_of_single_code_set:
        bond_code_df.sort_values(by='cb_over_rate', ascending=True, inplace=True)
        sub_code_list_1 = bond_code_df['ts_code'].tolist()
        for code in sub_code_list_1:
            if len(sub_code_set) >= max_len_of_single_code_set:
                break
            if code not in remove_code_set:
                sub_code_set.add(code)
        sub_code_list_1 = list(sub_code_set)
    else:
        sub_code_list_1 = list(union_set)
    
    code_list = sub_code_list_1
    
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
    
    return code_list

# 更新持仓浮盈情况
def update_position_profit(trade_date, position_df, remove_code_list):
    if len(position_df) == 0:
        return
    
    global bond_daily_md_df
    bond_md_df = bond_daily_md_df[(bond_daily_md_df.trade_date == trade_date)].copy()
    
    # 可转债部分
    for i in range(0, len(position_df)):
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
    bond_md_df = bond_daily_md_df[(bond_daily_md_df.trade_date == trade_date)].copy()
    
    global per_fund
    position_dict = {}
    for i in range(0, len(code_list)):
        code = code_list[i]
        code_df = bond_md_df[bond_md_df.ts_code == code].copy()
        code_df.reset_index(drop=True, inplace=True)
        price = code_df.loc[0]['amount'] * 1000 / code_df.loc[0]['vol']
        vol = int(per_fund / price)
        vol -= vol % 10
        value_list = [vol, round(price, 2)]
        position_dict[code] = value_list
    
    return position_dict

# 计算持仓变化，生成交易指令
def calculate_order_list(trade_date, position_dict, position_df):
    order_list = []
    
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
    cal_date_list = get_cal_date_list(start_date, end_date)
    bond_daily_md_df = get_daily_md_data('bond', 'cb_daily_test', 'ts_code, trade_date, close, vol, amount, yield_to_maturity, cb_over_rate', start_date, end_date)
    
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
            remove_code_list = list(set(last_code_list) - set(code_list))
            order_list = calculate_bond_sell_order_list(trade_date, position_df, remove_code_list)
            
            # 根据今日市场数据确定今日新增可转债和期货合约的具体仓位
            add_code_list = list(set(code_list) - set(last_code_list))
            position_dict = calculate_position_dict(last_trade_date, trade_date, add_code_list)
            
            # 根据昨日持仓以及今日持仓计算得到今日的交易指令列表
            buy_order_list = calculate_order_list(trade_date, position_dict, position_df)
            order_list += buy_order_list
            
            # 将代码列表中保持的可转债代码进行持仓盈亏的更新
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
        
        print("交易日：{} | 总资金：{} | 可用资金：{} | 可转债标的数量：{} | 平仓盈亏：{} | 持仓盈亏：{} | 回测进度：{}%".format(trade_date, round(CurrentFund['asset'], 2),
              round(CurrentFund['available'], 2), len(code_list) - 1, round(CurrentFund['close_profit'], 2), round(CurrentFund['position_profit'], 2), round((i + 1) / (len(cal_date_list) - 2) * 100, 2)))
        CurrentFund['close_profit'] = 0
        CurrentFund['position_profit'] = 0
        last_code_list = code_list
    
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    timeStr = time.strftime('%H-%M-%S')
    if not os.path.exists('output/{}/'.format(todayStr)):
        os.makedirs('output/{}/'.format(todayStr))
    book_name = './output/{}/{}-{}-简化低溢价进攻选债净值回测-{}.xlsx'.format(todayStr, start_date, end_date, timeStr)
    write_data_to_xlsx(book_name, setting_data)
    write_close_profit_to_xlsx(book_name)


if __name__ == "__main__":
    main()
