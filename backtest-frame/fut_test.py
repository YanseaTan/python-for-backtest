# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-03-06
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-08

import pandas as pd
import xlwings as xw
import datetime
import time
import json
import os
import sys
sys.path.append('./backtest-frame/api/')
from api.BackTestApi import *

start_date = '20200331'
end_date = '20200815'
fut_code = 'V'
fut_multiplier = 5
margin_rate = 0.2
spread_type = '09-01'
vol = 1

acct_id = 'fut_test'
init_fund = 50000

setting_data = pd.DataFrame(columns=['init_fund', 'start_date', 'end_date', 'fut_code', 'fut_multiplier', 'margin_rate', 'spread_type', 'vol'])
setting_data.loc[0] = [init_fund, start_date, end_date, fut_code, fut_multiplier, margin_rate, spread_type, vol]

def main():
    
    # 获取交易日历以及期货行情
    cal_date_list = get_cal_date_list(start_date, end_date)
    first_day = cal_date_list[0]
    second_day = cal_date_list[1]
    fut_daily_md_df = get_daily_md_data('future', 'fut_daily', 'ts_code, trade_date, close', start_date, end_date)
    
    # 获取指定两腿的行情
    first_md_df = fut_daily_md_df[(fut_daily_md_df.ts_code.str.startswith(fut_code)) & (fut_daily_md_df.ts_code.str.contains('{}.'.format(spread_type[:2])))].copy()
    second_md_df = fut_daily_md_df[(fut_daily_md_df.ts_code.str.startswith(fut_code)) & (fut_daily_md_df.ts_code.str.contains('{}.'.format(spread_type[-2:])))].copy()
    
    # 第一天设置初始资金
    set_init_fund(acct_id, first_day, init_fund)
    
    # 第二天进行交易，保证金只收一边
    md_df = first_md_df[first_md_df.trade_date == second_day].copy()
    md_df.reset_index(drop=True, inplace=True)
    first_ts_code = md_df.loc[0]['ts_code']
    price = md_df.loc[0]['close']
    place_order(acct_id, second_day, [first_ts_code, vol, DIRECTION_BUY, OPEN_CLOSE_OPEN, price, 0])
    add_position_data(acct_id, second_day, first_ts_code, vol, DIRECTION_BUY, price, 0)
    CurrentFund['available'] -= vol * fut_multiplier * price * margin_rate
    
    md_df = second_md_df[second_md_df.trade_date == second_day].copy()
    md_df.reset_index(drop=True, inplace=True)
    second_ts_code = md_df.loc[0]['ts_code']
    price = md_df.loc[0]['close']
    place_order(acct_id, second_day, [second_ts_code, vol, DIRECTION_SELL, OPEN_CLOSE_OPEN, price, 0])
    add_position_data(acct_id, second_day, second_ts_code, vol, DIRECTION_SELL, price, 0)
    
    CurrentFund['trade_date'] = second_day
    add_fund_data(list(CurrentFund.values()))
    
    # 更新每日的持仓盈亏
    for i in range(2, len(cal_date_list)):
        last_trade_date = cal_date_list[i - 1]
        trade_date = cal_date_list[i]
        
        # 获取上一日持仓
        position_df = get_position_data(acct_id, last_trade_date)
        position_df.reset_index(drop=True, inplace=True)
        
        # 计算当日持仓盈亏
        open_price = position_df.loc[0]['open_price']
        md_df = first_md_df[first_md_df.trade_date == trade_date].copy()
        md_df.reset_index(drop=True, inplace=True)
        price = md_df.loc[0]['close']
        position_profit = round((price - open_price) * vol * fut_multiplier, 2)
        add_position_data(acct_id, trade_date, first_ts_code, vol, DIRECTION_BUY, open_price, position_profit)
        CurrentFund['position_profit'] += position_profit
        CurrentFund['available'] = init_fund - vol * fut_multiplier * price * margin_rate
        
        open_price = position_df.loc[1]['open_price']
        md_df = second_md_df[second_md_df.trade_date == trade_date].copy()
        md_df.reset_index(drop=True, inplace=True)
        price = md_df.loc[0]['close']
        position_profit = -round((price - open_price) * vol * fut_multiplier, 2)
        add_position_data(acct_id, trade_date, second_ts_code, vol, DIRECTION_SELL, open_price, position_profit)
        CurrentFund['position_profit'] += position_profit
        
        # 更新资金情况
        CurrentFund['trade_date'] = trade_date
        last_fund = get_fund_data(acct_id, last_trade_date)
        last_fund.reset_index(drop=True, inplace=True)
        CurrentFund['asset'] = CurrentFund['asset'] + (CurrentFund['position_profit'] - last_fund.loc[0]['position_profit'])
        add_fund_data(list(CurrentFund.values()))
        
        print("交易日：{} | 总资金：{} | 可用资金：{} | 股指期货持仓手数：{} | 持仓盈亏：{} | 回测进度：{}%".format(trade_date, round(CurrentFund['asset'], 2),
              round(CurrentFund['available'], 2), vol, round(CurrentFund['position_profit'], 2), round((i - 1) / (len(cal_date_list) - 2) * 100, 2)))
        CurrentFund['close_profit'] = 0
        CurrentFund['position_profit'] = 0
        
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    timeStr = time.strftime('%H-%M-%S')
    if not os.path.exists('output/{}/'.format(todayStr)):
        os.makedirs('output/{}/'.format(todayStr))
    book_name = './output/{}/{}-{}-{}{}季节性走势回测-{}.xlsx'.format(todayStr, start_date, end_date, fut_code, spread_type, timeStr)
    write_data_to_xlsx(book_name, setting_data)

if __name__ == "__main__":
    main()
