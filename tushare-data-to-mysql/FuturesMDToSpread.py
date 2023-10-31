# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-10-13
# @Last Modified by:   Yansea
# @Last Modified time: 2023-10-27

import pandas as pd
import tushare as ts
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import xlwings as xw
import datetime
from DatabaseTools import *

# Tushare 账户 token
token = 'a526c0dd1419c44623d2257ad618848962a5ad988f36ced44ae33981'

# 获取上一交易日的日期
def get_last_trade_date():
   today = datetime.date.today()
   todayStr = today.strftime('%Y%m%d')
   df = pro.trade_cal(**{"cal_date":todayStr}, fields=["pretrade_date"])
   return df.loc[0]['pretrade_date']
    
# 获取指定两个合约在所有重合交易日的价差数据，并存入数据库
def store_spread_daily_by_ts_code(fut_code, ins_1, ins_2):
    engine_ts = creat_engine_with_database('futures')
    sql = "select trade_date from fut_daily where ts_code = '{}' and close is not NULL;".format(ins_1)
    date_1 = read_data(engine_ts, sql)
    sql = "select trade_date from fut_daily where ts_code = '{}' and close is not NULL;".format(ins_2)
    date_2 = read_data(engine_ts, sql)
    date = pd.merge(date_1, date_2)
    sql = "select trade_date, close from fut_daily where ts_code = '{}';".format(ins_1)
    close_1 = read_data(engine_ts, sql)
    sql = "select trade_date, close from fut_daily where ts_code = '{}';".format(ins_2)
    close_2 = read_data(engine_ts, sql)
    
    ts_code = ins_1[:ins_1.index('.')] + '-' + ins_2[:ins_2.index('.')]
    spread_type = ins_1[:ins_1.index('.')][-2:] + '-' + ins_2[:ins_2.index('.')][-2:]
    # 当数据表不为空，需要新增数据时使用，适用于有可能插入重复数据的情况（更慢）
    # ts_code_list = [ts_code]
    # fut_code_list = [fut_code]
    # spread_type_list = [spread_type]
    # trade_date_list = []
    # close_list = []
    # df = pd.DataFrame()
    
    # for i in range(0, len(date)):
    #     trade_date = date.loc[i]['trade_date']
    #     spread = close_1[close_1['trade_date'] == trade_date].iat[0, 1] - close_2[close_2['trade_date'] == trade_date].iat[0, 1]
    #     trade_date_list.append(trade_date)
    #     close_list.append(spread)
    #     df['ts_code'] = ts_code_list
    #     df['fut_code'] = fut_code_list
    #     df['spread_type'] = spread_type_list
    #     df['trade_date'] = trade_date_list
    #     df['close'] = close_list
    #     trade_date_list.clear()
    #     close_list.clear()
    #     # 写入数据库，避免 Key 重复后报错
    #     try:
    #         write_data(engine_ts, 'fut_spread_daily', df)
    #     except:
    #         continue
    
    # 当数据表为空时运行，或者保证插入数据不存在重复数据时运行（更快）
    ts_code_list = [ts_code] * len(date)
    fut_code_list = [fut_code] * len(date)
    spread_type_list = [spread_type] * len(date)
    trade_date_list = []
    close_list = []
    df = pd.DataFrame()
    
    for i in range(0, len(date)):
        trade_date = date.loc[i]['trade_date']
        spread = close_1[close_1['trade_date'] == trade_date].iat[0, 1] - close_2[close_2['trade_date'] == trade_date].iat[0, 1]
        trade_date_list.append(trade_date)
        close_list.append(spread)
        
    df['ts_code'] = ts_code_list
    df['fut_code'] = fut_code_list
    df['spread_type'] = spread_type_list
    df['trade_date'] = trade_date_list
    df['close'] = close_list
    
    write_data(engine_ts, 'fut_spread_daily', df)
    # write_data(engine_ts, 'au_spread', df)
        
    print('写入完毕！数据量：{} 合约组合：{} '.format(len(date), ts_code), end='')
    
    # 绘制图像
    # figure,axes=plt.subplots(nrows=1,ncols=2,figsize=(20,5))
    # df.plot(ax=axes[0])         # 折线图
    # df.plot.kde(ax=axes[1])     # 概率分布图
    # plt.show()                  # 保持图像显示

# 获取指定品种在指定到期日区间内所有的相邻月组合列表，并将所有合约对在重合交易日的价差数据存入数据库
def store_spread_daily_by_fut_code(fut_code, start_date, end_date):
    engine_ts = creat_engine_with_database('futures')
    sql = "select ts_code from fut_basic where fut_code = '{}' and list_date > '{}' and list_date < '{}' order by ts_code;".format(fut_code, start_date, end_date)
    code_df = read_data(engine_ts, sql)
    combination_list = []
    for i in range(0, len(code_df) - 1):
        ins_1 = code_df.loc[i]['ts_code']
        ins_2 = code_df.loc[i + 1]['ts_code']
        combination = []
        combination.append(ins_1)
        combination.append(ins_2)
        combination_list.append(combination)
    
    for i in range(0, len(combination_list)):
        ins_1 = combination_list[i][0]
        ins_2 = combination_list[i][1]
        store_spread_daily_by_ts_code(fut_code, ins_1, ins_2)
        print('总进度：{}%'.format(format((i + 1) / len(combination_list) * 100, '.2f')))

# 获取指定交易日所有合约组合的日行情价差数据
def update_spread_daily_data(last_trade_date):
    engine_ts = creat_engine_with_database('futures')
    sql = "select distinct fut_code from fut_basic order by fut_code;"
    fut_df = read_data(engine_ts, sql)
    fut_list = fut_df['fut_code'].tolist()
    for i in range(0, len(fut_list)):
        fut_code = fut_list[i]
        sql = "select ts_code from fut_basic where fut_code = '{}' and list_date > '{}' and list_date < '{}' order by ts_code;".format(fut_code, '20190723', last_trade_date)
        code_df = read_data(engine_ts, sql)
        combination_list = []
        for j in range(0, len(code_df) - 1):
            ins_1 = code_df.loc[j]['ts_code']
            ins_2 = code_df.loc[j + 1]['ts_code']
            
            engine_ts = creat_engine_with_database('futures')
            sql = "select trade_date from fut_daily where ts_code = '{}' and close is not NULL;".format(ins_1)
            date_1 = read_data(engine_ts, sql)
            sql = "select trade_date from fut_daily where ts_code = '{}' and close is not NULL;".format(ins_2)
            date_2 = read_data(engine_ts, sql)
            date = pd.merge(date_1, date_2)
            if last_trade_date in date['trade_date'].values:
                sql = "select close from fut_daily where ts_code = '{}' and trade_date = '{}';".format(ins_1, last_trade_date)
                close_1 = read_data(engine_ts, sql)
                sql = "select close from fut_daily where ts_code = '{}' and trade_date = '{}';".format(ins_2, last_trade_date)
                close_2 = read_data(engine_ts, sql)
                if len(close_1) and len(close_2):
                    spread = close_1.loc[0]['close'] - close_2.loc[0]['close']
                    ts_code = ins_1[:ins_1.index('.')] + '-' + ins_2[:ins_2.index('.')]
                    spread_type = ins_1[:ins_1.index('.')][-2:] + '-' + ins_2[:ins_2.index('.')][-2:]
                    
                    df = pd.DataFrame()
                    df['ts_code'] = [ts_code]
                    df['fut_code'] = [fut_code]
                    df['spread_type'] = [spread_type]
                    df['trade_date'] = [last_trade_date]
                    df['close'] = [spread]
                
                    write_data(engine_ts, 'fut_spread_daily', df)
                    
        print('{} 品种日价差数据写入完毕！总进度：{}%'.format(fut_code, format((i + 1) / len(fut_list) * 100, '.2f')))

def main():
    # 导入所选时间内所有合约组合的日行情价差数据到数据库中
    # engine_ts = creat_engine_with_database('futures')
    # sql = "select distinct fut_code from fut_basic order by fut_code;"
    # fut_df = read_data(engine_ts, sql)
    # fut_list = fut_df['fut_code'].tolist()
    # for i in range(0, len(fut_list)):
    #     store_spread_daily_by_fut_code(fut_list[i], '20190723', '20231024')
    
    last_trade_date = get_last_trade_date()
    update_spread_daily_data(last_trade_date)
    
    # combination_list = [['AU2012.SHF', 'AU2102.SHF'], ['AU2112.SHF', 'AU2202.SHF'], ['AU2212.SHF', 'AU2302.SHF'], ['AU2312.SHF', 'AU2402.SHF']]
    # for i in range(0, len(combination_list)):
    #     ins_1 = combination_list[i][0]
    #     ins_2 = combination_list[i][1]
    #     store_spread_daily_by_ts_code('AU', ins_1, ins_2)
    #     print('总进度：{}%'.format(format((i + 1) / len(combination_list) * 100, '.2f')))


if __name__ == "__main__":
    # 登录 Tushare 接口
    pro = ts.pro_api(token)
    main()
