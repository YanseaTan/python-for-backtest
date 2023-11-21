# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-10-18
# @Last Modified by:   Yansea
# @Last Modified time: 2023-11-21

import pandas as pd
from sqlalchemy import create_engine
import json
from DatabaseTools import *
import numpy as np
import matplotlib.pyplot as plt

def update_spread_config():
    f = open('./productOps.json', 'r')
    content = f.read()
    ops_json = json.loads(content)
    f.close()

    engine_ts = creat_engine_with_database('futures')
    for i in range(0, len(ops_json)):
        fut_code = ops_json[i]['ProductID']
        # fut_code = 'CJ'
        sql = "select distinct spread_type from fut_spread_daily where fut_code = '{}' order by spread_type".format(fut_code)
        spread_type_df = read_data(engine_ts, sql)
        spread_type_list = []
        spread_price_list = []
        for j in range(0, len(spread_type_df)):
            spread_type = spread_type_df.loc[j]['spread_type']
            # spread_type = '03-05'
            sql = "select distinct ts_code from fut_spread_daily where fut_code = '{}' and spread_type = '{}' order by ts_code".format(fut_code, spread_type)
            ts_code_df = read_data(engine_ts, sql)
            num_of_trade_date = 0
            close_df = pd.DataFrame()
            for k in range(0, len(ts_code_df)):
                ts_code = ts_code_df.loc[k]['ts_code']
                sql = "select trade_date, close, vol from fut_spread_daily where ts_code = '{}' order by ts_code".format(ts_code)
                df = read_data(engine_ts, sql)
                if k == 0:
                    num_of_trade_date = len(df)
                    if num_of_trade_date < 66:
                        break
                # 去除交割前一个月的数据（如果未达到日期则不去除）
                df.drop(df[df.index >= (num_of_trade_date - 22)].index, inplace=True)
                # 通过中位数剔除毛刺数据
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
            # 计算底部 10% 区间的价差阈值
            high = close_df.loc[num - 1]['close']
            low = close_df.loc[0]['close']
            rec_spread = round((low + (high - low) * 0.1), 1)
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
        ops_json[i]['SpreadType'] = spread_type_list
        ops_json[i]['RecPrice'] = spread_price_list
        print('{} 价差配置写入成功，文件更新进度：{}%'.format(fut_code, format((i + 1) / len(ops_json) * 100, '.2f')))
    
    f = open('./productOps.json', 'w')
    content = json.dumps(ops_json, indent=2)
    f.write(content)
    f.close()
    print('价差配置文件更新完毕！')

def main():
    update_spread_config()


if __name__ == "__main__":
    main()
