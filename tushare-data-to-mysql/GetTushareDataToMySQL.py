# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-10-10
# @Last Modified by:   Yansea
# @Last Modified time: 2023-10-12

import time
import datetime
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine

# Tushare 账户 token
token = 'a526c0dd1419c44623d2257ad618848962a5ad988f36ced44ae33981'

# 数据库用户配置
user = 'root'
password = '0527'
addr = 'localhost'

# 获取昨天的日期
def getYesterday():
   today = datetime.date.today()
   oneday = datetime.timedelta(days=1)
   yesterday = today - oneday
   yesterdaystr = yesterday.strftime('%Y%m%d')
   return yesterdaystr

# 创建指定数据库操作引擎
def creat_engine_with_database(database):
    engine_ts = create_engine('mysql://{}:{}@/{}?charset=utf8&use_unicode=1'.format(user, password, database))
    return engine_ts

# 获取指定数据库的指定表格内容
def read_data(engine_ts, tableName, sql):
    df = pd.read_sql_query(sql, engine_ts)
    return df

# 将指定内容写入指定数据库的指定表格中
def write_data(engine_ts, tableName, df):
    res = df.to_sql(tableName, engine_ts, index=False, if_exists='append', chunksize=5000)
    print('写入成功！数据量:', res)

# 获取股票基本信息
def get_stock_basic_data():
    engine_ts = creat_engine_with_database('stock')
    df = pro.stock_basic()
    write_data(engine_ts, 'stock_basic', df)

# 获取可转债基本信息
def get_cb_basic_data():
    engine_ts = creat_engine_with_database('bond')
    df = pro.cb_basic(fields=["ts_code","bond_short_name","stk_code","stk_short_name","maturity","par","issue_price","issue_size",
                              "remain_size","value_date","maturity_date","coupon_rate","list_date","delist_date","exchange",
                              "conv_start_date","conv_end_date","conv_stop_date","first_conv_price","conv_price","add_rate"])
    write_data(engine_ts, 'cb_basic', df)
    
# 获取所有可转债的所有历史日行情数据
def get_cb_daily_data():
    engine_ts = creat_engine_with_database('bond')
    sql = 'SELECT ts_code FROM cb_basic'
    ts_code = read_data(engine_ts, 'cb_basic', sql)
    for i in range(0, len(ts_code)):
        # 若调用次数达到限制，则在一分钟内反复尝试
        for _ in range(60):
            try:
                df = pro.cb_daily(**{'ts_code': ts_code.loc[i]['ts_code']})
                if len(df):
                    write_data(engine_ts, 'cb_daily', df)
                else:
                    print('回调数据为空！可转债代码:', ts_code.loc[i]['ts_code'])
            except:
                time.sleep(1)
            else:
                break
            
# 获取指定日期内所有可转债的日行情数据
def get_cb_md_data(start_date = '', end_date = ''):
    engine_ts = creat_engine_with_database('bond')
    df = pro.cb_daily(**{"start_date": start_date, "end_date": end_date})
    write_data(engine_ts, 'cb_daily', df)

# 获取期货合约基本信息
def get_fut_basic_data():
    engine_ts = creat_engine_with_database('futures')
    futuresExchanges = ['CFFEX', 'DCE', 'CZCE', 'SHFE', 'INE', 'GFEX']
    for exchange in futuresExchanges:
        df = pro.fut_basic(**{"exchange": exchange}, fields=["symbol","exchange","ts_code","name","fut_code","trade_unit","per_unit","list_date","delist_date","d_month","last_ddate"])
        write_data(engine_ts, 'fut_basic', df)

# 获取所有期货合约的所有历史日行情数据
def get_fut_daily_data():
    engine_ts = creat_engine_with_database('futures')
    # 去除主力/连续合约
    sql = 'SELECT ts_code FROM fut_basic WHERE per_unit is not NULL'
    ts_code = read_data(engine_ts, 'fut_basic', sql)
    for i in range(0, len(ts_code)):
        # 若调用次数达到限制，则在一分钟内反复尝试
        for _ in range(60):
            try:
                df = pro.fut_daily(**{'ts_code': ts_code.loc[i]['ts_code']})
                if len(df):
                    write_data(engine_ts, 'fut_daily', df)
                else:
                    print('回调数据为空！期货合约代码:', ts_code.loc[i]['ts_code'])
            except:
                time.sleep(1)
            else:
                break
            
# 获取指定日期内所有期货合约的日行情数据
def get_fut_md_data(start_date = '', end_date = ''):
    engine_ts = creat_engine_with_database('futures')
    df = pro.fut_daily(**{"start_date": start_date, "end_date": end_date})
    write_data(engine_ts, 'fut_daily', df)
        
# 每日将新增的各类日行情自动导入对应的表中
def update_daily_md_data():
    yesterday = getYesterday()
    get_cb_md_data(yesterday, yesterday)
    get_fut_md_data(yesterday, yesterday)

if __name__ == '__main__':
    # 登录 Tushare 接口
    pro = ts.pro_api(token)
    
    # df = read_data('stock_basic', engine_ts)
    
    # update_daily_md_data()
    
    exit(1)
