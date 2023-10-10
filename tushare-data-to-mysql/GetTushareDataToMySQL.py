# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-10-10
# @Last Modified by:   Yansea
# @Last Modified time: 2023-10-10

import pandas as pd
import tushare as ts
from sqlalchemy import create_engine

pro = ts.pro_api('a526c0dd1419c44623d2257ad618848962a5ad988f36ced44ae33981')

def creat_engine_with_database(database):
    engine_ts = create_engine('mysql://root:0527@localhost/' + database + '?charset=utf8&use_unicode=1')
    return engine_ts

def read_data(tableName):
    sql = 'SELECT * FROM ' + tableName + ' LIMIT 20'
    df = pd.read_sql_query(sql, engine_ts)
    return df

def write_data(df, tableName, engine_ts):
    res = df.to_sql(tableName, engine_ts, index=False, if_exists='append', chunksize=5000)
    print('写入成功！数据量：', res)

def get_stock_basic_data():
    df = pro.stock_basic()
    return df

def get_cb_basic_data():
    df = pro.cb_basic(fields=["ts_code","bond_short_name","stk_code","stk_short_name","maturity","par","issue_price","issue_size",
                              "remain_size","value_date","maturity_date","coupon_rate","list_date","delist_date","exchange",
                              "conv_start_date","conv_end_date","conv_stop_date","first_conv_price","conv_price","add_rate"])
    return df

if __name__ == '__main__':
    engine_ts = creat_engine_with_database('bond')
    # df = read_data('stock_basic')
    # df = get_stock_basic_data()
    df = get_cb_basic_data()
    write_data(df, 'cb_basic', engine_ts)
