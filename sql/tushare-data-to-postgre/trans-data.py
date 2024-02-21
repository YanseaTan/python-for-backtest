# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-01-25
# @Last Modified by:   Yansea
# @Last Modified time: 2024-02-19

import pandas as pd
from sqlalchemy import create_engine

# 数据库用户配置
user = 'root'
password = '0527'
addr = 'localhost'

# 创建指定数据库操作引擎
def creat_engine_with_database(database):
    engine_ts = create_engine('mysql://{}:{}@/{}?charset=utf8&use_unicode=1'.format(user, password, database))
    return engine_ts

# 获取指定数据库的指定表格内容
def read_data(engine_ts, sql):
    df = pd.read_sql_query(sql, engine_ts)
    return df

# 将指定内容写入指定数据库的指定表格中
def write_data(engine_ts, tableName, df):
    res = df.to_sql(tableName, engine_ts, index=False, if_exists='append', chunksize=5000)
    
def main():
    engine_ts = creat_engine_with_database('futures')
    sql = "select * from fut_spread_daily where trade_date > 20240130 and trade_date < 20240205"
    df = read_data(engine_ts, sql)
    
    print(df)
    
    engine_ts = create_engine('postgresql://postgres:shan3353@10.10.20.189:5432/{}?sslmode=disable'.format('future'))
    df.to_sql('fut_spread_daily', engine_ts, 'future', index=False, if_exists='append', chunksize=5000)
    


if __name__ == "__main__":
    main()