# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-10-18
# @Last Modified by:   Yansea
# @Last Modified time: 2023-10-20

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
