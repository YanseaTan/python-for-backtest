# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-10-18
# @Last Modified by:   Yansea
# @Last Modified time: 2024-01-03

import pandas as pd
from sqlalchemy import create_engine

# 创建指定数据库操作引擎
def creat_engine_with_database(database):
    engine_ts = create_engine('postgresql://postgres:shan3353@10.10.20.189:5432/{}?sslmode=disable'.format(database))
    return engine_ts

# 获取指定数据库的指定表格内容
def read_data(engine_ts, sql):
    df = pd.read_sql_query(sql, engine_ts)
    return df

# 将指定内容写入指定数据库的指定表格中
def write_data(engine_ts, tableName, schemaName, df):
    df.to_sql(tableName, engine_ts, schemaName, index=False, if_exists='append', chunksize=5000)
