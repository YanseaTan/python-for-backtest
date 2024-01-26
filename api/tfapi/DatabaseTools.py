# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-10-18
# @Last Modified by:   Yansea
# @Last Modified time: 2024-01-25

import pandas as pd
from sqlalchemy import create_engine

# 本地 mysql 数据库用户配置
mysql_user = 'root'
mysql_password = '0527'
mysql_addr = 'localhost'

# 服务器 postgre 数据库用户配置
postgre_user = 'postgres'
postgre_password = 'shan3353'
postgre_addr = '10.10.20.189:5432'

# 创建指定数据库操作引擎
def creat_mysql_engine(database):
    engine_ts = create_engine('mysql://{}:{}@/{}?charset=utf8&use_unicode=1'.format(mysql_user, mysql_password, database))
    return engine_ts

# 创建指定数据库操作引擎
def creat_postgre_engine(database):
    engine_ts = create_engine('postgresql://{}:{}@{}/{}?sslmode=disable'.format(postgre_user, postgre_password, postgre_addr, database))
    return engine_ts

# 获取指定数据库的指定表格内容
def read_data(engine_ts, sql):
    df = pd.read_sql_query(sql, engine_ts)
    return df

# 将指定内容写入指定数据库的指定表格中
def write_data(engine_ts, tableName, schemaName, df):
    df.to_sql(tableName, engine_ts, schemaName, index=False, if_exists='append', chunksize=5000)
