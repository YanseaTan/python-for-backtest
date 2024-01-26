# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-10-18
# @Last Modified by:   Yansea
# @Last Modified time: 2024-01-26

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
postgre_database = 'future'

# 创建 postgre 数据库操作引擎
postgre_engine_ts = create_engine('postgresql://{}:{}@{}/{}?sslmode=disable'.format(postgre_user, postgre_password, postgre_addr, postgre_database))

# 获取指定数据库的指定表格内容
def read_data(database, sql):
    engine_ts = create_engine('mysql://{}:{}@/{}?charset=utf8&use_unicode=1'.format(mysql_user, mysql_password, database))
    df = pd.read_sql_query(sql, engine_ts)
    return df

# 将指定内容写入指定数据库的指定表格中
def write_data(tableName, schemaName, df):
    engine_ts = create_engine('mysql://{}:{}@/{}?charset=utf8&use_unicode=1'.format(mysql_user, mysql_password, schemaName))
    df.to_sql(tableName, engine_ts, schemaName, index=False, if_exists='append', chunksize=5000)
    try:
        if schemaName == 'futures':
            schemaName = 'future'
        df.to_sql(tableName, postgre_engine_ts, schemaName, index=False, if_exists='append', chunksize=5000)
    except:
        print('写入服务器数据库失败！')
