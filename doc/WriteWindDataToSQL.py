# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-02-01
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-19

import pandas as pd
import xlwings as xw
import sys
sys.path.append('.')
from tools.DatabaseTools import *
import os

# 服务器 postgre 数据库用户配置
postgre_user = 'postgres'
postgre_password = 'shan3353'
postgre_addr = '10.10.20.189:5432'
postgre_database = 'future'

# 创建 postgre 数据库操作引擎
postgre_engine_ts = create_engine('postgresql://{}:{}@{}/{}?sslmode=disable'.format(postgre_user, postgre_password, postgre_addr, postgre_database))

# 读取服务器数据库
def read_postgre_data(sql):
    df = pd.read_sql_query(sql, postgre_engine_ts)
    return df

def get_code_list(file_path):
    app = xw.App(visible = False, add_book = False)
    app.display_alerts = False
    app.screen_updating = False
    workbook = app.books.open(file_path)
    worksheet = workbook.sheets.active
    rng = worksheet.range("A2").expand("table")
    nRows = rng.rows.count
    code_list = []
    for i in range(2, nRows + 2):
        codeAddr = "A" + str(i)
        code = str(worksheet.range(codeAddr).value)
        code_list.append(code)
    workbook.close()
    app.quit()
    
    return code_list

def get_wind_code_data(code):
    # 本地 excel 处理，否则不能用 pandas 读取
    file_path = './doc/bond-data/{}.xlsx'.format(code)
    app = xw.App(visible = False, add_book = False)
    app.display_alerts = False
    app.screen_updating = False
    workbook = app.books.open(file_path)
    workbook.save()
    workbook.close()
    app.quit()
    
    data = pd.read_excel(file_path, skipfooter=2, names=['trade_date', 'close', 'change', 'pct_chg', 'vol', 'amount',
                                                        'yield_to_maturity', 'bp', 'cb_value', 'bond_value',
                                                        'cb_over_rate', 'bond_over_rate', 'low_over_rate'],
                         dtype={'trade_date': str})
    data = pd.DataFrame(data)
    data.dropna(axis=0, how='any', inplace=True)
    data.reset_index(drop=True, inplace=True)
    
    # 去除末期连续的无成交量数据，中期的数据保留
    for i in range(0, len(data)):
        vol = data.loc[i]['vol']
        if vol == 0:
            data.drop(index=i, inplace=True)
        else:
            break
    data.reset_index(drop=True, inplace=True)
    
    # 将日期转换为通用的字符串形式
    trade_date = data.trade_date.str.replace(' 00:00:00', '')
    trade_date = trade_date.str.replace('-', '')
    data.trade_date = trade_date
    
    # 加入转债代码列
    data.insert(0, 'ts_code', code)
    
    write_data('cb_daily_test', 'bond', data)

def write_wind_code_data_to_sql():
    code_list = get_code_list('./doc/bond-list.xlsx')
    for i in range(0, len(code_list)):
        code = code_list[i]
        get_wind_code_data(code)
        print("{} 日行情数据写入完毕，进度：{}%".format(code, round((i + 1) / len(code_list) * 100, 2)))

def get_wind_daily_data(file):
    # 本地 excel 处理，否则不能用 pandas 读取
    file_path = './doc/bond-daily-data/{}'.format(file)
    app = xw.App(visible = False, add_book = False)
    app.display_alerts = False
    app.screen_updating = False
    workbook = app.books.open(file_path)
    workbook.save()
    workbook.close()
    app.quit()
    
    data = pd.read_excel(file_path, skipfooter=2, names=['id', 'ts_code', 'cn_name', 'trade_date', 'close', 'change', 'pct_chg', 'amount',
                                                        'yield_to_maturity', 'cb_value', 'bond_value', 'cb_over_rate', 'bond_over_rate'],
                         dtype={'trade_date': str})
    data = pd.DataFrame(data)
    data.dropna(axis=0, how='any', inplace=True)
    data.drop(columns=['id', 'cn_name'], inplace=True)
    data.reset_index(drop=True, inplace=True)
    
    # 去除末期连续的无成交量数据，中期的数据保留
    for i in range(0, len(data)):
        amount = data.loc[i]['amount']
        if amount == 0:
            data.drop(index=i, inplace=True)
    data.reset_index(drop=True, inplace=True)
    
    # 将日期转换为通用的字符串形式
    trade_date = data.trade_date.str.replace(' 00:00:00', '')
    trade_date = trade_date.str.replace('-', '')
    data.trade_date = trade_date
    
    # 加入缺失列
    sql = "select ts_code, vol from bond.cb_daily where trade_date = '{}'".format(file[:8])
    vol_df = read_postgre_data(sql)
    
    vol_list = []
    for i in range(0, len(data)):
        code = data.loc[i]['ts_code']
        df = vol_df[vol_df.ts_code == code].copy()
        df.reset_index(drop=True, inplace=True)
        vol = df.loc[0]['vol']
        vol_list.append(vol)
        
    data.insert(5, 'vol', vol_list)
    data.insert(8, 'bp', None)
    data.insert(len(data.columns), 'low_over_rate', None)
    
    write_data('cb_daily_test', 'bond', data)

def write_wind_daily_data_to_sql():
    path = './doc/bond-daily-data'
    files = os.listdir(path)
    for i in range(0, len(files)):
        file = files[i]
        get_wind_daily_data(file)
        print("{} 日行情数据写入完毕，进度：{}%".format(file[:8], round((i + 1) / len(files) * 100, 2)))

def get_wind_treasury_bonds_data(file):
    # 本地 excel 处理，否则不能用 pandas 读取
    file_path = './doc/treasury-bonds-data/{}'.format(file)
    app = xw.App(visible = False, add_book = False)
    app.display_alerts = False
    app.screen_updating = False
    workbook = app.books.open(file_path)
    workbook.save()
    workbook.close()
    app.quit()
    
    data = pd.read_excel(file_path, skipfooter=2, names=['trade_date', 'bond_code', 'bond_price', 'yield', 'irr', 'spread', 'basis', 'rank'],
                         dtype={'trade_date': str})
    data = pd.DataFrame(data)
    data.dropna(axis=0, how='any', inplace=True)
    data.drop(columns=['rank'], inplace=True)
    data.reset_index(drop=True, inplace=True)
    
    # 去除末期连续的无成交量数据，中期的数据保留
    for i in range(0, len(data)):
        bond_price = data.loc[i]['bond_price']
        if bond_price == '--':
            data.drop(index=i, inplace=True)
    data.reset_index(drop=True, inplace=True)
    
    # 修改列属性
    data[['bond_price']] = data[['bond_price']].astype('float')
    data[['yield']] = data[['yield']].astype('float')
    data[['irr']] = data[['irr']].astype('float')
    data[['spread']] = data[['spread']].astype('float')
    data[['basis']] = data[['basis']].astype('float')
    
    # 将日期转换为通用的字符串形式
    trade_date = data.trade_date.str.replace(' 00:00:00', '')
    trade_date = trade_date.str.replace('-', '')
    data.trade_date = trade_date
    
    data.insert(1, 'ts_code', file[:file.index('-')] + '.CFX')
    rank = int(file[file.index('-') + 1:file.index('.')])
    data.insert(len(data.columns), 'rank', rank)
    
    write_data('treasury_bond_data', 'bond', data)

def write_wind_treasury_bonds_data_to_sql():
    path = './doc/treasury-bonds-data'
    files = os.listdir(path)
    for i in range(0, len(files)):
        file = files[i]
        get_wind_treasury_bonds_data(file)
        print("{} 国债期货可交割现券日行情数据写入完毕，进度：{}%".format(file[:file.index('-')], round((i + 1) / len(files) * 100, 2)))

def main():
    # write_wind_code_data_to_sql()
    # write_wind_daily_data_to_sql()
    write_wind_treasury_bonds_data_to_sql()


if __name__ == "__main__":
    main()
