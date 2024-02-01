# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-02-01
# @Last Modified by:   Yansea
# @Last Modified time: 2024-02-01

import pandas as pd
import xlwings as xw
import sys
sys.path.append('.')
from tools.DatabaseTools import *

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

def get_wind_data(code):
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

def write_wind_data_to_sql():
    code_list = get_code_list('./doc/bond-list.xlsx')
    for i in range(0, len(code_list)):
        code = code_list[i]
        get_wind_data(code)
        print("{} 日行情数据写入完毕，进度：{}%".format(code, round((i + 1) / len(code_list) * 100, 2)))
    

def main():
    write_wind_data_to_sql()


if __name__ == "__main__":
    main()
