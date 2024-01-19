# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-01-19
# @Last Modified by:   Yansea
# @Last Modified time: 2024-01-19

from sqlalchemy import create_engine
import xlwings as xw
import datetime
import os
from DatabaseTools import *
import numpy as np

def write_bond_data_to_xlsx():
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    
    init_fund = 10000000
    start_date = '20220401'
    code_list = ['123118.SZ', '123129.SZ', '128143.SZ', '110084.SH', '123112.SZ', '123059.SZ', '123050.SZ', '123061.SZ', '123087.SZ',
                 '128133.SZ', '111000.SH', '113600.SH', '128127.SZ', '113610.SH', '123082.SZ', '111001.SH', '127019.SZ']
    code_num = len(code_list)
    per_fund = init_fund / code_num
    total_fund_dict = {}
    total_fund_dict[start_date] = init_fund
    
    engine_ts = creat_engine_with_database('bond')
    for code in code_list:
        sql = "select close, trade_date from cb_daily where ts_code = '{}' and trade_date >= '{}' order by trade_date".format(code, start_date)
        close_df = read_data(engine_ts, sql)
        init_close = close_df.loc[0]['close']
        vol = int(per_fund / init_close)
        for i in range(1, len(close_df) - 1):
            close = close_df.loc[i]['close']
            trade_date = close_df.loc[i]['trade_date']
            fund = vol * close
            if trade_date in total_fund_dict.keys():
                total_fund_dict[trade_date] += fund
            else:
                total_fund_dict[trade_date] = fund
    
    total_fund_list = [[k, v] for k, v in total_fund_dict.items()]
    for i in range(0, len(total_fund_list)):
        trade_date = total_fund_list[i][0]
        total_fund_list[i][0] = trade_date[:4] + '/' + trade_date[4:6] + '/' + trade_date[6:]
        close = total_fund_list[i][1]
        total_fund_list[i][1] = round(close, 2)
    
    # 写入标题
    ws = wb.sheets.add()
    title = ['日期', '总资金']
    ws.range('A1').value = title
    rng = ws.range('A1').expand()
    for i in range(0, len(title)):
        rng.columns[i][0].color = (211, 211, 211)
        
    # 写入内容
    ws.range('A2').value = total_fund_list
    
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    if not os.path.exists('output/{}/'.format(todayStr)):
        os.makedirs('output/{}/'.format(todayStr))
    wb.save('./output/{}/{}-转债量化回测.xlsx'.format(todayStr, todayStr))
    wb.close()
    app.quit()
    print('转债量化回测 Excel 导出完毕！')
    

def main():
    write_bond_data_to_xlsx()


if __name__ == "__main__":
    main()
