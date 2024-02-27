# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-02-26
# @Last Modified by:   Yansea
# @Last Modified time: 2024-02-27

import pandas as pd
import xlwings as xw
import datetime
import time
import os
from copy import deepcopy
from sqlalchemy import column, create_engine
import sys
sys.path.append('./backtest-frame/api/')
from api.BackTestApi import *

def main():
    start_date = '20190101'
    end_date = '20240201'
    fut_code = 'IC'
    
    cal_date_list = get_cal_date_list(start_date, end_date)
    sql = "select ts_code, trade_date, close, vol, oi from future.fut_daily where trade_date >= '{}' and trade_date <= '{}'".format(start_date, end_date)
    fut_daily_md_df = read_postgre_data(sql)
    fut_daily_md_df = fut_daily_md_df[((fut_daily_md_df.ts_code.str.startswith(fut_code)) & (fut_daily_md_df.ts_code.str.len() > 6))]
    sql = "select update_date, value from future.fut_funds where index_name = '中证500' and update_date >= '{}' and update_date <= '{}'".format(start_date, end_date)
    index_daily_md_df = read_postgre_data(sql)
    
    index_close_list = ['股指收盘价']
    fut_ts_code_list = ['股指期货主力合约']
    fut_close_list = ['合约收盘价']
    fut_diff_list = ['合约升贴水']
    fut_days_list = ['合约剩余天数']
    date_list = ['日期']
    value_list = ['年化升贴水率']
    mini_value = 0
    for i in range(0, len(cal_date_list) - 2):
        last_trade_date = cal_date_list[i]
        trade_date = cal_date_list[i + 1]
        next_trade_date = cal_date_list[i + 2]
        
        fut_md_df = fut_daily_md_df[(fut_daily_md_df.trade_date == last_trade_date)].copy()
        fut_md_df.sort_values(by='ts_code', ascending=True, inplace=True)
        fut_md_df.reset_index(drop=True, inplace=True)
        fut_ts_code = fut_md_df.loc[3]['ts_code']
        fut_md_df = fut_daily_md_df[((fut_daily_md_df.trade_date == trade_date) & (fut_daily_md_df.ts_code == fut_ts_code))].copy()
        fut_md_df.reset_index(drop=True, inplace=True)
        fut_clsoe = fut_md_df.loc[0]['close']
        
        # fut_ts_code = fut_md_df.loc[0]['ts_code']
        # fut_clsoe = fut_md_df.loc[0]['close']
        # # 检查合约代码在当前以及下一个交易日是否存在交易
        # new_fut_md_df = fut_daily_md_df[(fut_daily_md_df.trade_date == trade_date)].copy()
        # next_fut_md_df = fut_daily_md_df[(fut_daily_md_df.trade_date == next_trade_date)].copy()
        # code_df = new_fut_md_df[new_fut_md_df.ts_code == fut_ts_code].copy()
        # code_df.reset_index(drop=True, inplace=True)
        # next_code_df = next_fut_md_df[next_fut_md_df.ts_code == fut_ts_code].copy()
        # next_code_df.reset_index(drop=True, inplace=True)
        # if len(code_df) == 0 or code_df.loc[0]['vol'] == 0 or len(next_code_df) == 0 or next_code_df.loc[0]['vol'] == 0:
        #     fut_ts_code = fut_md_df.loc[1]['ts_code']
        #     fut_clsoe = fut_md_df.loc[1]['close']
            
        fut_ts_code_list.append(fut_ts_code)
        fut_close_list.append(fut_clsoe)
            
        fut_md_df = fut_daily_md_df[(fut_daily_md_df.ts_code == fut_ts_code) & (fut_daily_md_df.trade_date >= next_trade_date)].copy()
        days = len(fut_md_df)
        fut_days_list.append(days)
            
        index_md_df = index_daily_md_df[index_daily_md_df.update_date == trade_date].copy()
        index_md_df.reset_index(drop=True, inplace=True)
        index_close = index_md_df.loc[0]['value']
        index_close_list.append(index_close)
        
        date = trade_date[:4] + '/' + trade_date[4:6] + '/' + trade_date[6:8]
        date_list.append(date)
        
        fut_diff = index_close - fut_clsoe
        fut_diff_list.append(fut_diff)
        value = round(fut_diff * 250 * 100 / index_close / days, 2)
        value_list.append(value)
        mini_value = min(mini_value, value)
        
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    ws = wb.sheets.add()
    
    ws.range('A1').options(transpose=True).value = fut_ts_code_list
    ws.range('B1').options(transpose=True).value = index_close_list
    ws.range('C1').options(transpose=True).value = fut_close_list
    ws.range('D1').options(transpose=True).value = fut_diff_list
    ws.range('E1').options(transpose=True).value = fut_days_list
    ws.range('F1').options(transpose=True).value = date_list
    ws.range('G1').options(transpose=True).value = value_list
    
    ws.autofit()
    
    cnt_of_date = len(date_list)
    chart = ws.charts.add(20, 120, 800, 400)
    chart.set_source_data(ws.range((1,6),(cnt_of_date,7)))
    # Excel VBA 指令
    chart.chart_type = 'xy_scatter_lines_no_markers'
    chart.api[1].SetElement(2)          #显示标题
    chart.api[1].SetElement(101)        #显示图例
    chart.api[1].SetElement(301)        #x轴标题
    # chart.api[1].SetElement(311)      #y轴标题
    chart.api[1].SetElement(305)        #y轴的网格线
    # chart.api[1].SetElement(334)      #x轴的网格线
    chart.api[1].Axes(1).AxisTitle.Text = "日期"          #x轴标题的名字
    # chart.api[1].Axes(2).AxisTitle.Text = "价差"             #y轴标题的名字
    chart.api[1].ChartTitle.Text = "下下个季度合约连续年化升贴水率%"     #改变标题文本
    # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
    # chart.api[1].Axes(2).TickLabels.NumberFormatLocal = "#,##0.00_);[红色](#,##0.00)"      # 纵坐标格式
    chart.api[1].Axes(2).MaximumScale = 50
    chart.api[1].Axes(2).MinimumScale = -50
    chart.api[1].Axes(2).MajorUnit = 5      # 纵坐标单位值
    chart.api[1].Axes(1).MajorUnit = int(len(date_list) / 8)      # 横坐标单位值
    chart.api[1].Legend.Position = -4107    # 图例显示在下方
    # chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
    chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "yy/mm/dd"      # 格式化横坐标显示
    chart.api[1].Axes(2).CrossesAt = mini_value - 2
    chart.api[1].Axes(2).MinimumScale = mini_value - 2
    chart.api[1].ChartStyle = 245       # 图表格式
    
    wb.save('IC隔季度合约连续升贴水情况.xlsx')
    wb.close()
    app.quit()
        
        
        


if __name__ == "__main__":
    main()
