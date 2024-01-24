# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-01-19
# @Last Modified by:   Yansea
# @Last Modified time: 2024-01-24

from sqlalchemy import create_engine
import xlwings as xw
import datetime
import os
from DatabaseTools import *
import numpy as np

def get_max_drawdown_sys(array):
    array = pd.Series(array)
    cumsum = array.cummax()
    return max(cumsum-array)

def write_bond_data_to_xlsx():
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    
    start_date = '20220406'
    file_name = '76只转债综合'
    
    # 转债基础信息
    init_fund = 10000000
    # code_list = ['123118.SZ', '123129.SZ', '128143.SZ', '110084.SH', '123112.SZ', '123059.SZ', '123050.SZ', '123061.SZ', '123087.SZ',
    #              '128133.SZ', '111000.SH', '113600.SH', '128127.SZ', '113610.SH', '123082.SZ', '111001.SH', '127019.SZ']
    # 读取万得正股 excel 文件，获取转债列表
    app2 = xw.App(visible = True, add_book = False)
    app2.display_alerts = False
    app2.screen_updating = False
    workbook = app2.books.open('./bond-quantify/转债代码.xlsx')
    worksheet = workbook.sheets.active
    rng = worksheet.range("A2").expand("table")
    nRows = rng.rows.count
    code_list = []
    for i in range(2, nRows + 2):
        codeAddr = "A" + str(i)
        code_list.append(str(worksheet.range(codeAddr).value))
    workbook.close()
    app2.quit()
    
    # 股指期货基础信息
    fut_code = 'IC.CFX'
    fut_name = '中证500'
    fut_multiplier = 200
    
    # 对冲参数
    hedge_rate_list = [0.5, 1, 2, 4]
    
    # 获取纯转债多头净值数据
    result_list = [['对冲比例', '最大回撤', '最终收益', '风险收益比']]
    mini_worth = 1
    code_num = len(code_list)
    per_fund = init_fund / code_num
    fund_dict = {}
    fund_dict[start_date] = init_fund
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
            if trade_date in fund_dict.keys():
                fund_dict[trade_date] += fund
            else:
                fund_dict[trade_date] = fund
    
    trade_date_list = [k for k in fund_dict.keys()]
    worth_list = [v / init_fund for v in fund_dict.values()]
    for i in range(0, len(worth_list)):
        trade_date = trade_date_list[i]
        trade_date_list[i] = trade_date[:4] + '/' + trade_date[4:6] + '/' + trade_date[6:]
        worth = worth_list[i]
        worth_list[i] = round(worth, 4)
        mini_worth = min(mini_worth, worth)
    trade_date_list.insert(0, '日期')
    worth_list.insert(0, '转债多头净值')
    
    one_result_list = ['转债多头']
    max_drawdown = round(get_max_drawdown_sys(worth_list[1:]) * 100, 2)
    one_result_list.append(str(max_drawdown) + '%')
    profit = round((worth - 1) * 100, 2)
    one_result_list.append(str(profit) + '%')
    risk_rate = round(profit / max_drawdown, 1)
    if risk_rate > 0:
        one_result_list.append('1-' + str(risk_rate))
    else:
        one_result_list.append('/')
    result_list.append(one_result_list)
        
    # 获取股指期货空头净值数据
    fut_fund_dict = {}
    fut_worth_list = [1]
    engine_ts = creat_engine_with_database('futures')
    sql = "select trade_date, close from fut_daily where ts_code = '{}' and trade_date >= '{}' order by trade_date".format(fut_code, start_date)
    fut_close_df = read_data(engine_ts, sql)
    init_close = fut_close_df.loc[0]['close']
    fut_fund_dict[start_date] = init_close
    for i in range(1, len(fut_close_df) - 1):
        trade_date = fut_close_df.loc[i]['trade_date']
        close = fut_close_df.loc[i]['close']
        fut_fund_dict[trade_date] = init_close * 2 - close
        fut_worth = round(2 - (close / init_close), 4)
        fut_worth_list.append(fut_worth)
        mini_worth = min(mini_worth, fut_worth)
    fut_worth_list.insert(0, '{}空头净值'.format(fut_name))
    
    one_result_list = ['{}空头'.format(fut_name)]
    max_drawdown = round(get_max_drawdown_sys(fut_worth_list[1:]) * 100, 2)
    one_result_list.append(str(max_drawdown) + '%')
    profit = round((fut_worth - 1) * 100, 2)
    one_result_list.append(str(profit) + '%')
    risk_rate = round(profit / max_drawdown, 1)
    if risk_rate > 0:
        one_result_list.append('1-' + str(risk_rate))
    else:
        one_result_list.append('/')
    result_list.append(one_result_list)
        
    # 计算不同比例对冲的净值数据
    hedge_worth_list = []
    for rate in hedge_rate_list:
        one_result_list = ['1-{}'.format(round(1 / rate, 2))]
        one_hedge_worth_list = ['1-{} 对冲净值'.format(round(1 / rate, 2))]
        fut_vol = int(init_fund / rate / fut_multiplier / fut_close_df.loc[0]['close'])
        hedge_init_fund = init_fund + fut_vol * fut_close_df.loc[0]['close'] * fut_multiplier
        for trade_date in fund_dict.keys():
            hedge_worth = round((fund_dict[trade_date] + (fut_fund_dict[trade_date] * fut_vol * fut_multiplier)) / hedge_init_fund, 4)
            one_hedge_worth_list.append(hedge_worth)
        max_drawdown = round(get_max_drawdown_sys(one_hedge_worth_list[1:]) * 100, 2)
        one_result_list.append(str(max_drawdown) + '%')
        profit = round((hedge_worth - 1) * 100, 2)
        one_result_list.append(str(profit) + '%')
        risk_rate = round(profit / max_drawdown, 1)
        if risk_rate > 0:
            one_result_list.append('1-' + str(risk_rate))
        else:
            one_result_list.append('/')
        result_list.append(one_result_list)
        hedge_worth_list.append(one_hedge_worth_list)
    hedge_worth_list = list(map(list, zip(*hedge_worth_list)))
        
    # 写入内容
    ws = wb.sheets.add()
    ws.range('A1').options(transpose=True).value = trade_date_list
    ws.range('B1').options(transpose=True).value = worth_list
    ws.range('C1').options(transpose=True).value = fut_worth_list
    ws.range('D1').options(transpose=True).value = trade_date_list
    ws.range('E1').value = hedge_worth_list
    chara = chr(ord('E') + len(hedge_rate_list) + 1)
    ws.range('{}1'.format(chara)).value = result_list
    rng = ws.range('A1').expand()
    for i in range(0, 4 + len(hedge_rate_list)):
        rng.columns[i][0].color = (211, 211, 211)
    rng = ws.range('{}1'.format(chara)).expand()
    for i in range(0, 4):
        rng.columns[i][0].color = (200, 255, 200)
    ws.autofit()
    
    # 插入曲线
    # 转债多头-股指期货空头净值曲线
    cnt_of_date = len(worth_list)
    chart = ws.charts.add(20, 200, 650, 400)
    chart.set_source_data(ws.range((1,1),(cnt_of_date,3)))
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
    chart.api[1].ChartTitle.Text = "转债多头-{}空头净值曲线".format(fut_name)     #改变标题文本
    # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
    chart.api[1].Axes(2).TickLabels.NumberFormatLocal = "#,##0.00_);[红色](#,##0.00)"      # 纵坐标格式
    chart.api[1].Axes(2).MajorUnit = 0.02      # 纵坐标单位值
    chart.api[1].Axes(1).MajorUnit = 60      # 横坐标单位值
    chart.api[1].Legend.Position = -4107    # 图例显示在下方
    # chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
    chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "yy/mm/dd"      # 格式化横坐标显示
    chart.api[1].Axes(2).CrossesAt = mini_worth - 0.02
    chart.api[1].Axes(2).MinimumScale = mini_worth - 0.02
    chart.api[1].ChartStyle = 245       # 图表格式
    
    # 不同比例对冲净值曲线
    cnt_of_date = len(worth_list)
    chart = ws.charts.add(700, 200, 650, 400)
    chart.set_source_data(ws.range((1,4),(cnt_of_date,len(hedge_rate_list) + 4)))
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
    chart.api[1].ChartTitle.Text = "不同对冲比例净值曲线".format(fut_name)     #改变标题文本
    # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
    chart.api[1].Axes(2).TickLabels.NumberFormatLocal = "#,##0.00_);[红色](#,##0.00)"      # 纵坐标格式
    chart.api[1].Axes(2).MajorUnit = 0.02      # 纵坐标单位值
    chart.api[1].Axes(1).MajorUnit = 60      # 横坐标单位值
    chart.api[1].Legend.Position = -4107    # 图例显示在下方
    # chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
    chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "yy/mm/dd"      # 格式化横坐标显示
    chart.api[1].Axes(2).CrossesAt = mini_worth - 0.01
    chart.api[1].Axes(2).MinimumScale = mini_worth - 0.01
    chart.api[1].ChartStyle = 245       # 图表格式
    
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    if not os.path.exists('output/{}/'.format(todayStr)):
        os.makedirs('output/{}/'.format(todayStr))
    wb.save('./output/{}/{}-{}-{}股指期货对冲净值回测.xlsx'.format(todayStr, start_date, file_name, fut_name))
    wb.close()
    app.quit()
    print('转债-股指期货对冲净值回测 Excel 导出完毕！')
    

def main():
    write_bond_data_to_xlsx()


if __name__ == "__main__":
    main()
