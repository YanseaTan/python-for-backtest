# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-01-19
# @Last Modified by:   Yansea
# @Last Modified time: 2024-01-30

from mimetypes import init
from re import sub
from sqlalchemy import create_engine
import xlwings as xw
import datetime
import os
import tushare as ts
from DatabaseTools import *
import numpy as np
from copy import deepcopy

# Tushare 账户 token
token = 'e59d203345b5dac84a150b2abb7b49dcb06b6c2abefa7bc49c06bea1'

def get_max_drawdown_sys(array):
    array = pd.Series(array)
    cumsum = array.cummax()
    return max(cumsum-array)

def write_bond_fut_data_to_xlsx():
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    
    start_date = '20190101'
    today = datetime.date.today()
    end_date = today.strftime('%Y%m%d')
    end_date = '20190419'
    file_name = '113只转债综合'
    
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
    hedge_rate_list = [1, 2, 4, 10]
    
    # 获取纯转债多头净值数据
    mini_worth = 1
    code_num = len(code_list)
    per_fund = init_fund / code_num
    fund_dict = {}
    fund_dict[start_date] = init_fund
    engine_ts = creat_engine_with_database('bond')
    for code in code_list:
        sql = "select close, trade_date from cb_daily where ts_code = '{}' and trade_date >= '{}' and trade_date <= '{}' order by trade_date".format(code, start_date, end_date)
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
    
    result_list = [['对冲比例', '最大回撤', '最终收益', '风险收益比']]
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
    sql = "select trade_date, close from fut_daily where ts_code = '{}' and trade_date >= '{}' and trade_date <= '{}' order by trade_date".format(fut_code, start_date, end_date)
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
    mini_hedge_worth = 1
    hedge_worth_list = []
    for rate in hedge_rate_list:
        one_result_list = ['1-{}'.format(round(1 / rate, 2))]
        one_hedge_worth_list = ['1-{} 对冲净值'.format(round(1 / rate, 2))]
        fut_vol = int(init_fund / rate / fut_multiplier / fut_close_df.loc[0]['close'])
        hedge_init_fund = init_fund + fut_vol * fut_close_df.loc[0]['close'] * fut_multiplier
        for trade_date in fund_dict.keys():
            hedge_worth = round((fund_dict[trade_date] + (fut_fund_dict[trade_date] * fut_vol * fut_multiplier)) / hedge_init_fund, 4)
            one_hedge_worth_list.append(hedge_worth)
            mini_hedge_worth = min(mini_hedge_worth, hedge_worth)
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
    chart.api[1].Axes(2).CrossesAt = mini_hedge_worth - 0.02
    chart.api[1].Axes(2).MinimumScale = mini_hedge_worth - 0.02
    chart.api[1].ChartStyle = 245       # 图表格式
    
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    if not os.path.exists('output/{}/'.format(todayStr)):
        os.makedirs('output/{}/'.format(todayStr))
    wb.save('./output/{}/{}-{}-{}-{}股指期货对冲净值回测.xlsx'.format(todayStr, start_date, end_date, file_name, fut_name))
    wb.close()
    app.quit()
    print('转债-股指期货对冲净值回测 Excel 导出完毕！')

# 吾股排名转债多头
def write_wugu_bond_data_to_xlsx():
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    
    start_date = '20210104'
    today = datetime.date.today()
    end_date = today.strftime('%Y%m%d')
    file_name = '486只转债轮动'
    
    # 转债基础信息
    init_fund = 10000000
    # 买入上限
    highest_buy = 120
    # 卖出阈值
    lowest_sell = 150
    
    # 读取吾股排名 excel 文件，获取转债列表
    app2 = xw.App(visible = True, add_book = False)
    app2.display_alerts = False
    app2.screen_updating = False
    workbook = app2.books.open('./bond-quantify/吾股排名.xlsx')
    worksheet = workbook.sheets.active
    rng = worksheet.range("A2").expand("table")
    nRows = rng.rows.count
    code_list = []
    for i in range(2, nRows + 2):
        codeStr = str(worksheet.range("A" + str(i)).value)
        if codeStr[:2] == '11':
            codeStr = codeStr[:6] + '.SH'
        else:
            codeStr = codeStr[:6] + '.SZ'
        code_list.append(codeStr)
    workbook.close()
    app2.quit()
    
    # 每 100 个为一组进行回测
    sub_code_list = []
    engine_ts = creat_engine_with_database('bond')
    for i in range(0, len(code_list)):
        sub_code_list.append(code_list[i])
        if len(sub_code_list) > 99 or i == (len(code_list) - 1):
            close_dict = {}
            ipo_date_dict = {}
            resub_code_list = []
            for j in range(0, len(sub_code_list)):
                sql = "select trade_date, close from cb_daily where ts_code = '{}' order by trade_date".format(sub_code_list[j])
                close_df = read_data(engine_ts, sql)
                if close_df.loc[0]['close'] > highest_buy:
                    resub_code_list.append(sub_code_list[j])
                    continue
                trade_date_list = close_df['trade_date'].values.tolist()
                close_list = close_df['close'].values.tolist()
                close_dict[sub_code_list[j]] = dict(zip(trade_date_list, close_list))
                ipo_date = close_df.loc[0]['trade_date']
                if ipo_date > start_date:
                    if ipo_date in ipo_date_dict.keys():
                        ipo_date_dict[ipo_date].append(sub_code_list[j])
                    else:
                        ipo_date_dict[ipo_date] = [sub_code_list[j]]
                    resub_code_list.append(sub_code_list[j])
            sub_code_list = list(set(sub_code_list) - set(resub_code_list))
            
            # 计算总资金变化
            sql = "select trade_date from cb_daily where ts_code = '{}' and trade_date >= '{}' order by trade_date".format(sub_code_list[0], start_date)
            trade_date_df = read_data(engine_ts, sql)
            trade_date_list = trade_date_df['trade_date'].values.tolist()
            mini_worth = 1
            per_fund = init_fund / len(sub_code_list)
            remain_fund = init_fund
            fund_dict = {}
            fund_dict[start_date] = init_fund
            for j in range(1, len(trade_date_list)):
                num_list = []
                resub_code_list = []
                trade_date = trade_date_list[j]
                for k in range(0, len(sub_code_list)):
                    close = close_dict[sub_code_list[k]][trade_date_list[j - 1]]
                    num = int(per_fund / close)
                    num_list.append(num)
                    remain_fund -= num * close
                for k in range(0, len(sub_code_list)):
                    close = close_dict[sub_code_list[k]][trade_date]
                    remain_fund += num_list[k] * close
                    if close > lowest_sell:
                        resub_code_list.append(sub_code_list[k])
                fund_dict[trade_date] = remain_fund
                if trade_date in ipo_date_dict.keys():
                    sub_code_list += ipo_date_dict[trade_date]
                sub_code_list = list(set(sub_code_list) - set(resub_code_list))
            
            # 计算净值
            worth_list = [v / init_fund for v in fund_dict.values()]
            for j in range(0, len(worth_list)):
                trade_date = trade_date_list[j]
                trade_date_list[j] = trade_date[:4] + '/' + trade_date[4:6] + '/' + trade_date[6:]
                worth = worth_list[j]
                worth_list[j] = round(worth, 4)
                mini_worth = min(mini_worth, worth)
            trade_date_list.insert(0, '日期')
            worth_list.insert(0, '转债多头净值')
            
            # 计算分析结果
            result_list = [['对冲比例', '最大回撤', '最终收益', '风险收益比']]
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
            
            # 写入内容
            ws = wb.sheets.add()
            ws.range('A1').options(transpose=True).value = trade_date_list
            ws.range('B1').options(transpose=True).value = worth_list
            ws.range('D1').value = result_list
            rng = ws.range('A1').expand()
            for j in range(0, 2):
                rng.columns[j][0].color = (211, 211, 211)
            rng = ws.range('D1').expand()
            for j in range(0, 4):
                rng.columns[j][0].color = (200, 255, 200)
            ws.autofit()
            
            # 插入曲线
            # 转债多头净值曲线
            cnt_of_date = len(worth_list)
            chart = ws.charts.add(20, 200, 650, 400)
            chart.set_source_data(ws.range((1,1),(cnt_of_date,2)))
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
            chart.api[1].ChartTitle.Text = "转债多头净值曲线"     #改变标题文本
            # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
            chart.api[1].Axes(2).TickLabels.NumberFormatLocal = "#,##0.00_);[红色](#,##0.00)"      # 纵坐标格式
            chart.api[1].Axes(2).MajorUnit = 0.02      # 纵坐标单位值
            chart.api[1].Axes(1).MajorUnit = 150      # 横坐标单位值
            chart.api[1].Legend.Position = -4107    # 图例显示在下方
            # chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
            chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "yy/mm/dd"      # 格式化横坐标显示
            chart.api[1].Axes(2).CrossesAt = mini_worth - 0.02
            chart.api[1].Axes(2).MinimumScale = mini_worth - 0.02
            chart.api[1].ChartStyle = 245       # 图表格式
            
            sub_code_list = []
    
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    if not os.path.exists('output/{}/'.format(todayStr)):
        os.makedirs('output/{}/'.format(todayStr))
    wb.save('./output/{}/{}-{}-{}净值回测.xlsx'.format(todayStr, start_date, end_date, file_name))
    wb.close()
    app.quit()
    print('转债净值回测 Excel 导出完毕！')

# 吾股排名转债股指对冲
def write_wugu_bond_fut_data_to_xlsx():
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    
    start_date = '20210104'
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    end_date = (today - oneday).strftime('%Y%m%d')
    file_name = '300只转债轮动-股指对冲'
    
    # 转债基础信息
    init_fund = 10000000
    # 买入上限
    highest_buy = 120
    # 卖出阈值
    lowest_sell = 150
    
    # 股指期货基础信息
    fut_code = 'IC.CFX'
    fut_name = '中证500'
    fut_multiplier = 200
    margin_rate = 0.12
    margin_redundancy = 0.8
    hedge_rate_list = [0.4, 0.5, 0.6]
    
    # 读取吾股排名 excel 文件，获取转债列表
    app2 = xw.App(visible = True, add_book = False)
    app2.display_alerts = False
    app2.screen_updating = False
    workbook = app2.books.open('./bond-quantify/吾股排名.xlsx')
    worksheet = workbook.sheets.active
    rng = worksheet.range("A2").expand("table")
    nRows = rng.rows.count
    code_list = []
    for i in range(2, nRows + 2):
        codeStr = str(worksheet.range("A" + str(i)).value)
        if codeStr[:2] == '11':
            codeStr = codeStr[:6] + '.SH'
        else:
            codeStr = codeStr[:6] + '.SZ'
        code_list.append(codeStr)
    workbook.close()
    app2.quit()
    
    # 登录 Tushare 接口，获取交易日历
    pro = ts.pro_api(token)
    cal_date_df = pro.trade_cal(**{"start_date": start_date, "end_date": end_date, "is_open": "1"}, fields=["cal_date"])
    cal_date_list = sorted(cal_date_df["cal_date"].tolist())
    
    # 获取股指期货的收盘价信息
    engine_ts = creat_engine_with_database('futures')
    sql = "select trade_date, close from fut_daily where ts_code = '{}' and trade_date >= '{}' and trade_date <= '{}' order by trade_date".format(fut_code, start_date, end_date)
    fut_close_df = read_data(engine_ts, sql)
    trade_date_list = fut_close_df['trade_date'].values.tolist()
    close_list = fut_close_df['close'].values.tolist()
    fut_close_dict = dict(zip(trade_date_list, close_list))
    
    # 计算每个交易日的股指空头资金变化
    fut_init_fund = fut_close_dict[start_date]
    fut_fund_list = [fut_init_fund]
    fut_remain_fund = fut_init_fund
    for i in range(1, len(cal_date_list)):
        trade_date = cal_date_list[i]
        fut_pre_close = fut_close_dict[cal_date_list[i - 1]]
        fut_close = fut_close_dict[trade_date]
        fut_remain_fund += (fut_pre_close - fut_close)
        fut_fund_list.append(fut_remain_fund)
        
    # 股指空头净值
    fut_mini_worth = 1
    fut_worth_list = [round(v / fut_init_fund, 4) for v in fut_fund_list]
    fut_mini_worth = min(fut_mini_worth, min(fut_worth_list))
    fut_worth_list.insert(0, '{}空头净值'.format(fut_name))
    
    # 结果分析
    fut_result_list = ['{}空头'.format(fut_name)]
    max_drawdown = round(get_max_drawdown_sys(fut_worth_list[1:]) * 100, 2)
    fut_result_list.append(str(max_drawdown) + '%')
    profit = round((fut_worth_list[len(fut_worth_list) - 1] - 1) * 100, 2)
    fut_result_list.append(str(profit) + '%')
    risk_rate = round(profit / max_drawdown, 1)
    if risk_rate > 0:
        fut_result_list.append('1-' + str(risk_rate))
    else:
        fut_result_list.append('/')
    
    # 每 100 个为一组进行回测
    sub_code_list = []
    engine_ts = creat_engine_with_database('bond')
    for i in range(0, len(code_list)):
        sub_code_list.append(code_list[i])
        if len(sub_code_list) > 99 or i == (len(code_list) - 1):
            close_dict = {}
            ipo_date_dict = {}
            resub_code_list = []
            for j in range(0, len(sub_code_list)):
                sql = "select trade_date, close from cb_daily where ts_code = '{}' order by trade_date".format(sub_code_list[j])
                close_df = read_data(engine_ts, sql)
                if close_df.loc[0]['close'] > highest_buy:
                    resub_code_list.append(sub_code_list[j])
                    continue
                trade_date_list = close_df['trade_date'].values.tolist()
                close_list = close_df['close'].values.tolist()
                close_dict[sub_code_list[j]] = dict(zip(trade_date_list, close_list))
                ipo_date = close_df.loc[0]['trade_date']
                if ipo_date > start_date:
                    if ipo_date in ipo_date_dict.keys():
                        ipo_date_dict[ipo_date].append(sub_code_list[j])
                    else:
                        ipo_date_dict[ipo_date] = [sub_code_list[j]]
                    resub_code_list.append(sub_code_list[j])
            sub_code_list = list(set(sub_code_list) - set(resub_code_list))
            sub_code_list_copy = deepcopy(sub_code_list)
            
            # 计算总资金变化
            sql = "select trade_date from cb_daily where ts_code = '{}' and trade_date >= '{}' order by trade_date".format(sub_code_list_copy[0], start_date)
            trade_date_df = read_data(engine_ts, sql)
            trade_date_list = trade_date_df['trade_date'].values.tolist()
            per_fund = init_fund / len(sub_code_list_copy)
            remain_fund = init_fund
            fund_dict = {}
            fund_dict[start_date] = init_fund
            for j in range(1, len(trade_date_list)):
                num_list = []
                resub_code_list = []
                trade_date = trade_date_list[j]
                for k in range(0, len(sub_code_list_copy)):
                    close = close_dict[sub_code_list_copy[k]][trade_date_list[j - 1]]
                    num = int(per_fund / close)
                    num_list.append(num)
                    remain_fund -= num * close
                for k in range(0, len(sub_code_list_copy)):
                    close = close_dict[sub_code_list_copy[k]][trade_date]
                    remain_fund += num_list[k] * close
                    if close > lowest_sell:
                        resub_code_list.append(sub_code_list_copy[k])
                fund_dict[trade_date] = remain_fund
                if trade_date in ipo_date_dict.keys():
                    sub_code_list_copy += ipo_date_dict[trade_date]
                sub_code_list_copy = list(set(sub_code_list_copy) - set(resub_code_list))
            
            # 计算净值
            mini_worth = min(1, fut_mini_worth)
            worth_list = [v / init_fund for v in fund_dict.values()]
            for j in range(0, len(worth_list)):
                trade_date = trade_date_list[j]
                trade_date_list[j] = trade_date[:4] + '/' + trade_date[4:6] + '/' + trade_date[6:]
                worth = worth_list[j]
                worth_list[j] = round(worth, 4)
                mini_worth = min(mini_worth, worth)
            trade_date_list.insert(0, '日期')
            worth_list.insert(0, '转债多头净值')
            
            # 计算分析结果
            result_list = [['对冲比例', '最大回撤', '最终收益', '风险收益比']]
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
            result_list.append(fut_result_list)
            
            # 不同对冲风格的资金变化
            hedge_fund_list = [[init_fund] * len(hedge_rate_list)]
            hedge_remain_fund_list = [init_fund] * len(hedge_rate_list)
            per_fund_list = []
            fut_vol_list = []
            for j in range(0, len(hedge_rate_list)):
                hedge_rate = hedge_rate_list[j]
                bond_fund = init_fund / (1 + margin_rate * hedge_rate)
                per_fund = bond_fund / len(sub_code_list)
                per_fund_list.append(per_fund)
                fut_fund = init_fund - bond_fund
                fut_vol = fut_fund * margin_redundancy / margin_rate / fut_multiplier / fut_close_dict[start_date]
                fut_vol_list.append(fut_vol)
                
            for j in range(1, len(cal_date_list)):
                num_list = []
                resub_code_list = []
                trade_date = cal_date_list[j]
                # 买入
                for k in range(0, len(sub_code_list)):
                    code = sub_code_list[k]
                    close = close_dict[code][cal_date_list[j - 1]]
                    sub_num_list = []
                    for m in range(0, len(hedge_rate_list)):
                        num = int(per_fund_list[m] / close)
                        sub_num_list.append(num)
                        hedge_remain_fund_list[m] -= num * close
                    num_list.append(sub_num_list)
                for m in range(0, len(hedge_rate_list)):
                    close = fut_close_dict[cal_date_list[j - 1]]
                    hedge_remain_fund_list[m] += close * fut_multiplier * fut_vol_list[m]
                # 卖出
                for k in range(0, len(sub_code_list)):
                    code = sub_code_list[k]
                    if trade_date in close_dict[code].keys():
                        close = close_dict[code][trade_date]
                    else:
                        close = close_dict[code][cal_date_list[j - 1]]
                        resub_code_list.append(code)
                    for m in range(0, len(hedge_rate_list)):
                        hedge_remain_fund_list[m] += num_list[k][m] * close
                    if close > lowest_sell:
                        resub_code_list.append(code)
                for m in range(0, len(hedge_rate_list)):
                    close = fut_close_dict[trade_date]
                    hedge_remain_fund_list[m] -= close * fut_multiplier * fut_vol_list[m]
                hedge_fund_list.append(deepcopy(hedge_remain_fund_list))
                
                if trade_date in ipo_date_dict.keys():
                    sub_code_list += ipo_date_dict[trade_date]
                
                if len(resub_code_list):
                    sub_code_list = list(set(sub_code_list) - set(resub_code_list))
                    
            # 对冲净值
            hedge_worth_list = []
            for j in range(0, len(cal_date_list)):
                sub_hedge_worth_list = [round(v / init_fund, 4) for v in hedge_fund_list[j]]
                hedge_worth_list.append(sub_hedge_worth_list)
            hedge_worth_list = list(map(list, zip(*hedge_worth_list)))
            
            hedge_mini_worth = 1
            for j in range(0, len(hedge_rate_list)):
                hedge_rate = hedge_rate_list[j]
                one_result_list = ['1-{}'.format(hedge_rate)]
                one_hedge_worth_list = hedge_worth_list[j]
                hedge_mini_worth = min(hedge_mini_worth, min(one_hedge_worth_list))
                max_drawdown = round(get_max_drawdown_sys(one_hedge_worth_list) * 100, 2)
                one_result_list.append(str(max_drawdown) + '%')
                profit = round((one_hedge_worth_list[len(one_hedge_worth_list) - 1] - 1) * 100, 2)
                one_result_list.append(str(profit) + '%')
                risk_rate = round(profit / max_drawdown, 1)
                if risk_rate > 0:
                    one_result_list.append('1-' + str(risk_rate))
                else:
                    one_result_list.append('/')
                result_list.append(one_result_list)
                hedge_worth_list[j].insert(0, '1-{}'.format(hedge_rate))
            
            # 日期格式规范化
            cal_date_list_copy = deepcopy(cal_date_list)
            for j in range(0, len(cal_date_list_copy)):
                trade_date = cal_date_list_copy[j]
                cal_date_list_copy[j] = trade_date[:4] + '/' + trade_date[4:6] + '/' + trade_date[6:]
            cal_date_list_copy.insert(0, '日期')
            
            # 写入内容
            ws = wb.sheets.add()
            ws.range('A1').options(transpose=True).value = trade_date_list
            ws.range('B1').options(transpose=True).value = worth_list
            ws.range('C1').options(transpose=True).value = fut_worth_list
            ws.range('D1').options(transpose=True).value = cal_date_list_copy
            ws.range('E1').options(transpose=True).value = hedge_worth_list
            chara = chr(ord('E') + len(hedge_rate_list) + 1)
            ws.range('{}1'.format(chara)).value = result_list
            rng = ws.range('A1').expand()
            for j in range(0, 4 + len(hedge_rate_list)):
                rng.columns[j][0].color = (211, 211, 211)
            rng = ws.range('{}1'.format(chara)).expand()
            for j in range(0, 4):
                rng.columns[j][0].color = (200, 255, 200)
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
            chart.api[1].Axes(1).MajorUnit = 150      # 横坐标单位值
            chart.api[1].Legend.Position = -4107    # 图例显示在下方
            # chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
            chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "yy/mm/dd"      # 格式化横坐标显示
            chart.api[1].Axes(2).CrossesAt = mini_worth - 0.02
            chart.api[1].Axes(2).MinimumScale = mini_worth - 0.02
            chart.api[1].ChartStyle = 245       # 图表格式
            
            # 对冲净值曲线
            cnt_of_date = len(hedge_worth_list[0])
            chart = ws.charts.add(700, 200, 650, 400)
            chart.set_source_data(ws.range((1,4),(cnt_of_date,4 + len(hedge_rate_list))))
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
            chart.api[1].ChartTitle.Text = "不同对冲比例净值曲线"     #改变标题文本
            # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
            chart.api[1].Axes(2).TickLabels.NumberFormatLocal = "#,##0.00_);[红色](#,##0.00)"      # 纵坐标格式
            chart.api[1].Axes(2).MajorUnit = 0.02      # 纵坐标单位值
            chart.api[1].Axes(1).MajorUnit = 60      # 横坐标单位值
            chart.api[1].Legend.Position = -4107    # 图例显示在下方
            # chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
            chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "yy/mm/dd"      # 格式化横坐标显示
            chart.api[1].Axes(2).CrossesAt = hedge_mini_worth - 0.02
            chart.api[1].Axes(2).MinimumScale = hedge_mini_worth - 0.02
            chart.api[1].ChartStyle = 245       # 图表格式
            
            sub_code_list = []
    
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    if not os.path.exists('output/{}/'.format(todayStr)):
        os.makedirs('output/{}/'.format(todayStr))
    wb.save('./output/{}/{}-{}-{}净值回测.xlsx'.format(todayStr, start_date, end_date, file_name))
    wb.close()
    app.quit()
    print('转债净值回测 Excel 导出完毕！')

def write_turned_bond_fut_data_to_xlsx():
    # 转债基础信息
    init_fund = 100000000
    file_name = '按月轮换转债'
    
    # 股指期货基础信息
    fut_code = 'IC.CFX'
    fut_name = '中证500'
    fut_multiplier = 200
    margin_rate = 0.12
    hedge_type_cnt = 4
    margin_redundancy = 0.8
    
    app = xw.App(visible = False, add_book = False)
    app.display_alerts = False
    app.screen_updating = False
    workbook = app.books.open('./bond-quantify/对冲情形.xlsx')
    worksheet = workbook.sheets.active
    rng = worksheet.range("A2").expand("table")
    nRows = rng.rows.count
    date_list = []
    hedge_dict = {}
    for i in range(1, hedge_type_cnt + 1):
        sub_hedge_dict = {}
        hedge_dict[i] = sub_hedge_dict
    for i in range(2, nRows + 2):
        codeAddr = "A" + str(i)
        dateStr = str(worksheet.range(codeAddr).value)[:8]
        date_list.append(dateStr)
        chara = 'A'
        for j in range(1, hedge_type_cnt + 1):
            chara = chr(ord(chara) + 1)
            codeAddr = chara + str(i)
            hedge_rate = worksheet.range(codeAddr).value
            hedge_dict[j][dateStr] = hedge_rate
    workbook.close()
    app.quit()
    
    start_date = date_list[0]
    end_date = date_list[nRows - 1]
    # 登录 Tushare 接口，获取交易日历
    pro = ts.pro_api(token)
    cal_date_df = pro.trade_cal(**{"start_date": start_date, "end_date": end_date, "is_open": "1"}, fields=["cal_date"])
    cal_date_list = sorted(cal_date_df["cal_date"].tolist())
    
    # 获取不同时段的转债列表
    code_dict = {}
    code_set = set()
    for i in range(0, len(date_list) - 1):
        date = date_list[i]
        app = xw.App(visible = False, add_book = False)
        app.display_alerts = False
        app.screen_updating = False
        workbook = app.books.open('./bond-quantify/bond-list/{}.xlsx'.format(date))
        worksheet = workbook.sheets.active
        rng = worksheet.range("B2").expand("table")
        nRows = rng.rows.count
        code_list = []
        yield_list = []
        for j in range(2, nRows + 2):
            codeAddr = "B" + str(j)
            code = str(worksheet.range(codeAddr).value)
            code_list.append(code)
            code_set.add(code)
            codeAddr = "R" + str(j)
            yield_list.append(str(worksheet.range(codeAddr).value))
        workbook.close()
        app.quit()
        code_dict[date] = code_list
        print(yield_list)
        exit(1)
    code_list = list(code_set)
    
    # 获取所有转债的收盘价信息
    close_dict = {}
    engine_ts = creat_engine_with_database('bond')
    for code in code_list:
        sql = "select trade_date, close from cb_daily where ts_code = '{}' and trade_date >= '{}' and trade_date <= '{}' order by trade_date".format(code, start_date, end_date)
        close_df = read_data(engine_ts, sql)
        trade_date_list = close_df['trade_date'].values.tolist()
        close_list = close_df['close'].values.tolist()
        close_dict[code] = dict(zip(trade_date_list, close_list))
    
    # 获取股指期货的收盘价信息
    engine_ts = creat_engine_with_database('futures')
    sql = "select trade_date, close from fut_daily where ts_code = '{}' and trade_date >= '{}' and trade_date <= '{}' order by trade_date".format(fut_code, start_date, end_date)
    fut_close_df = read_data(engine_ts, sql)
    trade_date_list = fut_close_df['trade_date'].values.tolist()
    close_list = fut_close_df['close'].values.tolist()
    fut_close_dict = dict(zip(trade_date_list, close_list))
    
    # 计算每个交易日的转债多头资金变化
    fund_list = [init_fund]
    sub_code_list = code_dict[cal_date_list[0]]
    per_fund = init_fund / len(sub_code_list)
    remain_fund = init_fund
    for i in range(1, len(cal_date_list)):
        num_list = []
        resub_code_list = []
        trade_date = cal_date_list[i]
        for j in range(0, len(sub_code_list)):
            code = sub_code_list[j]
            close = close_dict[code][cal_date_list[i - 1]]
            num = int(per_fund / close)
            num_list.append(num)
            remain_fund -= num * close
        for j in range(0, len(sub_code_list)):
            code = sub_code_list[j]
            if trade_date in close_dict[code].keys():
                close = close_dict[code][trade_date]
            else:
                close = close_dict[code][cal_date_list[i - 1]]
                resub_code_list.append(code)
            remain_fund += num_list[j] * close
        fund_list.append(remain_fund)
        
        if i == len(cal_date_list) - 1:
            break
        if trade_date in date_list:
            sub_code_list = code_dict[trade_date]
        elif len(resub_code_list):
            sub_code_list = list(set(sub_code_list) - set(resub_code_list))
    
    # 转债多头净值
    worth_list = [round(v / init_fund, 4) for v in fund_list]
    mini_worth = min(worth_list)
    worth_list.insert(0, '转债多头净值')
    
    # 结果分析
    result_list = [['对冲比例', '最大回撤', '最终收益', '风险收益比']]
    one_result_list = ['转债多头']
    max_drawdown = round(get_max_drawdown_sys(worth_list[1:]) * 100, 2)
    one_result_list.append(str(max_drawdown) + '%')
    profit = round((worth_list[len(worth_list) - 1] - 1) * 100, 2)
    one_result_list.append(str(profit) + '%')
    risk_rate = round(profit / max_drawdown, 1)
    if risk_rate > 0:
        one_result_list.append('1-' + str(risk_rate))
    else:
        one_result_list.append('/')
    result_list.append(one_result_list)
    
    # 计算每个交易日的股指空头资金变化
    fut_init_fund = fut_close_dict[start_date]
    fut_fund_list = [fut_init_fund]
    fut_remain_fund = fut_init_fund
    for i in range(1, len(cal_date_list)):
        trade_date = cal_date_list[i]
        fut_pre_close = fut_close_dict[cal_date_list[i - 1]]
        fut_close = fut_close_dict[trade_date]
        fut_remain_fund += (fut_pre_close - fut_close)
        fut_fund_list.append(fut_remain_fund)
    
    # 股指空头净值
    fut_worth_list = [round(v / fut_init_fund, 4) for v in fut_fund_list]
    mini_worth = min(mini_worth, min(fut_worth_list))
    fut_worth_list.insert(0, '{}空头净值'.format(fut_name))
    
    # 结果分析
    one_result_list = ['{}空头'.format(fut_name)]
    max_drawdown = round(get_max_drawdown_sys(fut_worth_list[1:]) * 100, 2)
    one_result_list.append(str(max_drawdown) + '%')
    profit = round((fut_worth_list[len(fut_worth_list) - 1] - 1) * 100, 2)
    one_result_list.append(str(profit) + '%')
    risk_rate = round(profit / max_drawdown, 1)
    if risk_rate > 0:
        one_result_list.append('1-' + str(risk_rate))
    else:
        one_result_list.append('/')
    result_list.append(one_result_list)
    
    # 不同对冲风格的资金变化
    hedge_fund_list = [[init_fund] * hedge_type_cnt]
    hedge_remain_fund_list = [init_fund] * hedge_type_cnt
    sub_code_list = code_dict[cal_date_list[0]]
    per_fund_list = []
    fut_vol_list = []
    for i in range(1, hedge_type_cnt + 1):
        hedge_rate = hedge_dict[i][start_date]
        bond_fund = init_fund / (1 + margin_rate * hedge_rate)
        per_fund = bond_fund / len(sub_code_list)
        per_fund_list.append(per_fund)
        fut_fund = init_fund - bond_fund
        fut_vol = fut_fund * margin_redundancy / margin_rate / fut_multiplier / fut_close_dict[start_date]
        fut_vol_list.append(fut_vol)
    
    for i in range(1, len(cal_date_list)):
        num_list = []
        resub_code_list = []
        trade_date = cal_date_list[i]
        # 买入
        for j in range(0, len(sub_code_list)):
            code = sub_code_list[j]
            close = close_dict[code][cal_date_list[i - 1]]
            sub_num_list = []
            for k in range(0, hedge_type_cnt):
                num = int(per_fund_list[k] / close)
                sub_num_list.append(num)
                hedge_remain_fund_list[k] -= num * close
            num_list.append(sub_num_list)
        for k in range(0, hedge_type_cnt):
            close = fut_close_dict[cal_date_list[i - 1]]
            hedge_remain_fund_list[k] += close * fut_multiplier * fut_vol_list[k]
        # 卖出
        for j in range(0, len(sub_code_list)):
            code = sub_code_list[j]
            if trade_date in close_dict[code].keys():
                close = close_dict[code][trade_date]
            else:
                close = close_dict[code][cal_date_list[i - 1]]
                resub_code_list.append(code)
            for k in range(0, hedge_type_cnt):
                hedge_remain_fund_list[k] += num_list[j][k] * close
        for k in range(0, hedge_type_cnt):
            close = fut_close_dict[trade_date]
            hedge_remain_fund_list[k] -= close * fut_multiplier * fut_vol_list[k]
        hedge_fund_list.append(deepcopy(hedge_remain_fund_list))
        
        if i == len(cal_date_list) - 1:
            break
        if trade_date in date_list:
            sub_code_list = code_dict[trade_date]
            for j in range(1, hedge_type_cnt + 1):
                hedge_rate = hedge_dict[j][trade_date]
                bond_fund = hedge_remain_fund_list[j - 1] / (1 + margin_rate * hedge_rate)
                per_fund = bond_fund / len(sub_code_list)
                per_fund_list[j - 1] = per_fund
                fut_fund = hedge_remain_fund_list[j - 1] - bond_fund
                fut_vol = fut_fund * margin_redundancy / margin_rate / fut_multiplier / fut_close_dict[trade_date]
                fut_vol_list[j - 1] = fut_vol
        elif len(resub_code_list):
            sub_code_list = list(set(sub_code_list) - set(resub_code_list))

    # 对冲净值
    hedge_worth_list = []
    for i in range(0, len(cal_date_list)):
        sub_hedge_worth_list = [round(v / init_fund, 4) for v in hedge_fund_list[i]]
        hedge_worth_list.append(sub_hedge_worth_list)
    hedge_worth_list = list(map(list, zip(*hedge_worth_list)))
    
    hedge_mini_worth = 1
    for i in range(1, hedge_type_cnt + 1):
        one_result_list = ['对冲策略{}'.format(i)]
        one_hedge_worth_list = hedge_worth_list[i - 1]
        hedge_mini_worth = min(hedge_mini_worth, min(one_hedge_worth_list))
        max_drawdown = round(get_max_drawdown_sys(one_hedge_worth_list) * 100, 2)
        one_result_list.append(str(max_drawdown) + '%')
        profit = round((one_hedge_worth_list[len(one_hedge_worth_list) - 1] - 1) * 100, 2)
        one_result_list.append(str(profit) + '%')
        risk_rate = round(profit / max_drawdown, 1)
        if risk_rate > 0:
            one_result_list.append('1-' + str(risk_rate))
        else:
            one_result_list.append('/')
        result_list.append(one_result_list)
        hedge_worth_list[i - 1].insert(0, "对冲策略{}".format(i))
    
    # 日期格式规范化
    for i in range(0, len(cal_date_list)):
        trade_date = cal_date_list[i]
        cal_date_list[i] = trade_date[:4] + '/' + trade_date[4:6] + '/' + trade_date[6:]
    cal_date_list.insert(0, '日期')
    
    # 写入内容
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    ws = wb.sheets.add()
    ws.range('A1').options(transpose=True).value = cal_date_list
    ws.range('B1').options(transpose=True).value = worth_list
    ws.range('C1').options(transpose=True).value = fut_worth_list
    ws.range('D1').options(transpose=True).value = cal_date_list
    ws.range('E1').options(transpose=True).value = hedge_worth_list
    chara = chr(ord('E') + hedge_type_cnt + 1)
    ws.range('{}1'.format(chara)).value = result_list
    rng = ws.range('A1').expand()
    for i in range(0, 4 + hedge_type_cnt):
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
    
    # 对冲净值曲线
    cnt_of_date = len(hedge_worth_list[0])
    chart = ws.charts.add(700, 200, 650, 400)
    chart.set_source_data(ws.range((1,4),(cnt_of_date,4 + hedge_type_cnt)))
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
    chart.api[1].ChartTitle.Text = "不同对冲风格净值曲线"     #改变标题文本
    # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
    chart.api[1].Axes(2).TickLabels.NumberFormatLocal = "#,##0.00_);[红色](#,##0.00)"      # 纵坐标格式
    chart.api[1].Axes(2).MajorUnit = 0.02      # 纵坐标单位值
    chart.api[1].Axes(1).MajorUnit = 60      # 横坐标单位值
    chart.api[1].Legend.Position = -4107    # 图例显示在下方
    # chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
    chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "yy/mm/dd"      # 格式化横坐标显示
    chart.api[1].Axes(2).CrossesAt = hedge_mini_worth - 0.02
    chart.api[1].Axes(2).MinimumScale = hedge_mini_worth - 0.02
    chart.api[1].ChartStyle = 245       # 图表格式
    
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    if not os.path.exists('output/{}/'.format(todayStr)):
        os.makedirs('output/{}/'.format(todayStr))
    wb.save('./output/{}/{}-{}-{}-{}股指期货对冲净值回测.xlsx'.format(todayStr, start_date, end_date, file_name, fut_name))
    wb.close()
    app.quit()
    print('转债-股指期货对冲净值回测 Excel 导出完毕！')
    

def main():
    # write_bond_fut_data_to_xlsx()
    # write_wugu_bond_fut_data_to_xlsx()
    
    write_turned_bond_fut_data_to_xlsx()


if __name__ == "__main__":
    main()
