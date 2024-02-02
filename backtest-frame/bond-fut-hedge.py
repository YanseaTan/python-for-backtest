# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-02-01
# @Last Modified by:   Yansea
# @Last Modified time: 2024-02-02

import pandas as pd
import xlwings as xw
import datetime
import time
import os
from copy import deepcopy
from sqlalchemy import create_engine

DEFAULT_VALUE = 9999999

setting_data = pd.DataFrame()

init_fund = 0
start_date = '20190101'
end_date = '20240101'
alter_period = 0

fut_name = ''
fut_code = ''
fut_multiplier = 0
margin_rate = 0
margin_redundancy = 0

yield_low = -DEFAULT_VALUE
yield_high = DEFAULT_VALUE
close_low = -DEFAULT_VALUE
close_high = DEFAULT_VALUE
vol_low = -DEFAULT_VALUE
vol_high = DEFAULT_VALUE

hedge_yield_1 = DEFAULT_VALUE
hedge_yield_2 = DEFAULT_VALUE
hedge_rate_1 = DEFAULT_VALUE
hedge_rate_2 = DEFAULT_VALUE
cnt_of_level = 0
each_level = 0

# 服务器 postgre 数据库用户配置
postgre_user = 'postgres'
postgre_password = 'shan3353'
postgre_addr = '10.10.20.189:5432'
postgre_database = 'future'

# 创建 postgre 数据库操作引擎
postgre_engine_ts = create_engine('postgresql://{}:{}@{}/{}?sslmode=disable'.format(postgre_user, postgre_password, postgre_addr, postgre_database))

def read_postgre_data(sql):
    df = pd.read_sql_query(sql, postgre_engine_ts)
    return df

def read_config(file_path):
    print("读取设置文件...")
    global init_fund
    global start_date
    global end_date
    global alter_period
    global fut_name
    global fut_code
    global fut_multiplier
    global margin_rate
    global margin_redundancy
    global yield_low
    global yield_high
    global close_low
    global close_high
    global vol_low
    global vol_high
    global hedge_yield_1
    global hedge_yield_2
    global hedge_rate_1
    global hedge_rate_2
    global cnt_of_level
    global each_level
    
    global setting_data
    setting_data = pd.read_excel(file_path)
    setting_data = pd.DataFrame(setting_data)
    
    app = xw.App(visible = False, add_book = False)
    app.display_alerts = False
    app.screen_updating = False
    workbook = app.books.open(file_path)
    ws = workbook.sheets.active

    init_fund = ws.range('A3').value * 10000
    start_date = str(ws.range('B3').value)[:8]
    end_date = str(ws.range('C3').value)[:8]
    if len(start_date) < 8 or len(end_date) < 8 or start_date >= end_date:
        return -1
    alter_period = int(ws.range('D3').value)
    if init_fund <= 0 or alter_period <= 0:
        return -1
    
    fut_name = str(ws.range('A7').value)
    fut_code = str(ws.range('B7').value)
    if fut_name == '' or fut_code == '':
        return -1
    fut_multiplier = ws.range('C7').value
    margin_rate = ws.range('D7').value
    margin_redundancy = 1 - ws.range('E7').value
    if fut_multiplier == 0 or margin_rate == 0 or margin_redundancy == 0:
        return -1

    if ws.range('B11').value != None:
        yield_low = max(ws.range('B11').value, yield_low)
    if ws.range('B12').value != None:
        yield_high = min(ws.range('B12').value, yield_high)
    if ws.range('C11').value != None:
        close_low = max(ws.range('C11').value, close_low)
    if ws.range('C12').value != None:
        close_high = min(ws.range('C12').value, close_high)
    if ws.range('D11').value != None:
        vol_low = max(ws.range('D11').value, vol_low)
    if ws.range('D12').value != None:
        vol_high = min(ws.range('D12').value, vol_high)
    if yield_low >= yield_high or close_low >= close_high or vol_low >= vol_high:
        return -1

    hedge_yield_1 = ws.range('A16').value
    hedge_yield_2 = ws.range('A17').value
    hedge_rate_1 = ws.range('B16').value
    hedge_rate_2 = ws.range('B17').value
    if hedge_yield_1 == DEFAULT_VALUE or hedge_yield_2 == DEFAULT_VALUE or hedge_rate_1 == DEFAULT_VALUE or hedge_rate_2 == DEFAULT_VALUE:
        return -1
    cnt_of_level = int(ws.range('C16').value)
    each_level = ws.range('D16').value
    if cnt_of_level == 0 or each_level == 0:
        return -1
    
    workbook.close()
    app.quit()
    return 0

def get_max_drawdown_sys(array):
    array = pd.Series(array)
    cumsum = array.cummax()
    return max(cumsum-array)

def get_result_list(worth_list, result_name):
    print("分析{}净值结果...".format(result_name))
    result_list = [result_name]
    max_drawdown = round(get_max_drawdown_sys(worth_list[1:]) * 100, 2)
    result_list.append(str(max_drawdown) + '%')
    profit = round((worth_list[len(worth_list) - 1] - 1) * 100, 2)
    result_list.append(str(profit) + '%')
    risk_rate = round(profit / max_drawdown, 1)
    if risk_rate > 0:
        result_list.append('1-' + str(risk_rate))
    else:
        result_list.append('/')
    print("分析{}净值结果完毕！".format(result_name))
    return result_list

def write_bond_fut_data_to_xlsx():
    ret = read_config('./可转债-股指期货对冲回测框架设置.xlsx')
    if ret != 0:
        print("设置读取错误，请检查设置文件！")
        exit(1)
    
    # 获取时间节点
    print("获取交易日历...")
    sql = "select distinct trade_date from bond.cb_daily_test where trade_date >= '{}' and trade_date <= '{}' order by trade_date".format(start_date, end_date)
    date_df = read_postgre_data(sql)
    cal_date_list = date_df['trade_date'].tolist()
    date_unit = int(len(cal_date_list) / 8)
    date_list = []
    for i in range(0, len(date_df), alter_period):
        date = date_df.loc[i]['trade_date']
        date_list.append(date)
    
    # 获取不同时间节点的对冲比例以及代码列表
    print("获取不同时间节点的对冲比例以及代码列表...")
    hedge_dict = {}
    for i in range(0, cnt_of_level):
        sub_hedge_dict = {}
        hedge_dict[i] = sub_hedge_dict

    code_dict = {}
    code_set = set()
    for i in range(0, len(date_list)):
        date = date_list[i]
        sql = "select ts_code, yield_to_maturity from bond.cb_daily_test where trade_date = '{}' and yield_to_maturity >= {} and yield_to_maturity <= {}\
            and close >= {} and close <= {} and vol >= {} and vol <= {}".format(date, yield_low, yield_high, close_low, close_high, vol_low, vol_high) 
        code_df = read_postgre_data(sql)
        code_list = code_df['ts_code'].tolist()
        code_dict[date] = code_list
        code_set = set(code_set | set(code_list))
        
        yield_list = code_df['yield_to_maturity'].tolist()
        yield_mean = sum(yield_list) / len(yield_list)
        if yield_mean <= hedge_yield_1:
            hedge_rate = hedge_rate_1
        elif yield_mean >= hedge_yield_2:
            hedge_rate = hedge_rate_2
        else:
            hedge_rate = hedge_rate_1 + (hedge_rate_2 - hedge_rate_1) / (hedge_yield_2 - hedge_yield_1) * yield_mean
        for j in range(0, cnt_of_level):
            hedge_dict[j][date] = round(hedge_rate + j * each_level, 2)
    code_list = list(code_set)
    
    # 获取所有转债的收盘价信息
    print("获取所有转债的收盘价信息...")
    close_dict = {}
    sql = "select ts_code, trade_date, close from bond.cb_daily_test where trade_date >= '{}' and trade_date <= '{}' order by trade_date".format(start_date, end_date)
    close_df = read_postgre_data(sql)
    for code in code_list:
        df = close_df[close_df['ts_code'] == code]
        trade_date_list = df['trade_date'].values.tolist()
        close_list = df['close'].values.tolist()
        close_dict[code] = dict(zip(trade_date_list, close_list))
        
    # 获取股指期货的收盘价信息
    print("获取股指期货的收盘价信息...")
    sql = "select trade_date, close from future.fut_daily where ts_code = '{}' and trade_date >= '{}' and trade_date <= '{}' order by trade_date".format(fut_code, start_date, end_date)
    fut_close_df = read_postgre_data(sql)
    trade_date_list = fut_close_df['trade_date'].values.tolist()
    close_list = fut_close_df['close'].values.tolist()
    fut_close_dict = dict(zip(trade_date_list, close_list))
    
    # 计算每个交易日的转债多头资金变化
    print("计算每个交易日的转债多头资金变化...")
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
    print("计算转债多头净值...")
    worth_list = [round(v / init_fund, 4) for v in fund_list]
    mini_worth = min(worth_list)
    worth_list.insert(0, '转债多头净值')

    # 结果分析
    result_list = [['对冲比例', '最大回撤', '最终收益', '风险收益比']]
    one_result_list = get_result_list(worth_list, '转债多头')
    result_list.append(one_result_list)
    
    # 计算每个交易日的股指空头资金变化
    print("计算每个交易日的股指空头资金变化...")
    fut_init_fund = fut_close_dict[cal_date_list[0]]
    fut_fund_list = [fut_init_fund]
    fut_remain_fund = fut_init_fund
    for i in range(1, len(cal_date_list)):
        trade_date = cal_date_list[i]
        fut_pre_close = fut_close_dict[cal_date_list[i - 1]]
        fut_close = fut_close_dict[trade_date]
        fut_remain_fund += (fut_pre_close - fut_close)
        fut_fund_list.append(fut_remain_fund)
        
    # 股指空头净值
    print("计算股指空头净值...")
    fut_worth_list = [round(v / fut_init_fund, 4) for v in fut_fund_list]
    mini_worth = min(mini_worth, min(fut_worth_list))
    fut_worth_list.insert(0, '{}空头净值'.format(fut_name))
    
    # 结果分析
    one_result_list = get_result_list(fut_worth_list, '{}空头'.format(fut_name))
    result_list.append(one_result_list)
    
    # 不同对冲风格的资金变化
    print("计算不同对冲风格下的资金变化...")
    hedge_fund_list = [[init_fund] * cnt_of_level]
    hedge_remain_fund_list = [init_fund] * cnt_of_level
    sub_code_list = code_dict[cal_date_list[0]]
    per_fund_list = []
    fut_vol_list = []
    for i in range(0, cnt_of_level):
        hedge_rate = hedge_dict[i][cal_date_list[0]]
        bond_fund = init_fund / (1 + margin_rate * hedge_rate)
        per_fund = bond_fund / len(sub_code_list)
        per_fund_list.append(per_fund)
        fut_fund = init_fund - bond_fund
        fut_vol = fut_fund * margin_redundancy / margin_rate / fut_multiplier / fut_close_dict[cal_date_list[0]]
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
            for k in range(0, cnt_of_level):
                num = int(per_fund_list[k] / close)
                sub_num_list.append(num)
                hedge_remain_fund_list[k] -= num * close
            num_list.append(sub_num_list)
        for k in range(0, cnt_of_level):
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
            for k in range(0, cnt_of_level):
                hedge_remain_fund_list[k] += num_list[j][k] * close
        for k in range(0, cnt_of_level):
            close = fut_close_dict[trade_date]
            hedge_remain_fund_list[k] -= close * fut_multiplier * fut_vol_list[k]
        hedge_fund_list.append(deepcopy(hedge_remain_fund_list))
        
        if i == len(cal_date_list) - 1:
            break
        # 重新进行资金分配
        if trade_date in date_list:
            sub_code_list = code_dict[trade_date]
            for j in range(0, cnt_of_level):
                hedge_rate = hedge_dict[j][trade_date]
                bond_fund = hedge_remain_fund_list[j] / (1 + margin_rate * hedge_rate)
                per_fund = bond_fund / len(sub_code_list)
                per_fund_list[j] = per_fund
                fut_fund = hedge_remain_fund_list[j] - bond_fund
                fut_vol = fut_fund * margin_redundancy / margin_rate / fut_multiplier / fut_close_dict[trade_date]
                fut_vol_list[j] = fut_vol
        elif len(resub_code_list):
            sub_code_list = list(set(sub_code_list) - set(resub_code_list))

    # 对冲净值
    print("计算不同对冲风格下的净值...")
    hedge_worth_list = []
    for i in range(0, len(cal_date_list)):
        sub_hedge_worth_list = [round(v / init_fund, 4) for v in hedge_fund_list[i]]
        hedge_worth_list.append(sub_hedge_worth_list)
    hedge_worth_list = list(map(list, zip(*hedge_worth_list)))
    
    hedge_mini_worth = 1
    for i in range(0, cnt_of_level):
        hedge_mini_worth = min(hedge_mini_worth, min(hedge_worth_list[i]))
        one_result_list = get_result_list(hedge_worth_list[i], '对冲策略{}'.format(i + 1))
        result_list.append(one_result_list)
        hedge_worth_list[i].insert(0, "对冲策略{}".format(i + 1))
    
    # 日期格式规范化
    for i in range(0, len(cal_date_list)):
        trade_date = cal_date_list[i]
        cal_date_list[i] = trade_date[:4] + '/' + trade_date[4:6] + '/' + trade_date[6:]
    cal_date_list.insert(0, '日期')
    
    # 写入内容
    print("向 Excel 写入内容...")
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    timeStr = time.strftime('%H-%M-%S')
    if not os.path.exists('output/{}/'.format(todayStr)):
        os.makedirs('output/{}/'.format(todayStr))
    book_name = './output/{}/{}-{}-转债轮换-{}股指期货对冲净值回测-{}.xlsx'.format(todayStr, start_date, end_date, fut_name, timeStr)
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    ws = wb.sheets.add('净值曲线')
    ws.range('A1').options(transpose=True).value = cal_date_list
    ws.range('B1').options(transpose=True).value = worth_list
    ws.range('C1').options(transpose=True).value = fut_worth_list
    ws.range('D1').options(transpose=True).value = cal_date_list
    ws.range('E1').options(transpose=True).value = hedge_worth_list
    chara = chr(ord('E') + cnt_of_level + 1)
    ws.range('{}1'.format(chara)).value = result_list
    rng = ws.range('A1').expand()
    for i in range(0, 4 + cnt_of_level):
        rng.columns[i][0].color = (211, 211, 211)
    rng = ws.range('{}1'.format(chara)).expand()
    for i in range(0, 4):
        rng.columns[i][0].color = (200, 255, 200)
    ws.autofit()
    wb.save(book_name)
    wb.close()
    app.quit()
    
    with pd.ExcelWriter(book_name, mode='a') as writer:
        setting_data.to_excel(writer, sheet_name='参数设置', index=False)
    app = xw.App(visible=True,add_book=False)
    wb = app.books.open(book_name)
    ws = wb.sheets['净值曲线']
    
    # 插入曲线
    # 转债多头-股指期货空头净值曲线
    print("向 Excel 插入曲线...")
    cnt_of_date = len(worth_list)
    chart = ws.charts.add(20, 120, 800, 400)
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
    chart.api[1].Axes(2).MajorUnit = 0.08      # 纵坐标单位值
    chart.api[1].Axes(1).MajorUnit = date_unit      # 横坐标单位值
    chart.api[1].Legend.Position = -4107    # 图例显示在下方
    # chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
    chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "yy/mm/dd"      # 格式化横坐标显示
    chart.api[1].Axes(2).CrossesAt = mini_worth - 0.02
    chart.api[1].Axes(2).MinimumScale = mini_worth - 0.02
    chart.api[1].ChartStyle = 245       # 图表格式
    
    # 对冲净值曲线
    cnt_of_date = len(hedge_worth_list[0])
    chart = ws.charts.add(20, 495, 800, 400)
    chart.set_source_data(ws.range((1,4),(cnt_of_date,4 + cnt_of_level)))
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
    chart.api[1].Axes(2).MajorUnit = 0.05      # 纵坐标单位值
    chart.api[1].Axes(1).MajorUnit = date_unit      # 横坐标单位值
    chart.api[1].Legend.Position = -4107    # 图例显示在下方
    # chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
    chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "yy/mm/dd"      # 格式化横坐标显示
    chart.api[1].Axes(2).CrossesAt = hedge_mini_worth - 0.02
    chart.api[1].Axes(2).MinimumScale = hedge_mini_worth - 0.02
    chart.api[1].ChartStyle = 245       # 图表格式
    
    if len(wb.sheets) > 1:
        wb.sheets['Sheet1'].delete()
    wb.save(book_name)
    wb.close()
    app.quit()
    print('转债-股指期货对冲净值回测 Excel 导出完毕！')
    

def main():
    write_bond_fut_data_to_xlsx()

if __name__ == "__main__":
    main()
