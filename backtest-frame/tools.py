# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-02-26
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-28

import pandas as pd
import xlwings as xw
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import datetime
import os
import sys
sys.path.append('./backtest-frame/api/')
from api.BackTestApi import *

# 导出季连股指期货年化升贴水率走势
def write_fut_diff_to_xlsx(start_date, end_date, fut_code):
    cal_date_list = get_cal_date_list(start_date, end_date)
    sql = "select ts_code, trade_date, close, vol, oi from future.fut_daily where trade_date >= '{}' and trade_date <= '{}'".format(start_date, end_date)
    fut_daily_md_df = read_postgre_data(sql)
    fut_daily_md_df = fut_daily_md_df[((fut_daily_md_df.ts_code.str.startswith(fut_code)) & (fut_daily_md_df.ts_code.str.len() > 9))]
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
        
        fut_md_df = fut_daily_md_df[(fut_daily_md_df.trade_date == last_trade_date)].copy()
        fut_md_df.sort_values(by='ts_code', ascending=True, inplace=True)
        fut_md_df.reset_index(drop=True, inplace=True)
        fut_ts_code = fut_md_df.loc[2]['ts_code']
        fut_md_df = fut_daily_md_df[((fut_daily_md_df.trade_date == trade_date) & (fut_daily_md_df.ts_code == fut_ts_code))].copy()
        fut_md_df.reset_index(drop=True, inplace=True)
        fut_clsoe = fut_md_df.loc[0]['close']
            
        fut_ts_code_list.append(fut_ts_code)
        fut_close_list.append(fut_clsoe)
            
        days = calculate_remain_days(fut_ts_code, trade_date)
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
    fut_diff_rate_dict = dict(zip(date_list, value_list))
    
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
    
    wb.save('IC季连年化升贴水走势.xlsx')
    wb.close()
    app.quit()
    
    return fut_diff_rate_dict

# 导出指数走势
def write_index_to_xlsx(start_date, end_date, index_name):
    cal_date_list = get_cal_date_list(start_date, end_date)
    sql = "select update_date, value from future.fut_funds where index_name = '{}' and update_date >= '{}' and update_date <= '{}'".format(index_name, start_date, end_date)
    index_daily_md_df = read_postgre_data(sql)
    
    date_list = ['日期']
    index_close_list = [index_name]
    for i in range(0, len(cal_date_list)):
        trade_date = cal_date_list[i]
        index_md_df = index_daily_md_df[index_daily_md_df.update_date == trade_date].copy()
        index_md_df.reset_index(drop=True, inplace=True)
        index_close = index_md_df.loc[0]['value']
        index_close_list.append(index_close)
        
        date = trade_date[:4] + '/' + trade_date[4:6] + '/' + trade_date[6:8]
        date_list.append(date)
    
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    ws = wb.sheets.add()
    
    ws.range('A1').options(transpose=True).value = date_list
    ws.range('B1').options(transpose=True).value = index_close_list
    ws.autofit()
    wb.save('{}指数走势.xlsx'.format(index_name))
    wb.close()
    app.quit()

# 导出可转债全市场平均收益率走势
def write_mean_yield_to_maturity_to_xlsx(start_date, end_date):
    cal_date_list = get_cal_date_list(start_date, end_date)
    bond_daily_md_df = get_daily_md_data('bond', 'cb_daily_test', 'trade_date, yield_to_maturity', start_date, end_date)
    
    date_list = ['日期']
    mean_yield_to_maturity_list = ['平均收益率']
    for i in range(0, len(cal_date_list)):
        trade_date = cal_date_list[i]
        bond_md_df = bond_daily_md_df[bond_daily_md_df.trade_date == trade_date].copy()
        yield_to_maturity_list = bond_md_df['yield_to_maturity'].tolist()
        mean_yield_to_maturity = sum(yield_to_maturity_list) / len(yield_to_maturity_list)
        mean_yield_to_maturity_list.append(mean_yield_to_maturity)
        
        date = trade_date[:4] + '/' + trade_date[4:6] + '/' + trade_date[6:8]
        date_list.append(date)
        
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    ws = wb.sheets.add()
    
    ws.range('A1').options(transpose=True).value = date_list
    ws.range('B1').options(transpose=True).value = mean_yield_to_maturity_list
    ws.autofit()
    wb.save('可转债市场平均收益率走势.xlsx')
    wb.close()
    app.quit()

# 导出可转债全市场平均溢价率走势
def write_mean_cb_over_rate_to_xlsx(start_date, end_date):
    cal_date_list = get_cal_date_list(start_date, end_date)
    bond_daily_md_df = get_daily_md_data('bond', 'cb_daily_test', 'trade_date, cb_over_rate', start_date, end_date)
    fut_diff_rate_dict = write_fut_diff_to_xlsx('20190101', '20240229', 'IC')
    
    date_list = ['日期']
    mean_cb_over_rate_list = ['平均溢价率']
    hedge_rate_list = ['默认对冲比例']
    fix_hedge_rate_list = ['修正对冲比例']
    for i in range(0, len(cal_date_list)):
        trade_date = cal_date_list[i]
        bond_md_df = bond_daily_md_df[bond_daily_md_df.trade_date == trade_date].copy()
        cb_over_rate_list = bond_md_df['cb_over_rate'].tolist()
        mean_cb_over_rate = sum(cb_over_rate_list) / len(cb_over_rate_list)
        mean_cb_over_rate_list.append(mean_cb_over_rate)
        
        if mean_cb_over_rate <= 20:
            hedge_rate = 0.3
        elif mean_cb_over_rate >= 60:
            hedge_rate = 0.1
        else:
            hedge_rate = 0.3 + (0.1 - 0.3) * (mean_cb_over_rate - 20) / (60 - 20)
        
        hedge_rate_list.append(hedge_rate)
        
        date = trade_date[:4] + '/' + trade_date[4:6] + '/' + trade_date[6:8]
        date_list.append(date)
        
        if date in fut_diff_rate_dict.keys():
            fut_diff_rate = fut_diff_rate_dict[date]
            if fut_diff_rate >= 10 and fut_diff_rate <= 20:
                hedge_rate -= 0.1
        fix_hedge_rate_list.append(hedge_rate)
        
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    ws = wb.sheets.add()
    
    ws.range('A1').options(transpose=True).value = date_list
    ws.range('B1').options(transpose=True).value = mean_cb_over_rate_list
    ws.range('C1').options(transpose=True).value = hedge_rate_list
    ws.range('D1').options(transpose=True).value = fix_hedge_rate_list
    ws.autofit()
    wb.save('可转债市场平均溢价率走势.xlsx')
    wb.close()
    app.quit()

# 导出可转债全市场平均收盘价走势
def write_mean_close_to_xlsx(start_date, end_date):
    cal_date_list = get_cal_date_list(start_date, end_date)
    bond_daily_md_df = get_daily_md_data('bond', 'cb_daily_test', 'trade_date, close', start_date, end_date)
    
    date_list = ['日期']
    mean_close_list = ['平均收盘价']
    for i in range(0, len(cal_date_list)):
        trade_date = cal_date_list[i]
        bond_md_df = bond_daily_md_df[bond_daily_md_df.trade_date == trade_date].copy()
        close_list = bond_md_df['close'].tolist()
        mean_close = sum(close_list) / len(close_list)
        mean_close_list.append(mean_close)
        
        date = trade_date[:4] + '/' + trade_date[4:6] + '/' + trade_date[6:8]
        date_list.append(date)
        
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    ws = wb.sheets.add()
    
    ws.range('A1').options(transpose=True).value = date_list
    ws.range('B1').options(transpose=True).value = mean_close_list
    ws.autofit()
    wb.save('可转债市场平均收盘价走势.xlsx')
    wb.close()
    app.quit()

# 相关性分析
def correlation_analysis():
    df = pd.read_excel('C:/Users/yanse/Desktop/相关性分析.xlsx', names=['worth', 'fut_diff', 'IC', 'mean_yield', 'mean_close', 'mean_cb_over_rate', 'hedge_rate', 'fix_hedge_rate'])
    sns.pairplot(data=df)
    plt.show()
    
# 净值分析
def analyze_worth_result(book_name, column):
    app = xw.App(visible=True,add_book=False)
    wb = app.books.open(book_name)
    ws = wb.sheets['综合']
    rng = ws.range('B1').expand()
    nrows = rng.rows.count
    strloc = column + '2:' + column + str(nrows)
    date_list = ws.range(strloc).value
    column = chr(ord(column) + 1)
    strloc = column + '2:' + column + str(nrows)
    asset_list = ws.range(strloc).value
    new_date_list = ['日期']
    worth_list = ['净值']
    mini_worth = 1
    worth_dict = {}
    year = ''
    init_fund = asset_list[0]
    for i in range(0, len(date_list)):
        asset = asset_list[i]
        worth = round(asset / init_fund, 4)
        worth_list.append(worth)
        mini_worth = min(mini_worth, worth)
        
        date = date_list[i]
        date = date[:4] + '/' + date[4:6] + '/' + date[6:8]
        new_date_list.append(date)
        
        if year != date[:4]:
            year = date[:4]
            worth_dict[year] = [1]
            tmp_init_fund = asset
        else:
            tmp_worth = round(asset / tmp_init_fund, 4)
            worth_dict[year].append(tmp_worth)
    
    result_list = [['组别', '最大回撤', '最终收益', '风险收益比', '年化收益']]
    one_result_list = get_result_list(worth_list[1:], '总体')
    result_list.append(one_result_list)
    for year, year_worth_list in worth_dict.items():
        one_result_list = get_result_list(year_worth_list, year)
        result_list.append(one_result_list)
    
    column = chr(ord(column) + 5)
    ws.range('{}1'.format(column)).value = result_list
    rng = ws.range('{}1'.format(column)).expand()
    for i in range(0, 5):
        rng.columns[i][0].color = (200, 255, 200)
    
    ws.autofit()
    wb.save()
    wb.close()
    app.quit()

# 期货库存与价差散点图绘制
def export_fut_inventory_spread_to_png(fut_code, spread_type, index_name, rec_spread, start_date, end_date, start_month, end_month):
    sql = "select update_date, value from future.fut_funds where index_name = '{}' and update_date >= '{}' and update_date <= '{}' order by update_date".format(index_name, start_date, end_date)
    inventory_df = read_postgre_data(sql)
    start_date = inventory_df.loc[0]['update_date']
    
    sql = "select trade_date, close from future.fut_spread_daily where fut_code = '{}' and spread_type = '{}' and trade_date >= '{}' and trade_date <= '{}' order by trade_date".format(fut_code, spread_type, start_date, end_date)
    spread_df = read_postgre_data(sql)
    
    sql = "select avg_value from future.futures_backtesting.fn_inventory_avg_line(5) where fut_code = '{}' and md <= '{}' order by md desc limit 1".format(fut_code, end_date[4:])
    avg_inventory_df  =read_postgre_data(sql)
    avg_inventory = avg_inventory_df.loc[0]['avg_value']
    
    last_trade_date = datetime.datetime.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')
    sql = "SELECT ticker_n, ticker_f, product, safe_spread from future.safe_spread('{}', '{}') where product = '{}'".format(last_trade_date, last_trade_date, fut_code)
    safe_spread_df = read_postgre_data(sql)
    for i in range(0, len(safe_spread_df)):
        spread_type_new = safe_spread_df.loc[i]['ticker_n'][-2:] + '-' + safe_spread_df.loc[i]['ticker_f'][-2:]
        if spread_type_new == spread_type:
            safe_spread = safe_spread_df.loc[i]['safe_spread']
    
    inventory_list = []
    spread_list = []
    index = 0
    check_index = True
    close_max = -9999
    close_min = 9999
    inventory_max = 0
    inventory_min = 9999999
    for i in range(0, len(spread_df)):
        trade_date = spread_df.loc[i]['trade_date']
        if (spread_type[:2] > spread_type[-2:] and trade_date[4:6] < start_month or trade_date[4:6] > end_month) or\
           (spread_type[:2] < spread_type[-2:] and trade_date[4:6] < start_month and trade_date[4:6] > end_month):
            continue
        if trade_date[:4] == end_date[:4] and check_index:
            index = len(inventory_list)
            check_index = False
        close = spread_df.loc[i]['close']
        close_max = max(close_max, close)
        close_min = min(close_min, close)
        inventory_copy_df = inventory_df[inventory_df.update_date <= trade_date].copy()
        inventory_copy_df.sort_values(by='update_date', ascending=False, inplace=True)
        inventory_copy_df.reset_index(drop=True, inplace=True)
        inventory = inventory_copy_df.loc[0]['value']
        inventory_max = max(inventory_max, inventory)
        inventory_min = min(inventory_min, inventory)
        inventory_list.append(inventory)
        spread_list.append(close)
        
    x = np.array(inventory_list)
    y = np.array(spread_list)
    
    slope, intercept = np.polyfit(x, y, 1)
    
    y_up = np.percentile(y, 75)
    y_down = np.percentile(y, 25)
    mean_x = x.mean()
    intercept_up = y_up - mean_x * slope
    intercept_down = y_down - mean_x * slope
    
    plt.figure(figsize=(10, 8))
    plt.scatter(x[:index], y[:index], color='lightblue', label='往年数据点')
    plt.scatter(x[index:], y[index:], color='lightcoral', label='今年数据点')
    plt.plot(x, slope * x + intercept, color='red', label='库存-价差拟合线')
    plt.plot(x, slope * x + intercept_up, color='blue', label='区域浮动边界')
    plt.plot(x, slope * x + intercept_down, color='blue')
    plt.axhline(y=rec_spread, label='安全价差', color='orange')
    plt.axhline(y = -safe_spread, label='无风险价差', color='fuchsia')
    plt.axvline(x = avg_inventory, label='历史库存均线', color='green')
    plt.plot(inventory_list[len(inventory_list) - 2], spread_list[len(spread_list) - 2], color='orange', marker='*', markersize='20', label='上一交易日')
    plt.plot(inventory_list[len(inventory_list) - 1], spread_list[len(spread_list) - 1], color='red', marker='*', markersize='20', label='最新交易日')
    plt.rcParams['font.sans-serif']=['SimHei']
    plt.rcParams['axes.unicode_minus']=False
    plt.xlabel(index_name)
    plt.ylabel('价差')
    plt.title("【{}-{}】【{}{}（{}月至{}月）】库存-价差散点分布及拟合图".format(start_date, end_date, fut_code, spread_type, int(start_month), int(end_month)))
    
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    if not os.path.exists('output/{}/库存-价差散点图/'.format(todayStr)):
        os.makedirs('output/{}/库存-价差散点图/'.format(todayStr))
    plt.legend()
    plt.savefig('./output/{}/库存-价差散点图/【{}-{}】【{}{}（{}月至{}月）】库存-价差散点分布及拟合图.png'.format(todayStr, start_date, end_date, fut_code, spread_type, int(start_month), int(end_month)), dpi=300)
    print("【{}-{}】【{}{}（{}月至{}月）】库存-价差散点分布及拟合图输出完毕！".format(start_date, end_date, fut_code, spread_type, int(start_month), int(end_month)))
    
    # plt.show()

def update_fut_inventory_spread_png():
    start_date = '20200101'
    option_list = [['HC', '10-01', '库存:热卷(板)', 40], ['V', '05-09', '社会库存合计', -100], ['FG', '09-01', '浮法玻璃生产线库存（万吨）', 30], ['NI', '05-06', '电解镍国内社会库存（吨）', -300],
                   ['M', '05-07', '豆粕库存_中国', 0], ['RM', '05-07', '菜粕库存_中国', 0], ['J', '05-09', '焦炭：库存合计：全样本（周）', 0], ['J', '09-01', '焦炭：库存合计：全样本（周）', 0]]
    option_list = [['SN', '05-06', '中国分地区锡锭社会库存-总库存', -500]]
    option_list = [['SP', '09-01', '港口纸浆总库存', -110]]
    option_list = [['SP', '05-09', '港口纸浆总库存', -110]]
    option_list = [['SP', '07-09', '港口纸浆总库存', -50]]
    option_list = [['RU', '05-09', '港口纸浆总库存', -50]]
    option_list = [['BU', '06-09', '沥青-华东炼厂库存量（万吨）', -50]]
    today = datetime.date.today()
    oneday = datetime.timedelta(days=1)
    todayStr = (today - oneday).strftime('%Y%m%d')
    date_list = get_cal_date_list(start_date, todayStr)
    end_date = date_list[len(date_list) - 1]
    for opt in option_list:
        start_month = str(int(opt[1][-2:]) + 2)
        if len(start_month) == 1:
            start_month = '0' + start_month
        end_month = str(int(opt[1][:2]) - 1)
        if len(end_month) == 1:
            end_month = '0' + end_month
        export_fut_inventory_spread_to_png(opt[0], opt[1], opt[2], opt[3], start_date, end_date, start_month, end_month)

def main():
    # write_fut_diff_to_xlsx('20190101', '20240229', 'IC')
    # write_index_to_xlsx('20190101', '20240229', '中证500')
    # write_mean_yield_to_maturity_to_xlsx('20190101', '20240228')
    # write_mean_cb_over_rate_to_xlsx('20190101', '20240228')
    # write_mean_close_to_xlsx('20190101', '20240228')
    # correlation_analysis()
    # analyze_worth_result('C:/Users/yanse/Desktop/综合.xlsx', 'C')

    # export_fut_inventory_spread_to_png('HC', '10-01', '库存:热卷(板)', 40, '20200101', '20240326', '03', '09')
    update_fut_inventory_spread_png()
        
if __name__ == "__main__":
    main()
