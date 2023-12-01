# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-10-18
# @Last Modified by:   Yansea
# @Last Modified time: 2023-12-01

from sqlalchemy import create_engine
import xlwings as xw
import datetime
import os
from DatabaseTools import *
import numpy as np

# 根据合约组合名称返回该组合的所有日行情信息
def get_spread_daily_by_ts_code(ts_code, index):
    engine_ts = creat_engine_with_database('futures')
    sql = "select * from fut_spread_daily where ts_code = '{}' order by {};".format(ts_code, index)
    df = read_data(engine_ts, sql)
    return df

# 将所有组合合约最低价差数据导出到 excel 中
def write_spread_low_to_xlsx():
    engine_ts = creat_engine_with_database('futures')
    sql = "select distinct ts_code from fut_spread_daily order by ts_code;"
    code_df = read_data(engine_ts, sql)
    sql = "select distinct fut_code from fut_spread_daily order by fut_code desc;"
    fut_df = read_data(engine_ts, sql)
    
    # 以品种名在表格中创建不同的 sheet
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    title = ['名称', '一腿交割月', '5%最低', '10%最低', '15%最低', '20%最低', '最低价差', '最高价差']
    spread_num_dict = {}
    for i in range(0, len(fut_df)):
        fut_code = fut_df.loc[i]["fut_code"]
        sql = "select distinct spread_type from fut_spread_daily where fut_code = '{}' order by spread_type;".format(fut_code)
        spread_type_df = read_data(engine_ts, sql)
        spread_num = len(spread_type_df)
        spread_num_dict[fut_code] = spread_num
        
        ws = wb.sheets.add(fut_code)
        ws.range('A1').value = title
        rng = ws.range('A1').expand('table')
        for j in range(0, 8):
            rng.columns[j][0].color = (211, 211, 211)
        
        # 写入各类跨月组合的汇总数据
        for j in range(0, spread_num):
            spread_type = spread_type_df.loc[j]['spread_type']
            sql = "select close from fut_spread_daily where fut_code = '{}' and spread_type = '{}' order by close;".format(fut_code, spread_type)
            df = read_data(engine_ts, sql)
            num = len(df)
            data_list = [spread_type.replace('-', '--') + '汇总', spread_type[:2], df.loc[max(round(num * 0.05), 1) - 1]['close'], df.loc[max(round(num * 0.1), 1) - 1]['close'],
                        df.loc[max(round(num * 0.15), 1) - 1]['close'], df.loc[max(round(num * 0.2), 1) - 1]['close'], df.loc[0]['close'], df.loc[num - 1]['close']]
            ws.range('A' + str(j + 2)).value = data_list
            ws.range('C' + str(j + 2)).color = (100, 100, 255)
            ws.range('D' + str(j + 2)).color = (130, 130, 255)
            ws.range('E' + str(j + 2)).color = (160, 160, 255)
            ws.range('F' + str(j + 2)).color = (190, 190, 255)
            ws.range('G' + str(j + 2)).color = (200, 255, 200)
            ws.range('H' + str(j + 2)).color = (255, 200, 200)
            
        # 写入总计数据
        sql = "select close from fut_spread_daily where fut_code = '{}' order by close;".format(fut_code)
        df = read_data(engine_ts, sql)
        num = len(df)
        data_list = ['总计', '/', df.loc[max(round(num * 0.05), 1) - 1]['close'], df.loc[max(round(num * 0.1), 1) - 1]['close'],
                     df.loc[max(round(num * 0.15), 1) - 1]['close'], df.loc[max(round(num * 0.2), 1) - 1]['close'], df.loc[0]['close'], df.loc[num - 1]['close']]
        ws.range('A' + str(spread_num + 2)).value = data_list
        ws.range('A' + str(spread_num + 3)).value = ' '
        ws.range('A' + str(spread_num + 4)).value = title
        rng = ws.range('A' + str(spread_num + 4)).expand('table')
        for j in range(0, 8):
            rng.columns[j][0].color = (211, 211, 211)
    
        # 插入汇总散点图
        chart = ws.charts.add(500, 10, 500, 300)
        chart.set_source_data(ws.range((1,2),(spread_num + 1,6)))
        # Excel VBA 指令
        chart.chart_type = 'xy_scatter_lines_no_markers'
        chart.api[1].SetElement(2)          #显示标题
        chart.api[1].SetElement(101)        #显示图例
        chart.api[1].SetElement(301)        #x轴标题
        # chart.api[1].SetElement(311)      #y轴标题
        chart.api[1].SetElement(305)        #y轴的网格线
        # chart.api[1].SetElement(334)      #x轴的网格线
        chart.api[1].Axes(1).AxisTitle.Text = "一腿交割月"          #x轴标题的名字
        # chart.api[1].Axes(2).AxisTitle.Text = "价差"             #y轴标题的名字
        chart.api[1].ChartTitle.Text = fut_code + ' 最低价差随一腿交割月变动趋势（汇总）'     #改变标题文本
        chart.api[1].PlotBy = 1                 # 切换数据（为了正确显示）
        chart.api[1].PlotBy = 2
        chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 1      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        print('{} 品种汇总数据写入完成！进度：{}%'.format(fut_code, format((i + 1) / len(fut_df) * 100, '.2f')))
        
    # 写入所有合约组合的详细最低价差数据
    cnt = len(code_df)
    for i in range(0, cnt):
        ts_code = code_df.loc[i]["ts_code"]
        df = get_spread_daily_by_ts_code(ts_code, 'close')
        fut_code = df.loc[0]['fut_code']
        spread_type = df.loc[0]['spread_type']
        num = len(df)
        ws = wb.sheets[fut_code]
        nRows = ws.range('A1').expand('table').rows.count
        date = ts_code[:ts_code.index('-')][-4:]
        date = '20' + date[:2] + '年' + date[-2:] + '月'
        data_list = [ts_code, date, df.loc[max(round(num * 0.05), 1) - 1]['close'], df.loc[max(round(num * 0.1), 1) - 1]['close'],
                     df.loc[max(round(num * 0.15), 1) - 1]['close'], df.loc[max(round(num * 0.2), 1) - 1]['close'], df.loc[0]['close'], df.loc[num - 1]['close']]
        ws.range('A' + str(nRows + 1)).value = data_list
        ws.autofit()
        print('写入详细价差数据，进度：{}%'.format(format((i + 1) / cnt * 100, '.2f')))
    
    # 按品种插入所有合约组合详细数据的连续最低价差数据
    for i in range(0, len(fut_df)):
        fut_code = fut_df.loc[i]["fut_code"]
        spread_num = spread_num_dict[fut_code]
        ws = wb.sheets[fut_code]
        nRows = ws.range('A' + str(spread_num + 4)).expand('table').rows.count
        chart = ws.charts.add(480, 330, 800, 300)
        chart.set_source_data(ws.range((spread_num + 4,2),(spread_num + nRows + 3,6)))
        # Excel VBA 指令
        chart.chart_type = 'xy_scatter_lines_no_markers'
        chart.api[1].SetElement(2)      #显示标题
        chart.api[1].SetElement(101)    #显示图例
        chart.api[1].SetElement(301)    #x轴标题
        # chart.api[1].SetElement(311)    #y轴标题
        chart.api[1].SetElement(305)    #y轴的网格线
        # chart.api[1].SetElement(334)    #x轴的网格线
        chart.api[1].Axes(1).AxisTitle.Text = "一腿交割月"        #x轴标题的名字
        # chart.api[1].Axes(2).AxisTitle.Text = "价差"        #y轴标题的名字
        chart.api[1].ChartTitle.Text = fut_code + ' 最低价差随一腿交割月变动趋势（连续）'     #改变标题文本
        chart.api[1].PlotBy = 1     # 切换数据（为了正确显示）
        chart.api[1].PlotBy = 2
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "yy/m"      # 格式化横坐标显示
        chart.api[1].Axes(1).MajorUnit = 90     # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        print('{} 品种详细数据图表插入完成！进度：{}%'.format(fut_code, format((i + 1) / len(fut_df) * 100, '.2f')))
    
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    if len(wb.sheets) > 1:
        wb.sheets['Sheet1'].delete()
    wb.save('./output/{}-所有品种历史最低价差统计分析.xlsx'.format(todayStr))
    wb.close()
    app.quit()
    print('所有品种历史最低价差统计分析 Excel 数据导出完毕！')
    
# 将指定组合合约价差日行情数据导出到 excel 中
def write_spread_daily_to_xlsx(fut_code):
    engine_ts = creat_engine_with_database('futures')
    sql = "select distinct spread_type from fut_spread_daily where fut_code = '{}' order by spread_type desc;".format(fut_code)
    spread_type_df = read_data(engine_ts, sql)
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    code_num = len(fut_code)
    
    for i in range(0, len(spread_type_df)):
        spread_type = spread_type_df.loc[i]['spread_type']
        sql = "select distinct ts_code from fut_spread_daily where fut_code = '{}' and spread_type = '{}' order by ts_code;".format(fut_code, spread_type)
        ts_code_df = read_data(engine_ts, sql)
        
        # 只保留临近四年的合约组合
        while len(ts_code_df) > 4:
            ts_code_df.drop([0], inplace=True)
            # 重置序号，不然会报错
            ts_code_df = ts_code_df.reset_index(drop=True)
        
        # 获取多年同跨月类型合约组合交易日的并集（为了展示在一张散点图上），并获取分合约组合分交易日期的收盘价差字典
        date_set = set()
        comb_dict = {}
        start_year = {}
        cnt_of_code = len(ts_code_df)
        for j in range(0, cnt_of_code):
            ts_code = ts_code_df.loc[j]['ts_code']
            sql = "select trade_date, close from fut_spread_daily where ts_code = '{}' and close is not NULL order by trade_date;".format(ts_code)
            df = read_data(engine_ts, sql)
            start_year[j] = df.loc[0]['trade_date'][2:4]
            close_dict = {}
            for k in range(0, len(df)):
                if df.loc[k]['trade_date'][2:4] > start_year[j]:
                    date = '31' + df.loc[k]['trade_date'][-4:]
                else:
                    date = '30' + df.loc[k]['trade_date'][-4:]
                date_set.add(date)
                close_dict[date] = df.loc[k]['close']
            comb_dict[ts_code] = close_dict
        # 交易日并集小于 90 天的不纳入统计中
        if len(date_set) < 90:
            continue
        date_list = sorted(date_set)
        
        title = ['统一日期']
        for j in range(0, cnt_of_code):
            title.append(ts_code_df.loc[j]['ts_code'][code_num:code_num + 2] + '年价差')
        title.append('统一日期')
        for j in range(0, cnt_of_code):
            title.append(ts_code_df.loc[j]['ts_code'][code_num:code_num + 2] + '年一腿价格')
        ws = wb.sheets.add(spread_type)
        ws.range('A1').value = title
        rng = ws.range('A1').expand()
        for j in range(0, len(title)):
            rng.columns[j][0].color = (211, 211, 211)
        
        # 一腿价格字典
        first_dict = {}
        first_leg_lowest = 99999
        for j in range(0, cnt_of_code):
            ts_code = ts_code_df.loc[j]['ts_code']
            first_leg = ts_code[:ts_code.index('-')]
            first_leg_list = [first_leg + '%']
            sql = "select trade_date, close from fut_daily where ts_code like %(tt)s and close is not NULL order by trade_date;"
            df = pd.read_sql_query(sql, engine_ts, params={'tt':first_leg_list})
            close_dict = {}
            for k in range(0, len(df)):
                if df.loc[k]['trade_date'][2:4] > start_year[j]:
                    date = '31' + df.loc[k]['trade_date'][-4:]
                else:
                    date = '30' + df.loc[k]['trade_date'][-4:]
                close_dict[date] = df.loc[k]['close']
                first_leg_lowest = min(first_leg_lowest, df.loc[k]['close'])
            first_dict[first_leg] = close_dict
        
        # 在 excel 中填入多组合约组合的价差以及一腿价格日行情数据
        data_list = []
        for j in range(0, len(date_list)):
            date = date_list[j]
            date_str = '20' + date[:2] + '/' + date[2:4] + '/' + date[-2:]
            close_list = [date_str] + [''] * cnt_of_code + [date_str] + [''] * cnt_of_code
            for k in range(0, cnt_of_code):
                ts_code = ts_code_df.loc[k]['ts_code']
                if date in comb_dict[ts_code]:
                    close_list[k + 1] = comb_dict[ts_code][date]
                first_leg = ts_code[:ts_code.index('-')]
                if date in first_dict[first_leg]:
                    close_list[k + cnt_of_code + 2] = first_dict[first_leg][date]
            data_list.append(close_list)
        ws.range('A2').value = data_list
        ws.autofit()
        
        # 插入散点图
        cnt_of_date = len(date_list)
        chart = ws.charts.add(530, 10, 650, 400)
        chart.set_source_data(ws.range((1,1),(cnt_of_date + 1,cnt_of_code + 1)))
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
        chart.api[1].ChartTitle.Text = fut_code + ' ' + spread_type + ' 价差季节性走势（汇总）'     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].ChartStyle = 245
        
        chart = ws.charts.add(530, 420, 650, 400)
        chart.set_source_data(ws.range((1,cnt_of_code + 2),(cnt_of_date + 1,cnt_of_code * 2 + 2)))
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
        chart.api[1].ChartTitle.Text = '一腿价格季节性走势（汇总）'     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].Axes(2).MinimumScale = first_leg_lowest - 500
        chart.api[1].ChartStyle = 245
        
        print('{} {} 跨月价差数据写入完成！进度：{}%'.format(fut_code, spread_type, format((i + 1) / len(spread_type_df) * 100, '.2f')))
        
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    if len(wb.sheets) > 1:
        wb.sheets['Sheet1'].delete()
    if not os.path.exists('output/{}/所有品种价差季节性走势/'.format(todayStr)):
        os.makedirs('output/{}/所有品种价差季节性走势/'.format(todayStr))
    wb.save('./output/{}/所有品种价差季节性走势/{}-{} 品种不同跨月组合价差季节性走势.xlsx'.format(todayStr, todayStr, fut_code))
    wb.close()
    app.quit()
    print('{} 品种不同跨月组合价差季节性走势 Excel 数据导出完毕！'.format(fut_code))

def test(fut_code):
    engine_ts = creat_engine_with_database('futures')
    sql = "select distinct spread_type from fut_spread_daily where fut_code = '{}' order by spread_type desc;".format(fut_code)
    spread_type_df = read_data(engine_ts, sql)
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    code_num = len(fut_code)
    
    for i in range(0, len(spread_type_df)):
        spread_type = spread_type_df.loc[i]['spread_type']
        sql = "select distinct ts_code from fut_spread_daily where fut_code = '{}' and spread_type = '{}' order by ts_code;".format(fut_code, spread_type)
        ts_code_df = read_data(engine_ts, sql)
        
        # 只保留临近四年的合约组合
        while len(ts_code_df) > 4:
            ts_code_df.drop([0], inplace=True)
            # 重置序号，不然会报错
            ts_code_df = ts_code_df.reset_index(drop=True)
        
        # 获取多年同跨月类型合约组合交易日的并集（为了展示在一张散点图上），并获取分合约组合分交易日期的收盘价差字典
        date_set = set()
        comb_dict = {}
        inventory_dict = {}
        inventory_lowest = 99999
        basis_dict = {}
        price_dict = {}
        price_lowest = 99999
        start_year = {}
        cnt_of_code = len(ts_code_df)
        for j in range(0, cnt_of_code):
            ts_code = ts_code_df.loc[j]['ts_code']
            sql = "select trade_date, close from fut_spread_daily where ts_code = '{}' and close is not NULL order by trade_date;".format(ts_code)
            df = read_data(engine_ts, sql)
            start_year[j] = df.loc[0]['trade_date'][2:4]
            
            start_date = df.loc[0]['trade_date']
            end_date = df.loc[len(df) - 1]['trade_date']
            sql = "select update_date, value from fut_funds where index_name = '甲醇-港口库存' and update_date >= '{}' and update_date <= '{}' order by update_date;".format(start_date, end_date)
            inventory_df = read_data(engine_ts, sql)
            sql = "select update_date, value from fut_funds where index_name = '甲醇主力合约基差' and update_date >= '{}' and update_date <= '{}' order by update_date;".format(start_date, end_date)
            basis_df = read_data(engine_ts, sql)
            sql = "select update_date, value from fut_funds where index_name = '甲醇（江苏低端）' and update_date >= '{}' and update_date <= '{}' order by update_date;".format(start_date, end_date)
            price_df = read_data(engine_ts, sql)
            
            close_dict = {}
            for k in range(0, len(df)):
                if df.loc[k]['trade_date'][2:4] > start_year[j]:
                    date = '31' + df.loc[k]['trade_date'][-4:]
                else:
                    date = '30' + df.loc[k]['trade_date'][-4:]
                date_set.add(date)
                close_dict[date] = df.loc[k]['close']
            comb_dict[ts_code] = close_dict
            
            value_dict = {}
            for k in range(0, len(inventory_df)):
                if inventory_df.loc[k]['update_date'][2:4] > start_year[j]:
                    date = '31' + inventory_df.loc[k]['update_date'][-4:]
                else:
                    date = '30' + inventory_df.loc[k]['update_date'][-4:]
                date_set.add(date)
                value_dict[date] = inventory_df.loc[k]['value']
                inventory_lowest = min(inventory_lowest, inventory_df.loc[k]['value'])
            inventory_dict[ts_code] = value_dict
            
            value_dict = {}
            for k in range(0, len(basis_df)):
                if basis_df.loc[k]['update_date'][2:4] > start_year[j]:
                    date = '31' + basis_df.loc[k]['update_date'][-4:]
                else:
                    date = '30' + basis_df.loc[k]['update_date'][-4:]
                date_set.add(date)
                value_dict[date] = basis_df.loc[k]['value']
            basis_dict[ts_code] = value_dict
            
            value_dict = {}
            for k in range(0, len(price_df)):
                if price_df.loc[k]['update_date'][2:4] > start_year[j]:
                    date = '31' + price_df.loc[k]['update_date'][-4:]
                else:
                    date = '30' + price_df.loc[k]['update_date'][-4:]
                date_set.add(date)
                value_dict[date] = price_df.loc[k]['value']
                price_lowest = min(price_lowest, price_df.loc[k]['value'])
            price_dict[ts_code] = value_dict
        
        date_list = sorted(date_set)
        
        title = ['统一日期']
        for j in range(0, cnt_of_code):
            title.append(ts_code_df.loc[j]['ts_code'][code_num:code_num + 2] + '年价差')
        title.append('统一日期')
        for j in range(0, cnt_of_code):
            title.append(ts_code_df.loc[j]['ts_code'][code_num:code_num + 2] + '年一腿价格')
        title.append('统一日期')
        for j in range(0, cnt_of_code):
            title.append(ts_code_df.loc[j]['ts_code'][code_num:code_num + 2] + '年库存')
        title.append('统一日期')
        for j in range(0, cnt_of_code):
            title.append(ts_code_df.loc[j]['ts_code'][code_num:code_num + 2] + '年主力合约基差')
        title.append('统一日期')
        for j in range(0, cnt_of_code):
            title.append(ts_code_df.loc[j]['ts_code'][code_num:code_num + 2] + '年现货价格')
        ws = wb.sheets.add(spread_type)
        ws.range('A1').value = title
        rng = ws.range('A1').expand()
        for j in range(0, len(title)):
            rng.columns[j][0].color = (211, 211, 211)
        
        # 一腿价格字典
        first_dict = {}
        first_leg_lowest = 99999
        for j in range(0, cnt_of_code):
            ts_code = ts_code_df.loc[j]['ts_code']
            first_leg = ts_code[:ts_code.index('-')]
            first_leg_list = [first_leg + '%']
            sql = "select trade_date, close from fut_daily where ts_code like %(tt)s and close is not NULL order by trade_date;"
            df = pd.read_sql_query(sql, engine_ts, params={'tt':first_leg_list})
            close_dict = {}
            for k in range(0, len(df)):
                if df.loc[k]['trade_date'][2:4] > start_year[j]:
                    date = '31' + df.loc[k]['trade_date'][-4:]
                else:
                    date = '30' + df.loc[k]['trade_date'][-4:]
                close_dict[date] = df.loc[k]['close']
                first_leg_lowest = min(first_leg_lowest, df.loc[k]['close'])
            first_dict[first_leg] = close_dict
        
        # 在 excel 中填入多组合约组合的价差以及一腿价格日行情数据
        data_list = []
        for j in range(0, len(date_list)):
            date = date_list[j]
            date_str = '20' + date[:2] + '/' + date[2:4] + '/' + date[-2:]
            close_list = [date_str] + [''] * cnt_of_code + [date_str] + [''] * cnt_of_code + [date_str] + [''] * cnt_of_code + [date_str] + [''] * cnt_of_code + [date_str] + [''] * cnt_of_code
            for k in range(0, cnt_of_code):
                ts_code = ts_code_df.loc[k]['ts_code']
                if date in comb_dict[ts_code]:
                    close_list[k + 1] = comb_dict[ts_code][date]
                first_leg = ts_code[:ts_code.index('-')]
                if date in first_dict[first_leg]:
                    close_list[k + cnt_of_code + 2] = first_dict[first_leg][date]
                if date in inventory_dict[ts_code]:
                    close_list[k + cnt_of_code * 2 + 3] = inventory_dict[ts_code][date]
                if date in basis_dict[ts_code]:
                    close_list[k + cnt_of_code * 3 + 4] = basis_dict[ts_code][date]
                if date in price_dict[ts_code]:
                    close_list[k + cnt_of_code * 4 + 5] = price_dict[ts_code][date]
            data_list.append(close_list)
        ws.range('A2').value = data_list
        ws.autofit()
        
        # 插入价差散点图
        cnt_of_date = len(date_list)
        chart = ws.charts.add(20, 20, 650, 400)
        chart.set_source_data(ws.range((1,1),(cnt_of_date + 1,cnt_of_code + 1)))
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
        chart.api[1].ChartTitle.Text = fut_code + ' ' + spread_type + ' 价差季节性走势（汇总）'     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].ChartStyle = 245
        
        # 插入一腿价格散点图
        chart = ws.charts.add(20, 395, 650, 400)
        chart.set_source_data(ws.range((1,cnt_of_code + 2),(cnt_of_date + 1,cnt_of_code * 2 + 2)))
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
        chart.api[1].ChartTitle.Text = '一腿价格季节性走势（汇总）'     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].Axes(2).MinimumScale = first_leg_lowest - 500
        chart.api[1].ChartStyle = 245
        
        # 插入库存散点图
        chart = ws.charts.add(670, 20, 650, 400)
        chart.set_source_data(ws.range((1,cnt_of_code * 2 + 3),(cnt_of_date + 1,cnt_of_code * 3 + 3)))
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
        chart.api[1].ChartTitle.Text = '甲醇-港口库存'     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].Axes(2).MinimumScale = inventory_lowest - 20
        chart.api[1].ChartStyle = 245
        
        # 插入主力合约基差散点图
        chart = ws.charts.add(670, 395, 650, 400)
        chart.set_source_data(ws.range((1,cnt_of_code * 3 + 4),(cnt_of_date + 1,cnt_of_code * 4 + 4)))
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
        chart.api[1].ChartTitle.Text = '甲醇主力合约基差'     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].ChartStyle = 245
        
        # 插入现货价格散点图
        chart = ws.charts.add(1320, 20, 650, 400)
        chart.set_source_data(ws.range((1,cnt_of_code * 4 + 5),(cnt_of_date + 1,cnt_of_code * 5 + 5)))
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
        chart.api[1].ChartTitle.Text = '甲醇现货价格'     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].Axes(2).MinimumScale = price_lowest - 500
        chart.api[1].ChartStyle = 245
        
        print('{} {} 跨月价差数据写入完成！进度：{}%'.format(fut_code, spread_type, format((i + 1) / len(spread_type_df) * 100, '.2f')))
    
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    if len(wb.sheets) > 1:
        wb.sheets['Sheet1'].delete()
    wb.save('./output/{}-{} 品种不同跨月组合价差季节性走势.xlsx'.format(todayStr, fut_code))
    wb.close()
    app.quit()
    print('{} 品种不同跨月组合价差季节性走势 Excel 数据导出完毕！'.format(fut_code))
    
def test_funds(fut_code):
    # 获取最新交易日
    engine_ts = creat_engine_with_database('futures')
    sql = "select distinct trade_date from fut_spread_daily where fut_code = '{}' order by trade_date desc limit 1".format(fut_code)
    last_trade_date_df = read_data(engine_ts, sql)
    last_trade_date = last_trade_date_df.loc[0]['trade_date']
    
    # 获取三个交易量最大的品种
    sql = "select ts_code, vol from fut_spread_daily where fut_code = '{}' and trade_date = '{}' order by vol desc limit 3".format(fut_code, last_trade_date)
    main_ts_code_df = read_data(engine_ts, sql)
    main_ts_code_df.sort_values(by='ts_code', ascending=True, inplace=True)
    main_ts_code_df.reset_index(drop=True, inplace=True)
    
    # 获取最近合约名称以及所有合约组合交易时间的并集（总区间）
    nearly_ts_code = 'zzzzzz'
    start_date = '99999999'
    end_date = '00000000'
    for i in range(0, len(main_ts_code_df)):
        ts_code = main_ts_code_df.loc[i]['ts_code']
        nearly_ts_code = min(nearly_ts_code, ts_code[:ts_code.index('-')])
        sql = "select trade_date from fut_spread_daily where ts_code = '{}' order by trade_date limit 1".format(ts_code)
        date_df = read_data(engine_ts, sql)
        date = date_df.loc[0]['trade_date']
        start_date = min(start_date, date)
        sql = "select trade_date from fut_spread_daily where ts_code = '{}' order by trade_date desc limit 1".format(ts_code)
        date_df = read_data(engine_ts, sql)
        date = date_df.loc[0]['trade_date']
        end_date = max(end_date, date)
    
    # 获取历史现货价格和期货价格，手动计算基差
    sql = "select update_date, value from fut_funds where fut_code = '{}' and index_type = '{}' and update_date >= '{}' and update_date <= '{}' order by update_date".format(fut_code, '现货价格', start_date, end_date)
    spot_price_df = read_data(engine_ts, sql)
    nearly_ts_code_list = [nearly_ts_code + '%']
    sql = "select trade_date, close from fut_daily where ts_code like %(tt)s and close is not NULL and trade_date >= '{}' and trade_date <= '{}' order by trade_date;".format(start_date, end_date)
    nearly_close_df = pd.read_sql_query(sql, engine_ts, params={'tt':nearly_ts_code_list})
    basis_dict = {}
    for i in range(0, len(spot_price_df)):
        date = spot_price_df.loc[i]['update_date']
        if date in nearly_close_df['trade_date'].values:
            spot_price = spot_price_df.loc[i]['value']
            close_df = nearly_close_df[nearly_close_df['trade_date'] == date]
            close_df.reset_index(drop=True, inplace=True)
            close = close_df.loc[0]['close']
            date = date[:4] + '/' + date[4:6] + '/' + date[-2:]
            basis_dict[date] = spot_price - close
    basis_list = [[k, v] for k, v in basis_dict.items()]
    
    # 获取时间区间内三个主流组合的价差数据
    close_dict = {}
    for i in range(0, len(main_ts_code_df)):
        ts_code = main_ts_code_df.loc[i]['ts_code']
        sql = "select trade_date, close from fut_spread_daily where ts_code = '{}' and trade_date >= '{}' and trade_date <= '{}' order by trade_date;".format(ts_code, start_date, end_date)
        close_df = read_data(engine_ts, sql)
        tmp_dict = {}
        date = start_date
        date = date[:4] + '/' + date[4:6] + '/' + date[-2:]
        tmp_dict[date] = ''
        for j in range(0, len(close_df)):
            date = close_df.loc[j]['trade_date']
            date = date[:4] + '/' + date[4:6] + '/' + date[-2:]
            tmp_dict[date] = close_df.loc[j]['close']
        tmp_list = [[k, v] for k, v in tmp_dict.items()]
        close_dict[ts_code] = tmp_list
    
    # 获取库存信息
    sql = "select update_date, value from fut_funds where fut_code = '{}' and index_name = '{}' and update_date >= '{}' and update_date <= '{}' order by update_date".format(fut_code, '甲醇-港口库存', start_date, end_date)
    inventory_df = read_data(engine_ts, sql)
    inventory_dict = {}
    date = start_date
    date = date[:4] + '/' + date[4:6] + '/' + date[-2:]
    inventory_dict[date] = ''
    for i in range(0, len(inventory_df)):
        date = inventory_df.loc[i]['update_date']
        date = date[:4] + '/' + date[4:6] + '/' + date[-2:]
        inventory_dict[date] = inventory_df.loc[i]['value']
    date = end_date
    date = date[:4] + '/' + date[4:6] + '/' + date[-2:]
    if date not in inventory_dict.keys():
        inventory_dict[date] = ''
    inventory_list = [[k, v] for k, v in inventory_dict.items()]
    
    # 标题行数组
    title = ['日期']
    title.append("{}{}基差".format(fut_code, nearly_ts_code[-2:]))
    for i in range(0, len(main_ts_code_df)):
        title.append('日期')
        ts_code = main_ts_code_df.loc[i]['ts_code']
        ts_code = ts_code[ts_code.index('-') - 2:ts_code.index('-')] + '-' + ts_code[-2:]
        title.append("{}价差".format(ts_code))
    title.append('日期')
    title.append("甲醇-港口库存")
    
    # 打开 excel
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    ws = wb.sheets.add('test')
    ws.range('A1').value = title
    rng = ws.range('A1').expand()
    for i in range(0, len(title)):
        rng.columns[i][0].color = (211, 211, 211)
        
    # 写入内容
    ws.range('A2').value = basis_list
    chara = 'A'
    for i in close_dict.values():
        chara = chr(ord(chara) + 2)
        ws.range('{}2'.format(chara)).value = i
    chara = chr(ord(chara) + 2)
    ws.range('{}2'.format(chara)).value = inventory_list
    ws.autofit()
    
    # 插入图像
    cnt_of_date = len(basis_list)
    chart = ws.charts.add(20, 20, 650, 400)
    chart.set_source_data(ws.range((1,1),(cnt_of_date + 1,2)))
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
    chart.api[1].ChartTitle.Text = "{}{}基差".format(fut_code, nearly_ts_code[-2:])     #改变标题文本
    # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
    chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
    chart.api[1].Legend.Position = -4107    # 图例显示在下方
    # chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
    chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
    chart.api[1].ChartStyle = 245       # 图表格式
    chart.api[1].ChartColor = 12        # 图表色系
    
    j = 0
    for i in close_dict.keys():
        cnt_of_date = len(close_dict[i])
        j += 1
        chart = ws.charts.add(20, 20 + j * 390, 650, 400)
        chart.set_source_data(ws.range((1,1 + j * 2),(cnt_of_date + 1, 2 + j * 2)))
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
        ts_code = i[i.index('-') - 2:i.index('-')] + '-' + i[-2:]
        chart.api[1].ChartTitle.Text = "{}价差".format(ts_code)     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        # chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].ChartStyle = 245       # 图表格式
        chart.api[1].ChartColor = 12        # 图表色系
    
    cnt_of_date = len(inventory_list)
    j += 1
    chart = ws.charts.add(20, 20 + j * 390, 650, 400)
    chart.set_source_data(ws.range((1,1 + j * 2),(cnt_of_date + 1, 2 + j * 2)))
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
    ts_code = i[i.index('-') - 2:i.index('-')] + '-' + i[-2:]
    chart.api[1].ChartTitle.Text = "甲醇-港口库存"     #改变标题文本
    # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
    chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
    chart.api[1].Legend.Position = -4107    # 图例显示在下方
    # chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
    chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
    chart.api[1].ChartStyle = 245       # 图表格式
    chart.api[1].ChartColor = 12        # 图表色系
    
    wb.save('./test.xlsx')
    wb.close()
    app.quit()
    exit(1)
        
    
    
    sql = "select index_type, value, update_date from fut_funds where fut_code = '{}' and index_type = '基差' and update_date > '20200101' order by update_date".format(fut_code)
    basis_df = read_data(engine_ts, sql)
    
    date_set = set()
    basis_dict = {}
    value_dict = {}
    year = ''
    for i in range(0, len(basis_df)):
        date = basis_df.loc[i]['update_date']
        if year != date[2:4] and len(year):
            basis_dict[year] = value_dict
            value_dict.clear()
        year = date[2:4]
        date = '2030/' + date[4:6] + '/' + date[6:]
        date_set.add(date)
        value_dict[date] = basis_df.loc[i]['value']
    basis_dict[year] = value_dict
    date_list = sorted(date_set)
    
    print(basis_dict)
    exit(1)
    
    sql = "select distinct spread_type from fut_spread_daily where fut_code = '{}' order by spread_type desc;".format(fut_code)
    spread_type_df = read_data(engine_ts, sql)
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    code_num = len(fut_code)
    
    for i in range(0, len(spread_type_df)):
        spread_type = spread_type_df.loc[i]['spread_type']
        sql = "select distinct ts_code from fut_spread_daily where fut_code = '{}' and spread_type = '{}' order by ts_code;".format(fut_code, spread_type)
        ts_code_df = read_data(engine_ts, sql)
        
        # 只保留临近四年的合约组合
        while len(ts_code_df) > 4:
            ts_code_df.drop([0], inplace=True)
            # 重置序号，不然会报错
            ts_code_df = ts_code_df.reset_index(drop=True)
        
        # 获取多年同跨月类型合约组合交易日的并集（为了展示在一张散点图上），并获取分合约组合分交易日期的收盘价差字典
        date_set = set()
        comb_dict = {}
        inventory_dict = {}
        inventory_lowest = 99999
        basis_dict = {}
        price_dict = {}
        price_lowest = 99999
        start_year = {}
        cnt_of_code = len(ts_code_df)
        for j in range(0, cnt_of_code):
            ts_code = ts_code_df.loc[j]['ts_code']
            sql = "select trade_date, close from fut_spread_daily where ts_code = '{}' and close is not NULL order by trade_date;".format(ts_code)
            df = read_data(engine_ts, sql)
            start_year[j] = df.loc[0]['trade_date'][2:4]
            
            start_date = df.loc[0]['trade_date']
            end_date = df.loc[len(df) - 1]['trade_date']
            sql = "select update_date, value from fut_funds where index_name = '甲醇-港口库存' and update_date >= '{}' and update_date <= '{}' order by update_date;".format(start_date, end_date)
            inventory_df = read_data(engine_ts, sql)
            sql = "select update_date, value from fut_funds where index_name = '甲醇主力合约基差' and update_date >= '{}' and update_date <= '{}' order by update_date;".format(start_date, end_date)
            basis_df = read_data(engine_ts, sql)
            sql = "select update_date, value from fut_funds where index_name = '甲醇（江苏低端）' and update_date >= '{}' and update_date <= '{}' order by update_date;".format(start_date, end_date)
            price_df = read_data(engine_ts, sql)
            
            close_dict = {}
            for k in range(0, len(df)):
                if df.loc[k]['trade_date'][2:4] > start_year[j]:
                    date = '31' + df.loc[k]['trade_date'][-4:]
                else:
                    date = '30' + df.loc[k]['trade_date'][-4:]
                date_set.add(date)
                close_dict[date] = df.loc[k]['close']
            comb_dict[ts_code] = close_dict
            
            value_dict = {}
            for k in range(0, len(inventory_df)):
                if inventory_df.loc[k]['update_date'][2:4] > start_year[j]:
                    date = '31' + inventory_df.loc[k]['update_date'][-4:]
                else:
                    date = '30' + inventory_df.loc[k]['update_date'][-4:]
                date_set.add(date)
                value_dict[date] = inventory_df.loc[k]['value']
                inventory_lowest = min(inventory_lowest, inventory_df.loc[k]['value'])
            inventory_dict[ts_code] = value_dict
            
            value_dict = {}
            for k in range(0, len(basis_df)):
                if basis_df.loc[k]['update_date'][2:4] > start_year[j]:
                    date = '31' + basis_df.loc[k]['update_date'][-4:]
                else:
                    date = '30' + basis_df.loc[k]['update_date'][-4:]
                date_set.add(date)
                value_dict[date] = basis_df.loc[k]['value']
            basis_dict[ts_code] = value_dict
            
            value_dict = {}
            for k in range(0, len(price_df)):
                if price_df.loc[k]['update_date'][2:4] > start_year[j]:
                    date = '31' + price_df.loc[k]['update_date'][-4:]
                else:
                    date = '30' + price_df.loc[k]['update_date'][-4:]
                date_set.add(date)
                value_dict[date] = price_df.loc[k]['value']
                price_lowest = min(price_lowest, price_df.loc[k]['value'])
            price_dict[ts_code] = value_dict
        
        date_list = sorted(date_set)
        
        title = ['统一日期']
        for j in range(0, cnt_of_code):
            title.append(ts_code_df.loc[j]['ts_code'][code_num:code_num + 2] + '年价差')
        title.append('统一日期')
        for j in range(0, cnt_of_code):
            title.append(ts_code_df.loc[j]['ts_code'][code_num:code_num + 2] + '年一腿价格')
        title.append('统一日期')
        for j in range(0, cnt_of_code):
            title.append(ts_code_df.loc[j]['ts_code'][code_num:code_num + 2] + '年库存')
        title.append('统一日期')
        for j in range(0, cnt_of_code):
            title.append(ts_code_df.loc[j]['ts_code'][code_num:code_num + 2] + '年主力合约基差')
        title.append('统一日期')
        for j in range(0, cnt_of_code):
            title.append(ts_code_df.loc[j]['ts_code'][code_num:code_num + 2] + '年现货价格')
        ws = wb.sheets.add(spread_type)
        ws.range('A1').value = title
        rng = ws.range('A1').expand()
        for j in range(0, len(title)):
            rng.columns[j][0].color = (211, 211, 211)
        
        # 一腿价格字典
        first_dict = {}
        first_leg_lowest = 99999
        for j in range(0, cnt_of_code):
            ts_code = ts_code_df.loc[j]['ts_code']
            first_leg = ts_code[:ts_code.index('-')]
            first_leg_list = [first_leg + '%']
            sql = "select trade_date, close from fut_daily where ts_code like %(tt)s and close is not NULL order by trade_date;"
            df = pd.read_sql_query(sql, engine_ts, params={'tt':first_leg_list})
            close_dict = {}
            for k in range(0, len(df)):
                if df.loc[k]['trade_date'][2:4] > start_year[j]:
                    date = '31' + df.loc[k]['trade_date'][-4:]
                else:
                    date = '30' + df.loc[k]['trade_date'][-4:]
                close_dict[date] = df.loc[k]['close']
                first_leg_lowest = min(first_leg_lowest, df.loc[k]['close'])
            first_dict[first_leg] = close_dict
        
        # 在 excel 中填入多组合约组合的价差以及一腿价格日行情数据
        data_list = []
        for j in range(0, len(date_list)):
            date = date_list[j]
            date_str = '20' + date[:2] + '/' + date[2:4] + '/' + date[-2:]
            close_list = [date_str] + [''] * cnt_of_code + [date_str] + [''] * cnt_of_code + [date_str] + [''] * cnt_of_code + [date_str] + [''] * cnt_of_code + [date_str] + [''] * cnt_of_code
            for k in range(0, cnt_of_code):
                ts_code = ts_code_df.loc[k]['ts_code']
                if date in comb_dict[ts_code]:
                    close_list[k + 1] = comb_dict[ts_code][date]
                first_leg = ts_code[:ts_code.index('-')]
                if date in first_dict[first_leg]:
                    close_list[k + cnt_of_code + 2] = first_dict[first_leg][date]
                if date in inventory_dict[ts_code]:
                    close_list[k + cnt_of_code * 2 + 3] = inventory_dict[ts_code][date]
                if date in basis_dict[ts_code]:
                    close_list[k + cnt_of_code * 3 + 4] = basis_dict[ts_code][date]
                if date in price_dict[ts_code]:
                    close_list[k + cnt_of_code * 4 + 5] = price_dict[ts_code][date]
            data_list.append(close_list)
        ws.range('A2').value = data_list
        ws.autofit()
        
        # 插入价差散点图
        cnt_of_date = len(date_list)
        chart = ws.charts.add(20, 20, 650, 400)
        chart.set_source_data(ws.range((1,1),(cnt_of_date + 1,cnt_of_code + 1)))
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
        chart.api[1].ChartTitle.Text = fut_code + ' ' + spread_type + ' 价差季节性走势（汇总）'     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].ChartStyle = 245
        
        # 插入一腿价格散点图
        chart = ws.charts.add(20, 395, 650, 400)
        chart.set_source_data(ws.range((1,cnt_of_code + 2),(cnt_of_date + 1,cnt_of_code * 2 + 2)))
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
        chart.api[1].ChartTitle.Text = '一腿价格季节性走势（汇总）'     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].Axes(2).MinimumScale = first_leg_lowest - 500
        chart.api[1].ChartStyle = 245
        
        # 插入库存散点图
        chart = ws.charts.add(670, 20, 650, 400)
        chart.set_source_data(ws.range((1,cnt_of_code * 2 + 3),(cnt_of_date + 1,cnt_of_code * 3 + 3)))
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
        chart.api[1].ChartTitle.Text = '甲醇-港口库存'     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].Axes(2).MinimumScale = inventory_lowest - 20
        chart.api[1].ChartStyle = 245
        
        # 插入主力合约基差散点图
        chart = ws.charts.add(670, 395, 650, 400)
        chart.set_source_data(ws.range((1,cnt_of_code * 3 + 4),(cnt_of_date + 1,cnt_of_code * 4 + 4)))
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
        chart.api[1].ChartTitle.Text = '甲醇主力合约基差'     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].ChartStyle = 245
        
        # 插入现货价格散点图
        chart = ws.charts.add(1320, 20, 650, 400)
        chart.set_source_data(ws.range((1,cnt_of_code * 4 + 5),(cnt_of_date + 1,cnt_of_code * 5 + 5)))
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
        chart.api[1].ChartTitle.Text = '甲醇现货价格'     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].Axes(2).MinimumScale = price_lowest - 500
        chart.api[1].ChartStyle = 245
        
        print('{} {} 跨月价差数据写入完成！进度：{}%'.format(fut_code, spread_type, format((i + 1) / len(spread_type_df) * 100, '.2f')))
    
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    if len(wb.sheets) > 1:
        wb.sheets['Sheet1'].delete()
    wb.save('./output/{}-{} 品种不同跨月组合价差季节性走势.xlsx'.format(todayStr, fut_code))
    wb.close()
    app.quit()
    print('{} 品种不同跨月组合价差季节性走势 Excel 数据导出完毕！'.format(fut_code))

def test_dataclean():
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    fut_code = 'M'
    code_num = 1
    
    engine_ts = creat_engine_with_database('futures')
    spread_type = '01-03'
    sql = "select distinct ts_code from fut_spread_daily where fut_code = '{}' and spread_type = '{}' order by ts_code;".format(fut_code, spread_type)
    ts_code_df = read_data(engine_ts, sql)
    
    # 只保留临近四年的合约组合
    while len(ts_code_df) > 4:
        ts_code_df.drop([0], inplace=True)
        # 重置序号，不然会报错
        ts_code_df = ts_code_df.reset_index(drop=True)
    
    # 获取多年同跨月类型合约组合交易日的并集（为了展示在一张散点图上），并获取分合约组合分交易日期的收盘价差字典
    date_set = set()
    comb_dict = {}
    start_year = {}
    cnt_of_code = len(ts_code_df)
    for j in range(0, cnt_of_code):
        ts_code = ts_code_df.loc[j]['ts_code']
        sql = "select trade_date, close from fut_spread_daily where ts_code = '{}' and close is not NULL order by trade_date;".format(ts_code)
        df = read_data(engine_ts, sql)
        me = np.median(df['close'])
        mad = np.median(abs(df['close'] - me))
        up = me + (2*mad)
        down = me - (2*mad)
        df.drop(df[((df.close < down) | (df.close > up))].index, inplace=True)
        df.reset_index(drop=True, inplace=True)
        # df['close'] = np.where(df['close']>up,up,df['close'])
        # df['close'] = np.where(df['close']<down,down,df['close'])
        start_year[j] = df.loc[0]['trade_date'][2:4]
        close_dict = {}
        for k in range(0, len(df)):
            if df.loc[k]['trade_date'][2:4] > start_year[j]:
                date = '31' + df.loc[k]['trade_date'][-4:]
            else:
                date = '30' + df.loc[k]['trade_date'][-4:]
            date_set.add(date)
            close_dict[date] = df.loc[k]['close']
        comb_dict[ts_code] = close_dict
    date_list = sorted(date_set)
    
    title = ['统一日期']
    for j in range(0, cnt_of_code):
        title.append(ts_code_df.loc[j]['ts_code'][code_num:code_num + 2] + '年价差')
    title.append('统一日期')
    for j in range(0, cnt_of_code):
        title.append(ts_code_df.loc[j]['ts_code'][code_num:code_num + 2] + '年一腿价格')
    ws = wb.sheets.add(spread_type)
    ws.range('A1').value = title
    rng = ws.range('A1').expand()
    for j in range(0, len(title)):
        rng.columns[j][0].color = (211, 211, 211)
    
    # 一腿价格字典
    first_dict = {}
    first_leg_lowest = 99999
    for j in range(0, cnt_of_code):
        ts_code = ts_code_df.loc[j]['ts_code']
        first_leg = ts_code[:ts_code.index('-')]
        first_leg_list = [first_leg + '%']
        sql = "select trade_date, close from fut_daily where ts_code like %(tt)s and close is not NULL order by trade_date;"
        df = pd.read_sql_query(sql, engine_ts, params={'tt':first_leg_list})
        close_dict = {}
        for k in range(0, len(df)):
            if df.loc[k]['trade_date'][2:4] > start_year[j]:
                date = '31' + df.loc[k]['trade_date'][-4:]
            else:
                date = '30' + df.loc[k]['trade_date'][-4:]
            close_dict[date] = df.loc[k]['close']
            first_leg_lowest = min(first_leg_lowest, df.loc[k]['close'])
        first_dict[first_leg] = close_dict
    
    # 在 excel 中填入多组合约组合的价差以及一腿价格日行情数据
    data_list = []
    for j in range(0, len(date_list)):
        date = date_list[j]
        date_str = '20' + date[:2] + '/' + date[2:4] + '/' + date[-2:]
        close_list = [date_str] + [''] * cnt_of_code + [date_str] + [''] * cnt_of_code
        for k in range(0, cnt_of_code):
            ts_code = ts_code_df.loc[k]['ts_code']
            if date in comb_dict[ts_code]:
                close_list[k + 1] = comb_dict[ts_code][date]
            first_leg = ts_code[:ts_code.index('-')]
            if date in first_dict[first_leg]:
                close_list[k + cnt_of_code + 2] = first_dict[first_leg][date]
        data_list.append(close_list)
    ws.range('A2').value = data_list
    ws.autofit()
    
    # 插入散点图
    cnt_of_date = len(date_list)
    chart = ws.charts.add(530, 10, 650, 400)
    chart.set_source_data(ws.range((1,1),(cnt_of_date + 1,cnt_of_code + 1)))
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
    chart.api[1].ChartTitle.Text = fut_code + ' ' + spread_type + ' 价差季节性走势（汇总）'     #改变标题文本
    # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
    chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
    chart.api[1].Legend.Position = -4107    # 图例显示在下方
    chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
    chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
    chart.api[1].ChartStyle = 245
    
    chart = ws.charts.add(530, 420, 650, 400)
    chart.set_source_data(ws.range((1,cnt_of_code + 2),(cnt_of_date + 1,cnt_of_code * 2 + 2)))
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
    chart.api[1].ChartTitle.Text = '一腿价格季节性走势（汇总）'     #改变标题文本
    # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
    chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
    chart.api[1].Legend.Position = -4107    # 图例显示在下方
    chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
    chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
    chart.api[1].Axes(2).MinimumScale = first_leg_lowest - 500
    chart.api[1].ChartStyle = 245
    
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    if len(wb.sheets) > 1:
        wb.sheets['Sheet1'].delete()
    wb.save('./output/{}-{} 品种不同跨月组合价差季节性走势.xlsx'.format(todayStr, fut_code))
    wb.close()
    app.quit()
    print('{} 品种不同跨月组合价差季节性走势 Excel 数据导出完毕！'.format(fut_code))

def write_all_spread_daily_to_xlsx():
    engine_ts = creat_engine_with_database('futures')
    sql = "select distinct fut_code from fut_basic order by fut_code desc;"
    fut_df = read_data(engine_ts, sql)
    fut_list = fut_df['fut_code'].tolist()
    # fut_list = ['SF', 'SM', 'SP', 'SS', 'RB', 'RU', 'MA', 'SA', 'SR', 'M', 'TA', 'V', 'C', 'SN', 'NI', 'FU', 'HC', 'CF', 'RM', 'EG', 'BU']
    fut_list = ['AL', 'FG']
    for i in range(0, len(fut_list)):
        write_spread_daily_to_xlsx(fut_list[i])

def main():
    # write_all_spread_daily_to_xlsx()
    # write_spread_low_to_xlsx()
    
    # test('MA')
    test_funds('MA')
    # test_dataclean()


if __name__ == "__main__":
    main()
