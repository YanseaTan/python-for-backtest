# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-12-14
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-08

from sqlalchemy import create_engine
import xlwings as xw
import datetime
import os
import sys
sys.path.append('.')
from tools.DatabaseTools import *
import numpy as np

def write_funds_to_xlsx(param_list):
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    
    for n in range(0, len(param_list)):
        fut_code = param_list[n][0]
        index_name = param_list[n][1]
        
        ws = wb.sheets.add(fut_code)
        
        # 以年为单位的数据组数
        cnt_of_year = 4
    
        # 获取最新交易日
        sql = "select distinct trade_date from fut_spread_daily where fut_code = '{}' order by trade_date desc limit 1".format(fut_code)
        last_trade_date_df = read_data('futures', sql)
        last_trade_date = last_trade_date_df.loc[0]['trade_date']
        
        # 获取三个交易量最大的组合品种以及对应的跨月类型
        sql = "select ts_code, spread_type, vol from fut_spread_daily where fut_code = '{}' and trade_date = '{}' order by vol desc limit 3".format(fut_code, last_trade_date)
        main_ts_code_df = read_data('futures', sql)
        main_ts_code_df.sort_values(by='ts_code', ascending=True, inplace=True)
        main_ts_code_df.reset_index(drop=True, inplace=True)
        
        # 获取最近合约名称，所有合约组合交易时间的并集（总区间），以及日期和价差字典
        nearly_ts_code = 'zzzzzz'
        start_date = '999999'
        end_date = '000000'
        price_dict = {}
        date_dict = {}
        ts_code_df_dict = {}
        minClose_dict = {}
        for i in range(0, len(main_ts_code_df)):
            ts_code = main_ts_code_df.loc[i]['ts_code']
            nearly_ts_code = min(nearly_ts_code, ts_code[:ts_code.index('-')])
            
            spread_type = main_ts_code_df.loc[i]['spread_type']
            sql = "select distinct ts_code from fut_spread_daily where fut_code = '{}' and spread_type = '{}' order by ts_code;".format(fut_code, spread_type)
            ts_code_df = read_data('futures', sql)
            if len(ts_code_df) < cnt_of_year:
                cnt_of_year = len(ts_code_df)
            # 只保留临近指定年份的合约组合
            while len(ts_code_df) > cnt_of_year:
                ts_code_df.drop([0], inplace=True)
                ts_code_df = ts_code_df.reset_index(drop=True)
            # 重置序号，不然会报错
            ts_code_df = ts_code_df.reset_index(drop=True)
            ts_code_df_dict[spread_type] = ts_code_df
            
            # 获取多年同跨月类型合约组合交易日的并集（为了展示在一张散点图上），并获取分合约组合分交易日期的收盘价差字典
            date_set = set()
            comb_dict = {}
            start_year = {}
            minClose = 99999
            for j in range(0, len(ts_code_df)):
                ts_code = ts_code_df.loc[j]['ts_code']
                sql = "select trade_date, close from fut_spread_daily where ts_code = '{}' and close is not NULL order by trade_date;".format(ts_code)
                df = read_data('futures', sql)
                start_year[j] = df.loc[0]['trade_date'][2:4]
                
                close_dict = {}
                for k in range(0, len(df)):
                    if df.loc[k]['trade_date'][2:4] > start_year[j]:
                        date = '24' + df.loc[k]['trade_date'][-4:]
                    else:
                        date = '23' + df.loc[k]['trade_date'][-4:]
                    date_set.add(date)
                    start_date = min(start_date, date)
                    end_date = max(end_date, date)
                    minClose = min(minClose, df.loc[k]['close'])
                    close_dict[date] = df.loc[k]['close']
                comb_dict[ts_code] = close_dict
            
            price_dict[spread_type] = comb_dict
            date_list = sorted(date_set)
            date_dict[spread_type] = date_list
            minClose_dict[i] = minClose
            
        # 将不同跨月类型的价差数据整理为多个二维表格
        spread_data_dict = {}
        for i in range(0, len(main_ts_code_df)):
            spread_type = main_ts_code_df.loc[i]['spread_type']
            ts_code_df = ts_code_df_dict[spread_type]
            
            date_list = date_dict[spread_type]
            if start_date < date_list[0]:
                date_list.insert(0, start_date)
            if end_date > date_list[len(date_list) - 1]:
                date_list.append(end_date)
            comb_dict = price_dict[spread_type]
            
            data_list = []
            for j in range(0, len(date_list)):
                date = date_list[j]
                date_str = '20' + date[:2] + '/' + date[2:4] + '/' + date[-2:]
                close_list = [date_str] + [''] * cnt_of_year
                for k in range(0, cnt_of_year):
                    ts_code = ts_code_df.loc[k]['ts_code']
                    if date in comb_dict[ts_code]:
                        close_list[k + 1] = comb_dict[ts_code][date]
                data_list.append(close_list)
            spread_data_dict[spread_type] = data_list
        
        # 获取历史现货价格和期货价格，手动计算基差
        # start_date_new = last_trade_date[:4] + start_date[-4:]
        # end_date_new = str(int(last_trade_date[:4]) + 1) + end_date[-4:]
        start_date_new = str(int(last_trade_date[:4]) - 1) + start_date[-4:]
        end_date_new = last_trade_date[:4] + end_date[-4:]
        sql = "select update_date, value from fut_funds where fut_code = '{}' and index_type = '{}' and update_date >= '{}' and update_date <= '{}' order by update_date".format(fut_code, '现货价格', start_date_new, end_date_new)
        spot_price_df = read_data('futures', sql)
        nearly_ts_code_list = [nearly_ts_code + '%']
        sql = "select trade_date, close from fut_daily where ts_code like %(tt)s and close is not NULL and trade_date >= '{}' and trade_date <= '{}' order by trade_date;".format(start_date_new, end_date_new)
        engine_ts = create_engine('mysql://{}:{}@/{}?charset=utf8&use_unicode=1'.format(mysql_user, mysql_password, 'futures'))
        nearly_close_df = pd.read_sql_query(sql, engine_ts, params={'tt':nearly_ts_code_list})
        basis_dict = {}
        minBasis = 99999
        start_date_new = start_date_new[:4] + '/' + start_date_new[4:6] + '/' + start_date_new[-2:]
        basis_dict[start_date_new] = ''
        for i in range(0, len(spot_price_df)):
            date = spot_price_df.loc[i]['update_date']
            if date in nearly_close_df['trade_date'].values:
                spot_price = spot_price_df.loc[i]['value']
                close_df = nearly_close_df[nearly_close_df['trade_date'] == date]
                close_df.reset_index(drop=True, inplace=True)
                close = close_df.loc[0]['close']
                date = date[:4] + '/' + date[4:6] + '/' + date[-2:]
                basis_dict[date] = spot_price - close
                minBasis = min(minBasis, (spot_price - close))
        end_date_new = end_date_new[:4] + '/' + end_date_new[4:6] + '/' + end_date_new[-2:]
        if end_date_new not in basis_dict.keys():
            basis_dict[end_date_new] = ''
        basis_list = [[k, v] for k, v in basis_dict.items()]
        
        # 获取库存信息
        date_set = set()
        comb_dict = {}
        start_year = {}
        for i in range(0, cnt_of_year):
            add_year = int(end_date[:2]) - int(start_date[:2])
            # start_date_new = '20' + str(int(last_trade_date[2:4]) - cnt_of_year + i + 1) + start_date[-4:]
            # end_date_new = '20' + str(int(last_trade_date[2:4]) - cnt_of_year + i + add_year + 1) + end_date[-4:]
            start_date_new = '20' + str(int(last_trade_date[2:4]) - cnt_of_year + i) + start_date[-4:]
            end_date_new = '20' + str(int(last_trade_date[2:4]) - cnt_of_year + i + add_year) + end_date[-4:]
            sql = "select update_date, value from fut_funds where fut_code = '{}' and index_name = '{}' and update_date >= '{}' and update_date <= '{}' order by update_date".format(fut_code, index_name, start_date_new, end_date_new)
            df = read_data('futures', sql)
            start_year[i] = df.loc[0]['update_date'][2:4]
            
            close_dict = {}
            for j in range(0, len(df)):
                if df.loc[j]['update_date'][2:4] > start_year[i]:
                    date = '24' + df.loc[j]['update_date'][-4:]
                else:
                    date = '23' + df.loc[j]['update_date'][-4:]
                if date == '230229':
                    date = '230228'
                date_set.add(date)
                close_dict[date] = df.loc[j]['value']
            comb_dict[i] = close_dict
            
        date_list = sorted(date_set)
        if start_date < date_list[0]:
            date_list.insert(0, start_date)
        if end_date > date_list[len(date_list) - 1]:
            date_list.append(end_date)
            
        inventory_list = []
        for i in range(0, len(date_list)):
            date = date_list[i]
            date_str = '20' + date[:2] + '/' + date[2:4] + '/' + date[-2:]
            close_list = [date_str] + [''] * cnt_of_year
            for j in range(0, cnt_of_year):
                if date in comb_dict[j]:
                    close_list[j + 1] = comb_dict[j][date]
            inventory_list.append(close_list)
        
        # 标题行数组
        title = ['日期']
        title.append("{}{}基差".format(fut_code, nearly_ts_code[-2:]))
        for i in range(0, len(main_ts_code_df)):
            title.append('日期')
            spread_type = main_ts_code_df.loc[i]['spread_type']
            ts_code_df = ts_code_df_dict[spread_type]
            for j in range(0, len(ts_code_df)):
                ts_code = ts_code_df.loc[j]['ts_code']
                ts_code = ts_code[ts_code.index('-') - 4:ts_code.index('-') - 2]
                title.append("{}年价差".format(ts_code))
        title.append('日期')
        for i in range(0, cnt_of_year):
            year = str(int(last_trade_date[2:4]) - cnt_of_year + i + 1)
            title.append("{}年库存".format(year))
        
        # 写入标题
        ws.range('A1').value = title
        rng = ws.range('A1').expand()
        for i in range(0, len(title)):
            rng.columns[i][0].color = (211, 211, 211)
            
        # 写入内容
        ws.range('A2').value = basis_list
        chara = chr(ord('C') - cnt_of_year - 1)
        for j in spread_data_dict.values():
            chara = chr(ord(chara) + cnt_of_year + 1)
            ws.range('{}2'.format(chara)).value = j
        chara = chr(ord(chara) + cnt_of_year + 1)
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
        chart.api[1].Axes(2).CrossesAt = minBasis - 50
        chart.api[1].ChartStyle = 245       # 图表格式
        chart.api[1].ChartColor = 17        # 图表色系
        
        j = -1
        for i in spread_data_dict.keys():
            cnt_of_date = len(spread_data_dict[i])
            j += 1
            chart = ws.charts.add(20, 20 + (j + 1) * 390, 650, 400)
            chart.set_source_data(ws.range((1,3 + j * (cnt_of_year + 1)),(cnt_of_date + 1, 2 + (j + 1) * (cnt_of_year + 1))))
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
            chart.api[1].ChartTitle.Text = "{}价差".format(i)     #改变标题文本
            # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
            chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
            chart.api[1].Legend.Position = -4107    # 图例显示在下方
            chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
            chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
            chart.api[1].Axes(2).CrossesAt = minClose_dict[j] - 50
            chart.api[1].ChartStyle = 245       # 图表格式
        
        cnt_of_date = len(inventory_list)
        j += 1
        chart = ws.charts.add(20, 20 + (j + 1) * 390, 650, 400)
        chart.set_source_data(ws.range((1,3 + j * (cnt_of_year + 1)),(cnt_of_date + 1, 2 + (j + 1) * (cnt_of_year + 1))))
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
        chart.api[1].ChartTitle.Text = index_name     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].ChartStyle = 245       # 图表格式
        
        print('{} 品种基本面数据走势数据写入完毕！进度：{}%'.format(fut_code, format(((n + 1) / len(param_list) * 100), '.2f')))
        
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    if len(wb.sheets) > 1:
        wb.sheets['Sheet1'].delete()
    if not os.path.exists('output/{}/'.format(todayStr)):
        os.makedirs('output/{}/'.format(todayStr))
    wb.save('./output/{}/{}-主流品种基本面数据季节性走势.xlsx'.format(todayStr, todayStr))
    wb.close()
    app.quit()
    print('所有主流品种基本面数据走势 Excel 导出完毕！')

def write_spread_funds_to_xlsx(fut_code, index_name):
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    
    # 以年为单位的数据组数
    cnt_of_year = 4
    # 成交量排名前几才会被计入数据
    cnt_of_vol_sort = 10
    
    # 获取最新交易日
    sql = "select distinct trade_date from fut_spread_daily where fut_code = '{}' order by trade_date desc limit 1".format(fut_code)
    last_trade_date_df = read_data('futures', sql)
    last_trade_date = last_trade_date_df.loc[0]['trade_date']
    
    # 获取部分交易量最大的组合品种以及对应的跨月类型
    sql = "select ts_code, spread_type, vol from fut_spread_daily where fut_code = '{}' and trade_date = '{}' order by vol desc limit {}".format(fut_code, last_trade_date, cnt_of_vol_sort)
    main_ts_code_df = read_data('futures', sql)
    main_ts_code_df.sort_values(by='ts_code', ascending=False, inplace=True)
    main_ts_code_df.reset_index(drop=True, inplace=True)
    
    for m in range(0, cnt_of_vol_sort):
        spread_type = main_ts_code_df.loc[m]['spread_type']
        ws = wb.sheets.add(spread_type)
    
        # 获取近几年的合约组合
        sql = "select distinct ts_code from fut_spread_daily where fut_code = '{}' and spread_type = '{}' order by ts_code;".format(fut_code, spread_type)
        ts_code_df = read_data('futures', sql)
        if len(ts_code_df) < cnt_of_year:
            cnt_of_year = len(ts_code_df)
        # 只保留临近指定年份的合约组合
        while len(ts_code_df) > cnt_of_year:
            ts_code_df.drop([0], inplace=True)
            ts_code_df = ts_code_df.reset_index(drop=True)
        # 重置序号，不然会报错
        ts_code_df = ts_code_df.reset_index(drop=True)
        
        # 获取月差数据
        nearly_ts_code = ''
        start_date = '999999'
        end_date = '000000'
        start_year = {}
        date_set = set()
        minClose = 99999
        comb_dict = {}
        for j in range(0, len(ts_code_df)):
            ts_code = ts_code_df.loc[j]['ts_code']
            nearly_ts_code = max(nearly_ts_code, ts_code[:ts_code.index('-')])
            sql = "select trade_date, close from fut_spread_daily where ts_code = '{}' and close is not NULL order by trade_date;".format(ts_code)
            df = read_data('futures', sql)
            start_year[j] = df.loc[0]['trade_date'][2:4]
            
            close_dict = {}
            for k in range(0, len(df)):
                if df.loc[k]['trade_date'][2:4] > start_year[j]:
                    date = '24' + df.loc[k]['trade_date'][-4:]
                else:
                    date = '23' + df.loc[k]['trade_date'][-4:]
                if date == '230229':
                    date = '230228'
                date_set.add(date)
                start_date = min(start_date, date)
                end_date = max(end_date, date)
                minClose = min(minClose, df.loc[k]['close'])
                close_dict[date] = df.loc[k]['close']
            comb_dict[ts_code] = close_dict
        date_list = sorted(date_set)
        
        add_year = int(end_date[:2]) - int(start_date[:2])
        
        # 将价差数据整理为二维表格
        data_list = []
        for j in range(0, len(date_list)):
            date = date_list[j]
            date_str = '20' + date[:2] + '/' + date[2:4] + '/' + date[-2:]
            close_list = [date_str] + [''] * cnt_of_year
            for k in range(0, cnt_of_year):
                ts_code = ts_code_df.loc[k]['ts_code']
                if date in comb_dict[ts_code]:
                    close_list[k + 1] = comb_dict[ts_code][date]
            data_list.append(close_list)
            
        # 获得一腿价格信息
        first_dict = {}
        date_set = set()
        first_leg_lowest = 99999
        for j in range(0, cnt_of_year):
            ts_code = ts_code_df.loc[j]['ts_code']
            first_leg = ts_code[:ts_code.index('-')]
            first_leg_list = [first_leg + '%']
            start_date_new = '20' + str(int(ts_code[ts_code.index('-') - 4:ts_code.index('-') - 2]) - add_year) + start_date[-4:]
            end_date_new = '20' + ts_code[ts_code.index('-') - 4:ts_code.index('-') - 2] + end_date[-4:]
            sql = "select trade_date, close from fut_daily where ts_code like %(tt)s and close is not NULL and trade_date >= '{}' and trade_date <= '{}' order by trade_date;".format(start_date_new, end_date_new)
            engine_ts = create_engine('mysql://{}:{}@/{}?charset=utf8&use_unicode=1'.format(mysql_user, mysql_password, 'futures'))
            df = pd.read_sql_query(sql, engine_ts, params={'tt':first_leg_list})
            close_dict = {}
            close_dict[start_date_new[-6:]] = ''
            for k in range(0, len(df)):
                if df.loc[k]['trade_date'][2:4] > start_year[j]:
                    date = '24' + df.loc[k]['trade_date'][-4:]
                else:
                    date = '23' + df.loc[k]['trade_date'][-4:]
                if date == '230229':
                    date = '230228'
                date_set.add(date)
                close_dict[date] = df.loc[k]['close']
                first_leg_lowest = min(first_leg_lowest, df.loc[k]['close'])
            if end_date_new not in close_dict.keys():
                close_dict[end_date_new[-6:]] = ''
            first_dict[j] = close_dict
            
        date_list = sorted(date_set)
        if start_date < date_list[0]:
            date_list.insert(0, start_date)
        if end_date > date_list[len(date_list) - 1]:
            date_list.append(end_date)
            
        first_list = []
        for i in range(0, len(date_list)):
            date = date_list[i]
            date_str = '20' + date[:2] + '/' + date[2:4] + '/' + date[-2:]
            close_list = [date_str] + [''] * cnt_of_year
            for j in range(0, cnt_of_year):
                if date in first_dict[j]:
                    close_list[j + 1] = first_dict[j][date]
            first_list.append(close_list)
            
        # 获取历史现货价格和期货价格，手动计算基差
        start_date_new = str(int(last_trade_date[:4]) - add_year) + start_date[-4:]
        end_date_new = last_trade_date[:4] + end_date[-4:]
        sql = "select update_date, value from fut_funds where fut_code = '{}' and index_type = '{}' and update_date >= '{}' and update_date <= '{}' order by update_date".format(fut_code, '现货价格', start_date_new, end_date_new)
        spot_price_df = read_data('futures', sql)
        nearly_ts_code_list = [nearly_ts_code + '%']
        sql = "select trade_date, close from fut_daily where ts_code like %(tt)s and close is not NULL and trade_date >= '{}' and trade_date <= '{}' order by trade_date;".format(start_date_new, end_date_new)
        nearly_close_df = pd.read_sql_query(sql, engine_ts, params={'tt':nearly_ts_code_list})
        basis_dict = {}
        minBasis = 99999
        spot_start_year = start_date_new[2:4]
        start_date_new = start_date_new[:2] + '23' + '/' + start_date_new[4:6] + '/' + start_date_new[-2:]
        basis_dict[start_date_new] = ''
        for i in range(0, len(spot_price_df)):
            date = spot_price_df.loc[i]['update_date']
            if date in nearly_close_df['trade_date'].values:
                spot_price = spot_price_df.loc[i]['value']
                close_df = nearly_close_df[nearly_close_df['trade_date'] == date]
                close_df.reset_index(drop=True, inplace=True)
                close = close_df.loc[0]['close']
                if date[2:4] > spot_start_year:
                    date = '2024' + date[-4:]
                else:
                    date = '2023' + date[-4:]
                if date == '20230229':
                    date = '20230228'
                date = date[:4] + '/' + date[4:6] + '/' + date[-2:]
                basis_dict[date] = spot_price - close
                minBasis = min(minBasis, (spot_price - close))
        if end_date_new[2:4] > spot_start_year:
            end_date_new = end_date_new[:2] + '24' + '/' + end_date_new[4:6] + '/' + end_date_new[-2:]
        else:
            end_date_new = end_date_new[:2] + '23' + '/' + end_date_new[4:6] + '/' + end_date_new[-2:]
        if end_date_new not in basis_dict.keys():
            basis_dict[end_date_new] = ''
        basis_list = [[k, v] for k, v in basis_dict.items()]
        
        # 获取库存信息
        date_set = set()
        inventory_dict = {}
        start_year = {}
        for i in range(0, cnt_of_year):
            start_date_new = '20' + str(int(last_trade_date[2:4]) - cnt_of_year + i + 1 - add_year) + start_date[-4:]
            end_date_new = '20' + str(int(last_trade_date[2:4]) - cnt_of_year + i + 1) + end_date[-4:]
            sql = "select update_date, value from fut_funds where fut_code = '{}' and index_name = '{}' and update_date >= '{}' and update_date <= '{}' order by update_date".format(fut_code, index_name, start_date_new, end_date_new)
            df = read_data('futures', sql)
            start_year[i] = df.loc[0]['update_date'][2:4]
            
            close_dict = {}
            for j in range(0, len(df)):
                if df.loc[j]['update_date'][2:4] > start_year[i]:
                    date = '24' + df.loc[j]['update_date'][-4:]
                else:
                    date = '23' + df.loc[j]['update_date'][-4:]
                if date == '230229':
                    date = '230228'
                date_set.add(date)
                close_dict[date] = df.loc[j]['value']
            inventory_dict[i] = close_dict
            
        date_list = sorted(date_set)
        if start_date < date_list[0]:
            date_list.insert(0, start_date)
        if end_date > date_list[len(date_list) - 1]:
            date_list.append(end_date)
            
        inventory_list = []
        for i in range(0, len(date_list)):
            date = date_list[i]
            date_str = '20' + date[:2] + '/' + date[2:4] + '/' + date[-2:]
            close_list = [date_str] + [''] * cnt_of_year
            for j in range(0, cnt_of_year):
                if date in inventory_dict[j]:
                    close_list[j + 1] = inventory_dict[j][date]
            inventory_list.append(close_list)
            
        # 获取仓单信息
        date_set = set()
        warehouse_dict = {}
        start_year = {}
        for i in range(0, cnt_of_year):
            start_date_new = '20' + str(int(last_trade_date[2:4]) - cnt_of_year + i + 1 - add_year) + start_date[-4:]
            end_date_new = '20' + str(int(last_trade_date[2:4]) - cnt_of_year + i + 1) + end_date[-4:]
            sql = "select trade_date, vol from fut_warehouse_sum where symbol = '{}' and vol is not NULL and trade_date >= '{}' and trade_date <= '{}' order by trade_date".format(fut_code, start_date_new, end_date_new)
            vol_df = read_data('futures', sql)
            if len(vol_df):
                start_year[i] = vol_df.loc[0]['trade_date'][2:4]
                vol_dict = {}
                for j in range(0, len(vol_df)):
                    if vol_df.loc[j]['trade_date'][2:4] > start_year[i]:
                        date = '24' + vol_df.loc[j]['trade_date'][-4:]
                    else:
                        date = '23' + vol_df.loc[j]['trade_date'][-4:]
                    if date == '230229':
                        date = '230228'
                    date_set.add(date)
                    vol_dict[date] = vol_df.loc[j]['vol']
                warehouse_dict[i] = vol_dict
        
        date_list = sorted(date_set)
        if start_date < date_list[0]:
            date_list.insert(0, start_date)
        if end_date > date_list[len(date_list) - 1]:
            date_list.append(end_date)
            
        warehouse_list = []
        for i in range(0, len(date_list)):
            date = date_list[i]
            date_str = '20' + date[:2] + '/' + date[2:4] + '/' + date[-2:]
            close_list = [date_str] + [''] * cnt_of_year
            for j in range(0, cnt_of_year):
                if j in warehouse_dict.keys():
                    if date in warehouse_dict[j]:
                        close_list[j + 1] = warehouse_dict[j][date]
            warehouse_list.append(close_list)
        
        # 标题行数组
        title = ['日期']
        for j in range(0, len(ts_code_df)):
            ts_code = ts_code_df.loc[j]['ts_code']
            ts_code = ts_code[ts_code.index('-') - 4:ts_code.index('-') - 2]
            title.append("{}年价差".format(ts_code))
        title.append('日期')
        for j in range(0, len(ts_code_df)):
            ts_code = ts_code_df.loc[j]['ts_code']
            ts_code = ts_code[ts_code.index('-') - 4:ts_code.index('-') - 2]
            title.append("{}年一腿价格".format(ts_code))
        title.append('日期')
        title.append("{}{}基差".format(fut_code, nearly_ts_code[-2:]))
        title.append('日期')
        for j in range(0, cnt_of_year):
            year = str(int(last_trade_date[2:4]) - cnt_of_year + j + 1)
            title.append("{}年库存".format(year))
        title.append('日期')
        for j in range(0, cnt_of_year):
            year = str(int(last_trade_date[2:4]) - cnt_of_year + j + 1)
            title.append("{}年仓单".format(year))
        
        # 写入标题
        ws.range('A1').value = title
        rng = ws.range('A1').expand()
        for i in range(0, len(title)):
            rng.columns[i][0].color = (211, 211, 211)
            
        # 写入内容
        ws.range('A2').value = data_list
        chara = chr(ord('A') + cnt_of_year + 1)
        ws.range('{}2'.format(chara)).value = first_list
        chara = chr(ord(chara) + cnt_of_year + 1)
        ws.range('{}2'.format(chara)).value = basis_list
        chara = chr(ord(chara) + 2)
        ws.range('{}2'.format(chara)).value = inventory_list
        chara = chr(ord(chara) + cnt_of_year + 1)
        ws.range('{}2'.format(chara)).value = warehouse_list
        ws.autofit()
        
        # 插入图像
        # 月差图
        cnt_of_date = len(data_list)
        chart = ws.charts.add(20, 20, 650, 400)
        chart.set_source_data(ws.range((1,1),(cnt_of_date + 1,5)))
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
        chart.api[1].ChartTitle.Text = "{}{}价差".format(fut_code, spread_type)     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].Axes(2).CrossesAt = minClose - 50
        chart.api[1].Axes(2).MinimumScale = minClose - 50
        chart.api[1].ChartStyle = 245       # 图表格式
        
        # 一腿价格图
        cnt_of_date = len(first_list)
        chart = ws.charts.add(20, 410, 650, 400)
        chart.set_source_data(ws.range((1,2 + cnt_of_year),(cnt_of_date + 1,2 + cnt_of_year * 2)))
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
        chart.api[1].ChartTitle.Text = "{}{}一腿价格".format(fut_code, spread_type[:2])     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].Axes(2).CrossesAt = first_leg_lowest - 500
        chart.api[1].Axes(2).MinimumScale = first_leg_lowest - 500
        chart.api[1].ChartStyle = 245       # 图表格式
        
        # 基差图
        cnt_of_date = len(basis_list)
        chart = ws.charts.add(20, 800, 650, 400)
        chart.set_source_data(ws.range((1,3 + cnt_of_year * 2),(cnt_of_date + 1,4 + cnt_of_year * 2)))
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
        chart.api[1].ChartTitle.Text = "{}{}基差".format(fut_code, spread_type[:2])     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        # chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].Axes(2).CrossesAt = minBasis - 50
        chart.api[1].Axes(2).MinimumScale = minBasis - 50
        chart.api[1].ChartStyle = 245       # 图表格式
        chart.api[1].ChartColor = 17        # 图表色系
        
        # 库存图
        cnt_of_date = len(inventory_list)
        chart = ws.charts.add(20, 1190, 650, 400)
        chart.set_source_data(ws.range((1,5 + cnt_of_year * 2),(cnt_of_date + 1, 5 + cnt_of_year * 3)))
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
        chart.api[1].ChartTitle.Text = index_name     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].ChartStyle = 245       # 图表格式
        
        # 仓单图
        cnt_of_date = len(warehouse_list)
        chart = ws.charts.add(20, 1580, 650, 400)
        chart.set_source_data(ws.range((1,6 + cnt_of_year * 3),(cnt_of_date + 1, 6 + cnt_of_year * 4)))
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
        chart.api[1].ChartTitle.Text = "{}仓单".format(fut_code)     #改变标题文本
        # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
        chart.api[1].Axes(1).MajorUnit = 30      # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
        chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "m/d"      # 格式化横坐标显示
        chart.api[1].ChartStyle = 245       # 图表格式
        
        print('{}{} 月差-基本面数据写入完毕！'.format(fut_code, spread_type))
        
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    if len(wb.sheets) > 1:
        wb.sheets['Sheet1'].delete()
    if not os.path.exists('output/{}/主营品种价差-基本面季节性走势/'.format(todayStr)):
        os.makedirs('output/{}/主营品种价差-基本面季节性走势/'.format(todayStr))
    wb.save('./output/{}/主营品种价差-基本面季节性走势/{}-{} 月差-基本面数据季节性走势.xlsx'.format(todayStr, todayStr, fut_code))
    wb.close()
    app.quit()
    print('{} 月差-基本面数据走势 Excel 导出完毕！'.format(fut_code))

def write_all_funds_to_xlsx():
    param_list = [['MA', '甲醇-港口库存'], ['L', '卓创库存-上游PE'], ['PP', '卓创库存-上游PP'], ['V', '社会库存合计'], ['TA', 'PTA工厂（周）'], ['EG', 'MEG港口库存'],
                  ['SF', '硅铁：60家样本企业：库存：中国（周）'], ['PF', '量化:短纤库存'], ['SM', '硅锰63家样本企业：库存'], ['BU', '沥青-华东炼厂库存量（万吨）'],
                  ['RM', '菜粕库存_中国'], ['M', '豆粕库存_中国'], ['HC', '库存:热卷(板)'], ['SR', '新增工业库存:食糖:全国'], ['C', '南港库存'], ['OI', '菜油库存_华东'],
                  ['LC', '碳酸锂样本周度库存：冶炼厂'], ['RB', 'Mysteel螺纹社会库存'], ['FG', '浮法玻璃生产线库存（万吨）'], ['SP', '港口纸浆总库存'], ['SC', '国别库存-中国'],
                  ['CF', '棉花：商业库存：中国（周）'], ['SN', '中国分地区锡锭社会库存-总库存'], ['Y', '豆油库存_中国'], ['NI', '电解镍国内社会库存（吨）'],
                  ['EB', '华东苯乙烯周度港口库存'], ['SS', '库存-不锈钢库存-中国主要地区不锈钢库存-合计库存']]
    param_dict = {}
    for i in param_list:
        param_dict[i[0]] = i[1]
    param_list = sorted(param_dict.items(), key = lambda s:s[0], reverse = True)
    write_funds_to_xlsx(param_list)
    
def write_all_spread_funds_to_xlsx():
    param_list = [['MA', '甲醇-港口库存'], ['L', '卓创库存-上游PE'], ['PP', '卓创库存-上游PP'], ['V', '社会库存合计'], ['TA', 'PTA工厂（周）'], ['EG', 'MEG港口库存'],
                  ['SF', '硅铁：60家样本企业：库存：中国（周）'], ['PF', '量化:短纤库存'], ['SM', '硅锰63家样本企业：库存'], ['BU', '沥青-华东炼厂库存量（万吨）'],
                  ['RM', '菜粕库存_中国'], ['M', '豆粕库存_中国'], ['HC', '库存:热卷(板)'], ['SR', '新增工业库存:食糖:全国'], ['C', '南港库存'], ['OI', '菜油库存_华东'],
                  ['RB', 'Mysteel螺纹社会库存'], ['FG', '浮法玻璃生产线库存（万吨）'], ['SP', '港口纸浆总库存'], ['SC', '国别库存-中国'],
                  ['CF', '棉花：商业库存：中国（周）'], ['SN', '中国分地区锡锭社会库存-总库存'], ['Y', '豆油库存_中国'], ['NI', '电解镍国内社会库存（吨）'],
                  ['EB', '华东苯乙烯周度港口库存'], ['SS', '库存-不锈钢库存-中国主要地区不锈钢库存-合计库存']]
    # param_list = [['RB', 'Mysteel螺纹社会库存']]
    for i in param_list:
        write_spread_funds_to_xlsx(i[0], i[1])

def main():
    
    write_all_spread_funds_to_xlsx()
    
    # write_all_funds_to_xlsx()
    
    # write_spread_funds_to_xlsx('M', '豆粕库存_中国')


if __name__ == "__main__":
    main()
