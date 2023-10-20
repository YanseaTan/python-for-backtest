# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-10-18
# @Last Modified by:   Yansea
# @Last Modified time: 2023-10-20

from sqlalchemy import create_engine
import xlwings as xw
import datetime
from DatabaseTools import *

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
        print('{} 品种汇总数据写入完成！进度：{}%'.format(fut_code, format(i / len(fut_df) * 100, '.2f')))
        
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
        print('写入详细价差数据，进度：{}%'.format(format(i / cnt * 100, '.2f')))
    
    # 按品种插入所有合约组合详细数据的连续最低价差数据
    for i in range(0, len(fut_df)):
        fut_code = fut_df.loc[i]["fut_code"]
        spread_num = spread_num_dict[fut_code]
        ws = wb.sheets[fut_code]
        nRows = ws.range('A' + str(spread_num + 4)).expand('table').rows.count
        chart = ws.charts.add(480, 330, 600, 300)
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
        chart.api[1].Axes(1).MajorUnit = 60     # 横坐标单位值
        chart.api[1].Legend.Position = -4107    # 图例显示在下方
        print('{} 品种详细数据图表插入完成！进度：{}%'.format(fut_code, format(i / len(fut_df) * 100, '.2f')))
    
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    wb.save('./{}-所有品种历史最低价差统计分析.xlsx'.format(todayStr))
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
        
        cnt_of_code = len(ts_code_df)
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
        
        # 获取多年同跨月类型合约组合交易日的并集（为了展示在一张散点图上），并获取分合约组合分交易日期的收盘价差字典
        date_set = set()
        comb_dict = {}
        start_year = {}
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
        date_list = sorted(date_set)
        
        # 一腿价格字典
        first_dict = {}
        lowest = 99999
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
                lowest = min(lowest, df.loc[k]['close'])
            first_dict[first_leg] = close_dict
        
        # 在 excel 中填入多组合约组合的价差以及一腿价格日行情数据
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
            ws.range('A' + str(j + 2)).value = close_list
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
        chart.api[1].Axes(2).MinimumScale = lowest - 500
        
        print('{} {} 跨月价差数据写入完成！进度：{}%'.format(fut_code, spread_type, format(i / len(spread_type_df) * 100, '.2f')))
        
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    wb.sheets['Sheet1'].delete()
    wb.save('./{}-{} 品种不同跨月组合价差季节性走势.xlsx'.format(todayStr, fut_code))
    wb.close()
    app.quit()
    print('{} 品种不同跨月组合价差季节性走势 Excel 数据导出完毕！'.format(fut_code))

def main():
    # write_spread_low_to_xlsx()
    
    write_spread_daily_to_xlsx('SP')


if __name__ == "__main__":
    main()
