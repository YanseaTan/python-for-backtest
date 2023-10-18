# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-10-13
# @Last Modified by:   Yansea
# @Last Modified time: 2023-10-18

import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import xlwings as xw
import datetime

# 数据库用户配置
user = 'root'
password = '0527'
addr = 'localhost'

# 创建指定数据库操作引擎
def creat_engine_with_database(database):
    engine_ts = create_engine('mysql://{}:{}@/{}?charset=utf8&use_unicode=1'.format(user, password, database))
    return engine_ts

# 获取指定数据库的指定表格内容
def read_data(engine_ts, tableName, sql):
    df = pd.read_sql_query(sql, engine_ts)
    return df

# 将指定内容写入指定数据库的指定表格中
def write_data(engine_ts, tableName, df):
    res = df.to_sql(tableName, engine_ts, index=False, if_exists='append', chunksize=5000)
    
# 获取指定两个合约在所有重合交易日的价差数据，并存入数据库
def store_spread_daily_by_ts_code(fut_code, ins_1, ins_2):
    engine_ts = creat_engine_with_database('futures')
    sql = "select trade_date from fut_daily where ts_code = '{}' and close is not NULL;".format(ins_1)
    date_1 = read_data(engine_ts, 'fut_daily', sql)
    sql = "select trade_date from fut_daily where ts_code = '{}' and close is not NULL;".format(ins_2)
    date_2 = read_data(engine_ts, 'fut_daily', sql)
    date = pd.merge(date_1, date_2)
    sql = "select trade_date, close from fut_daily where ts_code = '{}';".format(ins_1)
    close_1 = read_data(engine_ts, 'fut_daily', sql)
    sql = "select trade_date, close from fut_daily where ts_code = '{}';".format(ins_2)
    close_2 = read_data(engine_ts, 'fut_daily', sql)
    
    ts_code = ins_1[:ins_1.index('.')] + '-' + ins_2[:ins_2.index('.')]
    spread_type = ins_1[:ins_1.index('.')][-2:] + '-' + ins_2[:ins_2.index('.')][-2:]
    # 当数据表不为空，需要新增数据时使用，适用于有可能插入重复数据的情况（更慢）
    ts_code_list = [ts_code]
    fut_code_list = [fut_code]
    spread_type_list = [spread_type]
    trade_date_list = []
    close_list = []
    df = pd.DataFrame()
    
    for i in range(0, len(date)):
        trade_date = date.loc[i]['trade_date']
        spread = close_1[close_1['trade_date'] == trade_date].iat[0, 1] - close_2[close_2['trade_date'] == trade_date].iat[0, 1]
        trade_date_list.append(trade_date)
        close_list.append(spread)
        df['ts_code'] = ts_code_list
        df['fut_code'] = fut_code_list
        df['spread_type'] = spread_type_list
        df['trade_date'] = trade_date_list
        df['close'] = close_list
        trade_date_list.clear()
        close_list.clear()
        # 写入数据库，避免 Key 重复后报错
        try:
            write_data(engine_ts, 'fut_spread_daily', df)
        except:
            continue
    
    # 当数据表为空时运行，或者保证插入数据不存在重复数据时运行（更快）
    # ts_code_list = [ts_code] * len(date)
    # fut_code_list = [fut_code] * len(date)
    # spread_type_list = [spread_type] * len(date)
    # trade_date_list = []
    # close_list = []
    # df = pd.DataFrame()
    
    # for i in range(0, len(date)):
    #     trade_date = date.loc[i]['trade_date']
    #     spread = close_1[close_1['trade_date'] == trade_date].iat[0, 1] - close_2[close_2['trade_date'] == trade_date].iat[0, 1]
    #     trade_date_list.append(trade_date)
    #     close_list.append(spread)
        
    # df['ts_code'] = ts_code_list
    # df['fut_code'] = fut_code_list
    # df['spread_type'] = spread_type_list
    # df['trade_date'] = trade_date_list
    # df['close'] = close_list
    
    # write_data(engine_ts, 'fut_spread_daily', df)
        
    print('写入完毕！数据量：{} 合约组合：{} '.format(len(date), ts_code))
    
    # 绘制图像
    # figure,axes=plt.subplots(nrows=1,ncols=2,figsize=(20,5))
    # df.plot(ax=axes[0])         # 折线图
    # df.plot.kde(ax=axes[1])     # 概率分布图
    # plt.show()                  # 保持图像显示

# 获取指定品种在指定到期日区间内所有的相邻月组合列表，并将所有合约对在重合交易日的价差数据存入数据库
def store_spread_daily_by_fut_code(fut_code, start_date, end_date):
    engine_ts = creat_engine_with_database('futures')
    sql = "select ts_code from fut_basic where fut_code = '{}' and delist_date > '{}' and delist_date < '{}' order by ts_code;".format(fut_code, start_date, end_date)
    code_df = read_data(engine_ts, 'fut_basic', sql)
    combination_list = []
    for i in range(0, len(code_df) - 1):
        ins_1 = code_df.loc[i]['ts_code']
        ins_2 = code_df.loc[i + 1]['ts_code']
        combination = []
        combination.append(ins_1)
        combination.append(ins_2)
        combination_list.append(combination)
    
    for i in range(0, len(combination_list)):
        ins_1 = combination_list[i][0]
        ins_2 = combination_list[i][1]
        store_spread_daily_by_ts_code(fut_code, ins_1, ins_2)

# 根据合约组合名称返回该组合的所有日行情信息
def get_spread_daily_by_ts_code(ts_code):
    engine_ts = creat_engine_with_database('futures')
    sql = "select * from fut_spread_daily where ts_code = '{}' order by close;".format(ts_code)
    df = read_data(engine_ts, 'fut_spread_daily', sql)
    return df

# 将所有组合合约价差数据导出到 excel 中
def write_spread_data_to_xlsx():
    engine_ts = creat_engine_with_database('futures')
    sql = "select distinct ts_code from fut_spread_daily order by ts_code;"
    code_df = read_data(engine_ts, 'fut_spread_daily', sql)
    sql = "select distinct fut_code from fut_spread_daily order by fut_code desc;"
    fut_df = read_data(engine_ts, 'fut_spread_daily', sql)
    
    # 以品种名在表格中创建不同的 sheet
    app = xw.App(visible=True,add_book=False)
    wb = app.books.add()
    title = ['名称', '一腿交割月', '5%最低', '10%最低', '15%最低', '20%最低', '最低价差', '最高价差']
    spread_num_dict = {}
    for i in range(0, len(fut_df)):
        fut_code = fut_df.loc[i]["fut_code"]
        sql = "select distinct spread_type from fut_spread_daily where fut_code = '{}' order by spread_type;".format(fut_code)
        spread_type_df = read_data(engine_ts, 'fut_spread_daily', sql)
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
            df = read_data(engine_ts, 'fut_spread_daily', sql)
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
        df = read_data(engine_ts, 'fut_spread_daily', sql)
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
        df = get_spread_daily_by_ts_code(ts_code)
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
        print('写入详细价差数据，进度：{}%'.format(format(i / len(fut_df) * 100, '.2f')))
    
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
    wb.save('./{}-所有品种历史价差走势.xlsx'.format(todayStr))
    wb.close()
    app.quit()
    print('Excel 数据导出完毕！')


def main():
    # 导入所选时间内所有合约组合的日行情价差数据到数据库中
    # engine_ts = creat_engine_with_database('futures')
    # sql = "select distinct fut_code from fut_basic order by fut_code;"
    # fut_df = read_data(engine_ts, 'fut_spread_daily', sql)
    # fut_list = fut_df['fut_code'].tolist()
    # for i in range(0, len(fut_list)):
    #     store_spread_daily_by_fut_code(fut_list[i], '20200717', '20231017')

    write_spread_data_to_xlsx()


if __name__ == "__main__":
    main()
