# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-02-22
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-14

import pandas as pd
import xlwings as xw
from sqlalchemy import create_engine

OPEN_CLOSE_NONE = 0
OPEN_CLOSE_OPEN = 1
OPEN_CLOSE_CLOSE = 2

DIRECTION_BUY = 0
DIRECTION_SELL = 1

# 账户资金记录
FundData = pd.DataFrame(columns=['acct_id', 'trade_date', 'available', 'asset', 'close_profit', 'position_profit'])
CurrentFund = {"acct_id":'', "trade_date":'', "available":0, "asset":0, "close_profit":0, "position_profit":0}

# 账户成交记录
TradeData = pd.DataFrame(columns=['acct_id', 'trade_date', 'ts_code', 'vol', 'direction', 'open_close', 'price', 'close_profit'])

# 账户持仓记录
PositionData = pd.DataFrame(columns=['acct_id', 'trade_date', 'ts_code', 'vol', 'direction', 'open_price', 'position_profit'])

# 服务器 postgre 数据库用户配置
postgre_user = 'postgres'
postgre_password = 'shan3353'
postgre_addr = '10.10.20.189:5432'
postgre_database = 'future'

# 创建 postgre 数据库操作引擎
postgre_engine_ts = create_engine('postgresql://{}:{}@{}/{}?sslmode=disable'.format(postgre_user, postgre_password, postgre_addr, postgre_database))

# 读取服务器数据库
def read_postgre_data(sql):
    df = pd.read_sql_query(sql, postgre_engine_ts)
    return df

# 获取交易日历
def get_cal_date_list(start_date, end_date):
    # print("获取交易日历...")
    sql = "select cal_date from future.fut_cal_date where cal_date >= '{}' and cal_date <= '{}' order by cal_date".format(start_date, end_date)
    date_df = read_postgre_data(sql)
    cal_date_list = date_df['cal_date'].tolist()
    return cal_date_list

# 获取日行情数据
def get_daily_md_data(database_name, table_name, param_list, start_date, end_date):
    print("获取 {} 日行情数据...".format(database_name))
    sql = "select {} from {}.{} where trade_date >= '{}' and trade_date <= '{}'".format(param_list, database_name, table_name, start_date, end_date)
    daily_md_df = read_postgre_data(sql)
    return daily_md_df

# 计算期货的剩余天数
delist_date_dict = {}
def calculate_remain_days(ts_code, trade_date):
    global delist_date_dict
    if ts_code in delist_date_dict.keys():
        delist_date = delist_date_dict[ts_code]
    else:
        sql = "select delist_date from future.fut_basic where ts_code = '{}'".format(ts_code)
        delist_date_df = read_postgre_data(sql)
        delist_date = delist_date_df.loc[0]['delist_date']
        delist_date_dict[ts_code] = delist_date
    cal_date_list = get_cal_date_list(trade_date, delist_date)
    return len(cal_date_list) - 1

# 账户资金相关
def set_init_fund(acct_id, trade_date, asset):
    CurrentFund['acct_id'] = acct_id
    CurrentFund['trade_date'] = trade_date
    CurrentFund['available'] = asset
    CurrentFund['asset'] = asset
    FundData.loc[0] = list(CurrentFund.values())
    
def add_fund_data(fund_list):
    FundData.loc[len(FundData)] = fund_list

def get_fund_data(acct_id, trade_date = ''):
    if trade_date == '':
        return FundData[FundData.acct_id == acct_id]
    else:
        return FundData[((FundData.acct_id == acct_id) & (FundData.trade_date == trade_date))]

# 账户成交相关
def add_trade_data(acct_id, trade_date, ts_code, vol, direction, open_close, price, close_profit):
    TradeData.loc[len(TradeData)] = [acct_id, trade_date, ts_code, vol, direction, open_close, price, close_profit]
    
def get_trade_data(acct_id, trade_date = ''):
    if trade_date == '':
        return TradeData[TradeData.acct_id == acct_id]
    else:
        return TradeData[((TradeData.acct_id == acct_id) & (TradeData.trade_date == trade_date))]

# 账户持仓相关
def add_position_data(acct_id, trade_date, ts_code, vol, direction, open_price, position_profit):
    PositionData.loc[len(PositionData)] = [acct_id, trade_date, ts_code, vol, direction, open_price, position_profit]

def get_position_data(acct_id, trade_date = ''):
    if trade_date == '':
        return PositionData[PositionData.acct_id == acct_id]
    else:
        return PositionData[((PositionData.acct_id == acct_id) & (PositionData.trade_date == trade_date))]

# 进行下单操作，更新账户可用资金
# fut_multiplier 为期货下单必填项，证券下单可不填
def place_order(acct_id, trade_date, ts_code, vol, direction, open_close, price, position_df, fut_multiplier=1):    
    # 无开平信息则为证券交易
    if open_close == OPEN_CLOSE_NONE:
        # 证券开仓
        if direction == DIRECTION_BUY:
            CurrentFund['available'] -= price * vol
            add_trade_data(acct_id, trade_date, ts_code, vol, direction, open_close, price, 0)
            # 查询同合约持仓记录
            position_record_df = position_df[position_df.ts_code == ts_code].copy()
            if len(position_record_df) == 0:
                add_position_data(acct_id, trade_date, ts_code, vol, direction, price, 0)
            else:
                position_record_df.reset_index(drop=True, inplace=True)
                last_vol = position_record_df.loc[0]['vol']
                last_price = position_record_df.loc[0]['open_price']
                total_vol = last_vol + vol
                open_price = round(((last_price * last_vol) + (price * vol)) / total_vol, 2)
                position_profit = round((price - open_price) * total_vol, 2)
                add_position_data(acct_id, trade_date, ts_code, total_vol, direction, open_price, position_profit)
                CurrentFund['position_profit'] += position_profit
        # 证券平仓
        elif direction == DIRECTION_SELL:
            CurrentFund['available'] += price * vol
            # 查询同合约持仓记录
            position_record_df = position_df[position_df.ts_code == ts_code].copy()
            if len(position_record_df) == 0:
                return
            position_record_df.reset_index(drop=True, inplace=True)
            last_vol = position_record_df.loc[0]['vol']
            last_price = position_record_df.loc[0]['open_price']
            remain_vol = last_vol - vol
            if remain_vol > 0:
                close_profit = (price - last_price) * vol
                position_profit = round((price - last_price) * remain_vol, 2)
                add_trade_data(acct_id, trade_date, ts_code, vol, direction, open_close, price, close_profit)
                add_position_data(acct_id, trade_date, ts_code, remain_vol, DIRECTION_BUY, last_price, position_profit)
                CurrentFund['close_profit'] += close_profit
                CurrentFund['position_profit'] += position_profit
            else:
                close_profit = (price - last_price) * vol
                add_trade_data(acct_id, trade_date, ts_code, vol, direction, open_close, price, close_profit)
                CurrentFund['close_profit'] += close_profit
                
    # 期货开仓
    elif open_close == OPEN_CLOSE_OPEN:
        add_trade_data(acct_id, trade_date, ts_code, vol, direction, open_close, price, 0)
        # 查询同合约同方向持仓记录
        position_record_df = position_df[(position_df.ts_code == ts_code) & (position_df.direction == direction)].copy()
        if len(position_record_df) == 0:
            add_position_data(acct_id, trade_date, ts_code, vol, direction, price, 0)
        else:
            position_record_df.reset_index(drop=True, inplace=True)
            last_vol = position_record_df.loc[0]['vol']
            last_price = position_record_df.loc[0]['open_price']
            total_vol = last_vol + vol
            open_price = round(((last_price * last_vol) + (price * vol)) / total_vol, 2)
            if direction == DIRECTION_BUY:
                position_profit = round((price - open_price) * total_vol * fut_multiplier, 2)
            elif direction == DIRECTION_SELL:
                position_profit = -round((price - open_price) * total_vol * fut_multiplier, 2)
            add_position_data(acct_id, trade_date, ts_code, total_vol, direction, open_price, position_profit)
            CurrentFund['position_profit'] += position_profit
    # 期货平仓
    elif open_close == OPEN_CLOSE_CLOSE:
        if direction == DIRECTION_BUY:
            opposite_direction = DIRECTION_SELL
        elif direction == DIRECTION_SELL:
            opposite_direction = DIRECTION_BUY
        # 查询同合约不同方向持仓记录
        position_record_df = position_df[(position_df.ts_code == ts_code) & (position_df.direction == opposite_direction)].copy()
        if len(position_record_df) == 0:
            return
        position_record_df.reset_index(drop=True, inplace=True)
        last_vol = position_record_df.loc[0]['vol']
        last_price = position_record_df.loc[0]['open_price']
        remain_vol = last_vol - vol
        if remain_vol > 0:
            if opposite_direction == DIRECTION_BUY:
                close_profit = (price - last_price) * vol * fut_multiplier
                position_profit = round((price - last_price) * remain_vol * fut_multiplier, 2)
            elif opposite_direction == DIRECTION_SELL:
                close_profit = -(price - last_price) * vol * fut_multiplier
                position_profit = -round((price - last_price) * remain_vol * fut_multiplier, 2)
            add_trade_data(acct_id, trade_date, ts_code, vol, direction, open_close, price, close_profit)
            add_position_data(acct_id, trade_date, ts_code, remain_vol, opposite_direction, last_price, position_profit)
            CurrentFund['close_profit'] += close_profit
            CurrentFund['available'] += close_profit
            CurrentFund['position_profit'] += position_profit
        else:
            if opposite_direction == DIRECTION_BUY:
                close_profit = (price - last_price) * vol * fut_multiplier
            elif opposite_direction == DIRECTION_SELL:
                close_profit = -(price - last_price) * vol * fut_multiplier
            add_trade_data(acct_id, trade_date, ts_code, vol, direction, open_close, price, close_profit)
            CurrentFund['close_profit'] += close_profit
            CurrentFund['available'] += close_profit

# 得到列表中数据的最大回撤
def get_max_drawdown_sys(array):
    array = pd.Series(array)
    cumsum = array.cummax()
    return max(cumsum-array)

# 得到指定交易年份交易记录的胜率
def get_win_rate(year):
    if year == '总体':
        TradeData_copy = TradeData.copy()
    else:
        TradeData_copy = TradeData[TradeData.trade_date.str.contains(year)].copy()
        TradeData_copy.reset_index(drop=True, inplace=True)
    win = 0
    loss = 0
    for i in range(0, len(TradeData_copy)):
        direction = TradeData_copy.loc[i]['direction']
        open_close = TradeData_copy.loc[i]['open_close']
        if direction == DIRECTION_BUY or open_close != OPEN_CLOSE_NONE:
            continue
        close_profit = TradeData_copy.loc[i]['close_profit']
        if close_profit >= 0:
            win += 1
        else:
            loss += 1
    if win == loss == 0:
        return 0
    else:
        return round(win * 100 / (win + loss), 2)

# 获取净值分析结果
def get_result_list(worth_list, result_name):
    print("分析{}净值结果...".format(result_name))
    result_list = [result_name]
    max_drawdown = round(get_max_drawdown_sys(worth_list) * 100, 2)
    result_list.append(str(max_drawdown) + '%')
    profit = round((worth_list[len(worth_list) - 1] - 1) * 100, 2)
    result_list.append(str(profit) + '%')
    if max_drawdown == 0:
        result_list.append('1')
    else:
        risk_rate = round(profit / max_drawdown, 1)
        if risk_rate > 0:
            result_list.append('1-' + str(risk_rate))
        else:
            result_list.append('/')
    year_profit = round((pow(worth_list[len(worth_list) - 1], 250 / len(worth_list)) - 1) * 100, 2)
    result_list.append(str(year_profit) + '%')
    win_rate = get_win_rate(result_name)
    result_list.append(str(win_rate) + '%')
    print("分析{}净值结果完毕！".format(result_name))
    return result_list

# 将结果输出至 Excel
def write_data_to_xlsx(book_name, setting_data):
    # 写入基础数据
    print('写入基础数据...')
    init_fund = FundData.loc[0]['asset']
    FundData.columns = ['账户ID', '交易日', '可用资金', '总资金', '平仓盈亏', '持仓盈亏']
    TradeData_copy = TradeData.copy()
    TradeData_copy['direction'].replace([0, 1], ['买', '卖'], inplace=True)
    TradeData_copy['open_close'].replace([0, 1, 2], ['/', '开', '平'], inplace=True)
    TradeData_copy.columns = ['账户ID', '交易日', '合约代码', '成交数量', '方向', '开平', '价格', '平仓盈亏']
    PositionData_copy = PositionData.copy()
    PositionData_copy['direction'].replace([0, 1], ['买', '卖'], inplace=True)
    PositionData_copy.columns = ['账户ID', '交易日', '合约代码', '持仓数量', '方向', '开仓均价', '持仓盈亏']
    with pd.ExcelWriter(book_name) as writer:
        FundData.to_excel(writer, sheet_name='资金数据', index=False)
        TradeData_copy.to_excel(writer, sheet_name='成交数据', index=False)
        PositionData_copy.to_excel(writer, sheet_name='持仓数据', index=False)
        setting_data.to_excel(writer, sheet_name='参数设置', index=False)
    
    # 计算净值，分析收益，插入曲线
    app = xw.App(visible=True,add_book=False)
    wb = app.books.open(book_name)
    ws = wb.sheets['资金数据']
    rng = ws.range('B1').expand()
    nrows = rng.rows.count
    date_list = ws.range(f'b2:b{nrows}').value
    asset_list = ws.range(f'd2:d{nrows}').value
    new_date_list = ['日期']
    worth_list = ['净值']
    mini_worth = 1
    max_worth = 1
    worth_dict = {}
    year = ''
    for i in range(0, len(date_list)):
        asset = asset_list[i]
        worth = round(asset / init_fund, 4)
        worth_list.append(worth)
        mini_worth = min(mini_worth, worth)
        max_worth = max(max_worth, worth)
        
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
        
    ws.range('G1').options(transpose=True).value = new_date_list
    ws.range('H1').options(transpose=True).value = worth_list
    
    result_list = [['组别', '最大回撤', '最终收益', '风险收益比', '年化收益', '胜率']]
    one_result_list = get_result_list(worth_list[1:], '总体')
    result_list.append(one_result_list)
    for year, year_worth_list in worth_dict.items():
        one_result_list = get_result_list(year_worth_list, year)
        result_list.append(one_result_list)
    
    ws.range('J1').value = result_list
    rng = ws.range('A1').expand()
    for i in range(0, 8):
        rng.columns[i][0].color = (211, 211, 211)
    rng = ws.range('J1').expand()
    for i in range(0, 6):
        rng.columns[i][0].color = (200, 255, 200)
    
    ws.autofit()
    
    # 插入净值曲线
    print("向 Excel 插入曲线...")
    cnt_of_date = len(worth_list)
    unit = (max_worth - mini_worth) / 10
    chart = ws.charts.add(20, 120, 800, 400)
    chart.set_source_data(ws.range((1,7),(cnt_of_date,8)))
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
    chart.api[1].ChartTitle.Text = "净值曲线"     #改变标题文本
    # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
    chart.api[1].Axes(2).TickLabels.NumberFormatLocal = "#,##0.000_);[红色](#,##0.000)"      # 纵坐标格式
    chart.api[1].Axes(2).MajorUnit = unit      # 纵坐标单位值
    chart.api[1].Axes(1).MajorUnit = int(len(worth_list) / 8)      # 横坐标单位值
    chart.api[1].Legend.Position = -4107    # 图例显示在下方
    # chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
    chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "yy/mm/dd"      # 格式化横坐标显示
    chart.api[1].Axes(2).CrossesAt = mini_worth - unit
    chart.api[1].Axes(2).MinimumScale = mini_worth - unit
    chart.api[1].ChartStyle = 245       # 图表格式
    
    wb.save(book_name)
    wb.close()
    app.quit()

# 输出多空平仓盈亏情况
def write_close_profit_to_xlsx(book_name):
    print('输出多空平仓盈亏情况...')
    last_trade_date = TradeData.loc[0]['trade_date']
    date_list = ['日期']
    cb_profit_list = ['多头平仓盈亏']
    fut_profit_list = ['空头平仓盈亏']
    cb_profit = 0
    fut_profit = 0
    for i in range(0, len(TradeData)):
        direction = TradeData.loc[i]['direction']
        open_close = TradeData.loc[i]['open_close']
        if (direction == DIRECTION_BUY and open_close == OPEN_CLOSE_NONE) or (direction == DIRECTION_SELL and open_close == OPEN_CLOSE_OPEN):
            continue
        trade_date = TradeData.loc[i]['trade_date']
        if trade_date != last_trade_date:
            last_trade_date = last_trade_date[:4] + '/' + last_trade_date[4:6] + '/' + last_trade_date[6:8]
            date_list.append(last_trade_date)
            cb_profit_list.append(cb_profit)
            fut_profit_list.append(fut_profit)
            last_trade_date = trade_date
        close_profit = TradeData.loc[i]['close_profit']
        if open_close == OPEN_CLOSE_NONE:
            cb_profit += close_profit
        else:
            fut_profit += close_profit
    last_trade_date = trade_date
    last_trade_date = last_trade_date[:4] + '/' + last_trade_date[4:6] + '/' + last_trade_date[6:8]
    date_list.append(last_trade_date)
    cb_profit_list.append(cb_profit)
    fut_profit_list.append(fut_profit)
    
    app = xw.App(visible=True,add_book=False)
    wb = app.books.open(book_name)
    ws = wb.sheets['成交数据']
        
    ws.range('I1').options(transpose=True).value = date_list
    ws.range('J1').options(transpose=True).value = cb_profit_list
    ws.range('K1').options(transpose=True).value = fut_profit_list
    ws.autofit()
    
    # 插入净值曲线
    print("向 Excel 插入曲线...")
    cnt_of_date = len(date_list)
    chart = ws.charts.add(20, 120, 800, 400)
    chart.set_source_data(ws.range((1,9),(cnt_of_date,11)))
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
    chart.api[1].ChartTitle.Text = "多空平仓盈亏曲线"     #改变标题文本
    # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
    # chart.api[1].Axes(2).TickLabels.NumberFormatLocal = "#,##0.00_);[红色](#,##0.00)"      # 纵坐标格式
    # chart.api[1].Axes(2).MajorUnit = (max_worth - mini_worth) / 10      # 纵坐标单位值
    chart.api[1].Axes(1).MajorUnit = int(len(date_list) / 2)      # 横坐标单位值
    chart.api[1].Legend.Position = -4107    # 图例显示在下方
    # chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
    chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "yy/mm/dd"      # 格式化横坐标显示
    # chart.api[1].Axes(2).CrossesAt = mini_worth - 0.02
    # chart.api[1].Axes(2).MinimumScale = mini_worth - 0.02
    chart.api[1].ChartStyle = 245       # 图表格式
    
    wb.save(book_name)
    wb.close()
    app.quit()
    
# 输出多空总盈亏情况
def write_total_profit_to_xlsx(book_name):
    print('输出多空总盈亏情况...')
    last_trade_date = PositionData.loc[0]['trade_date']
    date_list = ['日期']
    cb_profit_list = ['多头总盈亏']
    fut_profit_list = ['空头总盈亏']
    cb_profit = 0
    fut_profit = 0
    cb_close_profit = 0
    fut_close_profit = 0
    for i in range(0, len(PositionData)):
        trade_date = PositionData.loc[i]['trade_date']
        if trade_date != last_trade_date:
            TradeData_oneday = TradeData[TradeData.trade_date == last_trade_date].copy()
            if len(TradeData_oneday) != 0:
                TradeData_oneday.reset_index(drop=True, inplace=True)
                for j in range(0, len(TradeData_oneday)):
                    direction = TradeData_oneday.loc[j]['direction']
                    open_close = TradeData_oneday.loc[j]['open_close']
                    if (direction == DIRECTION_BUY and open_close == OPEN_CLOSE_NONE) or (direction == DIRECTION_SELL and open_close == OPEN_CLOSE_OPEN):
                        continue
                    close_profit = TradeData_oneday.loc[j]['close_profit']
                    if open_close == OPEN_CLOSE_NONE:
                        cb_close_profit += close_profit
                    else:
                        fut_close_profit += close_profit
            last_trade_date = last_trade_date[:4] + '/' + last_trade_date[4:6] + '/' + last_trade_date[6:8]
            date_list.append(last_trade_date)
            cb_profit_list.append(cb_profit + cb_close_profit)
            fut_profit_list.append(fut_profit + fut_close_profit)
            cb_profit = 0
            fut_profit = 0
            last_trade_date = trade_date
        direction = PositionData.loc[i]['direction']
        position_profit = PositionData.loc[i]['position_profit']
        if direction == DIRECTION_BUY:
            cb_profit += position_profit
        else:
            fut_profit += position_profit
    last_trade_date = trade_date
    TradeData_oneday = TradeData[TradeData.trade_date == last_trade_date].copy()
    if len(TradeData_oneday) != 0:
        TradeData_oneday.reset_index(drop=True, inplace=True)
        for j in range(0, len(TradeData_oneday)):
            direction = TradeData_oneday.loc[j]['direction']
            open_close = TradeData_oneday.loc[j]['open_close']
            if (direction == DIRECTION_BUY and open_close == OPEN_CLOSE_NONE) or (direction == DIRECTION_SELL and open_close == OPEN_CLOSE_OPEN):
                continue
            close_profit = TradeData_oneday.loc[j]['close_profit']
            if open_close == OPEN_CLOSE_NONE:
                cb_close_profit += close_profit
            else:
                fut_close_profit += close_profit
    last_trade_date = last_trade_date[:4] + '/' + last_trade_date[4:6] + '/' + last_trade_date[6:8]
    date_list.append(last_trade_date)
    cb_profit_list.append(cb_profit + cb_close_profit)
    fut_profit_list.append(fut_profit + fut_close_profit)
    
    app = xw.App(visible=True,add_book=False)
    wb = app.books.open(book_name)
    ws = wb.sheets['持仓数据']
        
    ws.range('H1').options(transpose=True).value = date_list
    ws.range('I1').options(transpose=True).value = cb_profit_list
    ws.range('J1').options(transpose=True).value = fut_profit_list
    ws.autofit()
    
    # 插入净值曲线
    print("向 Excel 插入曲线...")
    cnt_of_date = len(date_list)
    chart = ws.charts.add(20, 120, 800, 400)
    chart.set_source_data(ws.range((1,8),(cnt_of_date,10)))
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
    chart.api[1].ChartTitle.Text = "多空总盈亏曲线"     #改变标题文本
    # chart.api[1].Axes(1).MaximumScale = 13  # 横坐标最大值
    # chart.api[1].Axes(2).TickLabels.NumberFormatLocal = "#,##0.00_);[红色](#,##0.00)"      # 纵坐标格式
    # chart.api[1].Axes(2).MajorUnit = (max_worth - mini_worth) / 10      # 纵坐标单位值
    chart.api[1].Axes(1).MajorUnit = int(len(date_list) / 2)      # 横坐标单位值
    chart.api[1].Legend.Position = -4107    # 图例显示在下方
    # chart.api[1].DisplayBlanksAs = 3        # 使散点图连续显示
    chart.api[1].Axes(1).TickLabels.NumberFormatLocal = "yy/mm/dd"      # 格式化横坐标显示
    # chart.api[1].Axes(2).CrossesAt = mini_worth - 0.02
    # chart.api[1].Axes(2).MinimumScale = mini_worth - 0.02
    chart.api[1].ChartStyle = 245       # 图表格式
    
    wb.save(book_name)
    wb.close()
    app.quit()
    