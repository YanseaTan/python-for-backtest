# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-03-26
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-28

import sqlite3
import pandas as pd
import json
import logging
import datetime
import os

MAX_NUM = 99999999

def check_illegal_num(num):
    if num <= 0 or num >= MAX_NUM or num == None:
        return True
    return False

def check_tick_data(data):
    print("检查 tick 数据...")
    InstrumentID = data[0][2]
    last_data = ()
    for i in range(0, len(data)):
        id = data[i][0]
        TradingDay = data[i][1]
        UpperLimitPrice = data[i][17]
        LowerLimitPrice = data[i][18]
        UpdateTime = data[i][21]
        UpdateMillisec = data[i][22]
        LastPrice = data[i][5]
        BidPrice1 = data[i][23]
        BidVolume1 = data[i][24]
        AskPrice1 = data[i][25]
        AskVolume1 = data[i][26]
        
        # 非交易时间检查
        if (UpdateTime > '15:00:00' and UpdateTime < '21:00:00') or (UpdateTime > '01:00:00' and UpdateTime < '09:00:00') or\
           (UpdateTime > '10:15:00' and UpdateTime < '10:30:00') or (UpdateTime > '11:30:00' and UpdateTime < '13:30:00'):
               print("【发现时间戳为非交易时间】InstrumentID：{}  id：{}  TradingDay：{}  UpdateTime：{}  UpdateMillisec：{}".format(InstrumentID, id, TradingDay, UpdateTime, UpdateMillisec))
               
        # 重复行情检查
        if last_data[1:-1] == data[i][1:-1]:
            print("【发现重复行情】InstrumentID：{}  id：{}  TradingDay：{}  UpdateTime：{}  UpdateMillisec：{}".format(InstrumentID, id, TradingDay, UpdateTime, UpdateMillisec))
            
        # 超过涨跌停价格检查
        if LastPrice > UpperLimitPrice or LastPrice < LowerLimitPrice:
            print("【发现价格超过涨跌停板】InstrumentID：{}  id：{}  TradingDay：{}  UpdateTime：{}  UpdateMillisec：{}  LastPrice：{}  UpperLimitPrice：{}  LowerLimitPrice：{}"
                  .format(InstrumentID, id, TradingDay, UpdateTime, UpdateMillisec, LastPrice, UpperLimitPrice, LowerLimitPrice))
            
        # 一档买卖价/量非法值检查
        if (check_illegal_num(BidPrice1) and check_illegal_num(AskPrice1)) or (check_illegal_num(BidVolume1) and check_illegal_num(AskVolume1)):
            print("【发现买卖价格/委托量非法值】InstrumentID：{}  id：{}  TradingDay：{}  UpdateTime：{}  UpdateMillisec：{}  BidPrice1：{}  BidVolume1：{}  AskPrice1：{}  AskVolume1：{}"
                  .format(InstrumentID, id, TradingDay, UpdateTime, UpdateMillisec, BidPrice1, BidVolume1, AskPrice1, AskVolume1))
        
        last_data = data[i]
               
def cheak_kline_data(data):
    print("检查 kline 数据...")
    InstrumentID = data[0][3]
    for i in range(0, len(data)):
        id = data[i][0]
        UpperLimitPrice = data[i][23]
        LowerLimitPrice = data[i][24]
        OpenPrice  = data[i][9] 

def check_ctp_tick_data(file):
    conn = sqlite3.connect(file)
    cursor = conn.cursor()
    # res = cursor.execute("select name from sqlite_master where type='table' order by name").fetchall()
    # print(res)
    
    print("读取 tick 行情数据...")    
    res = cursor.execute("select * from DepthMarketData where InstrumentID = 'ni2405'").fetchall()
    # columnDes = cursor.description
    # columnNames = [columnDes[i][0] for i in range(len(columnDes))]
    # df = pd.DataFrame([list(i) for i in res],columns=columnNames)
    # print(df.columns)
    check_tick_data(res)
    cursor.close()
    conn.close()

def check_ctp_kline_data(file):
    conn = sqlite3.connect(file)
    cursor = conn.cursor()
    
    print("读取 kline 行情数据...")    
    res = cursor.execute("select * from '1' where InstrumentID = 'ni2405'").fetchall()
    # columnDes = cursor.description
    # columnNames = [columnDes[i][0] for i in range(len(columnDes))]
    # df = pd.DataFrame([list(i) for i in res],columns=columnNames)
    # print(df.columns)
    # exit(1)
    cheak_kline_data(res)
    cursor.close()
    conn.close()

def clean_db_tick_data(file, table, type):
    1

# 非交易时间检查
def check_update_time(update_time, market, exchange_id):
    if market == 'stock':
        if (update_time > '15:00:00' and update_time <= '23:59:59') or (update_time >= '00:00:00' and update_time < '09:15:00') or\
            (update_time > '11:30:00' and update_time < '13:00:00'):
                return False
    
    return True

# 获取所有价格的上下边界值
def calculate_max_and_min_price(price_list):
    max_price = -MAX_NUM
    min_price = MAX_NUM
    for price in price_list:
        if price:
            max_price = max(max_price, price)
            min_price = min(min_price, price)
    
    return max_price, min_price

# 清洗 csv 格式的 tick 数据
def clean_csv_tick_data(file, outfile, market, type):
    today = datetime.date.today()
    todayStr = today.strftime('%Y%m%d')
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler('./proc/cleandata/log/{}-clean-log.log'.format(todayStr), mode='a', encoding='utf-8')
    handler.setFormatter(logging.Formatter("%(asctime)s-%(name)s-%(levelname)s: %(message)s"))
    logger.addHandler(handler)
    logging.critical("源文件：{}".format(file))
    
    data = pd.read_csv(file, low_memory=False)
    f = open('./proc/cleandata/DataFormat.json', 'r', encoding='utf-8')
    content = f.read()
    data_format = json.loads(content)
    f.close()
    format_dict = data_format['csv']['tick'][market][type]
    data.sort_values(by=data.columns[format_dict['LocalTime']], ascending=True, inplace=True)
    data.reset_index(drop=True, inplace=True)
    
    drop_list = []
    last_data_row_list = []
    for i in range(0, len(data)):
        TradingDay = data.iloc[i][format_dict['TradingDay']]
        UpdateTime = data.iloc[i][format_dict['UpdateTime']]
        UpdateMillisec = data.iloc[i][format_dict['UpdateMillisec']]
        LocalTime = data.iloc[i][format_dict['LocalTime']]
        InstrumentID = data.iloc[i][format_dict['InstrumentID']]
        ExchangeID = data.iloc[i][format_dict['ExchangeID']]
        LastPrice = data.iloc[i][format_dict['LastPrice']]
        OpenPrice = data.iloc[i][format_dict['OpenPrice']]
        HighestPrice = data.iloc[i][format_dict['HighestPrice']]
        LowestPrice = data.iloc[i][format_dict['LowestPrice']]
        Volume = data.iloc[i][format_dict['Volume']]
        Turnover = data.iloc[i][format_dict['Turnover']]
        OpenInterest = data.iloc[i][format_dict['OpenInterest']]
        ClosePrice = data.iloc[i][format_dict['ClosePrice']]
        SettlementPrice = data.iloc[i][format_dict['SettlementPrice']]
        UpperLimitPrice = data.iloc[i][format_dict['UpperLimitPrice']]
        LowerLimitPrice = data.iloc[i][format_dict['LowerLimitPrice']]
        BidPrice1 = data.iloc[i][format_dict['BidPrice1']]
        BidVolume1 = data.iloc[i][format_dict['BidVolume1']]
        AskPrice1 = data.iloc[i][format_dict['AskPrice1']]
        AskVolume1 = data.iloc[i][format_dict['AskVolume1']]
        BidPrice2 = data.iloc[i][format_dict['BidPrice2']]
        BidVolume2 = data.iloc[i][format_dict['BidVolume2']]
        AskPrice2 = data.iloc[i][format_dict['AskPrice2']]
        AskVolume2 = data.iloc[i][format_dict['AskVolume2']]
        BidPrice3 = data.iloc[i][format_dict['BidPrice3']]
        BidVolume3 = data.iloc[i][format_dict['BidVolume3']]
        AskPrice3 = data.iloc[i][format_dict['AskPrice3']]
        AskVolume3 = data.iloc[i][format_dict['AskVolume3']]
        BidPrice4 = data.iloc[i][format_dict['BidPrice4']]
        BidVolume4 = data.iloc[i][format_dict['BidVolume4']]
        AskPrice4 = data.iloc[i][format_dict['AskPrice4']]
        AskVolume4 = data.iloc[i][format_dict['AskVolume4']]
        BidPrice5 = data.iloc[i][format_dict['BidPrice5']]
        BidVolume5 = data.iloc[i][format_dict['BidVolume5']]
        AskPrice5 = data.iloc[i][format_dict['AskPrice5']]
        AskVolume5 = data.iloc[i][format_dict['AskVolume5']]
        
        MaxPrice, MinPrice = calculate_max_and_min_price([LastPrice, OpenPrice, HighestPrice, LowestPrice, ClosePrice, SettlementPrice, BidPrice1, AskPrice1,
                                                          BidPrice2, AskPrice2, BidPrice3, AskPrice3, BidPrice4, AskPrice4, BidPrice5, AskPrice5])
        
        # 非交易时间检查
        if not check_update_time(UpdateTime, market, ExchangeID):
            drop_list.append(i)
            logging.info("【发现时间戳为非交易时间】InstrumentID：{}  id：{}  TradingDay：{}  UpdateTime：{}  UpdateMillisec：{}".format(InstrumentID, i, TradingDay, UpdateTime, UpdateMillisec))
            print("【发现时间戳为非交易时间】InstrumentID：{}  id：{}  TradingDay：{}  UpdateTime：{}  UpdateMillisec：{}".format(InstrumentID, i, TradingDay, UpdateTime, UpdateMillisec))
        
        # 重复行情检查
        elif data.loc[i].tolist() == last_data_row_list:
            drop_list.append(i)
            logging.info("【发现重复行情】InstrumentID：{}  id：{}  TradingDay：{}  UpdateTime：{}  UpdateMillisec：{}".format(InstrumentID, i, TradingDay, UpdateTime, UpdateMillisec))
            print("【发现重复行情】InstrumentID：{}  id：{}  TradingDay：{}  UpdateTime：{}  UpdateMillisec：{}".format(InstrumentID, i, TradingDay, UpdateTime, UpdateMillisec))
        
        # 超过涨跌停价格检查
        elif MaxPrice > UpperLimitPrice or MinPrice < LowerLimitPrice:
            drop_list.append(i)
            logging.info("【发现价格超过涨跌停板】InstrumentID：{}  id：{}  TradingDay：{}  UpdateTime：{}  UpdateMillisec：{}  MaxPrice：{}  MinPrice：{}  UpperLimitPrice：{}  LowerLimitPrice：{}"
                  .format(InstrumentID, i, TradingDay, UpdateTime, UpdateMillisec, MaxPrice, MinPrice, UpperLimitPrice, LowerLimitPrice))
            print("【发现价格超过涨跌停板】InstrumentID：{}  id：{}  TradingDay：{}  UpdateTime：{}  UpdateMillisec：{}  MaxPrice：{}  MinPrice：{}  UpperLimitPrice：{}  LowerLimitPrice：{}"
                  .format(InstrumentID, i, TradingDay, UpdateTime, UpdateMillisec, MaxPrice, MinPrice, UpperLimitPrice, LowerLimitPrice))
        
        # 一档买卖价/量非法值检查
        elif ((check_illegal_num(BidPrice1) and check_illegal_num(AskPrice1)) or (check_illegal_num(BidVolume1) and check_illegal_num(AskVolume1))) and\
            not ((UpdateTime >= '09:15:00' and UpdateTime < '09:30:00') or (UpdateTime >= '14:57:00' and UpdateTime <= '15:00:00')):
            drop_list.append(i)
            logging.info("【发现买卖价格/委托量非法值】InstrumentID：{}  id：{}  TradingDay：{}  UpdateTime：{}  UpdateMillisec：{}  BidPrice1：{}  BidVolume1：{}  AskPrice1：{}  AskVolume1：{}"
                  .format(InstrumentID, i, TradingDay, UpdateTime, UpdateMillisec, BidPrice1, BidVolume1, AskPrice1, AskVolume1))
            print("【发现买卖价格/委托量非法值】InstrumentID：{}  id：{}  TradingDay：{}  UpdateTime：{}  UpdateMillisec：{}  BidPrice1：{}  BidVolume1：{}  AskPrice1：{}  AskVolume1：{}"
                  .format(InstrumentID, i, TradingDay, UpdateTime, UpdateMillisec, BidPrice1, BidVolume1, AskPrice1, AskVolume1))
        
        last_data_row_list = data.loc[i].tolist()
    
    data.drop(data.index[drop_list], inplace=True)
    data.reset_index(drop=True, inplace=True)
    data.to_csv(outfile, index=False)
    
def clean_db_kline_data(file, table, type):
    1
    
def clean_csv_kline_data(file, type):
    1

def main():
    # check_ctp_tick_data("./doc/DepthMarketData_20240325.db")
    # check_ctp_kline_data("./doc/KMarketData_20240325.db")
    clean_csv_tick_data('./doc/tick-data/select-stock/SelectedLevel1MD20240301-20240308.csv', './doc/tick-data/select-stock/Cleaned-SelectedLevel1MD20240301-20240308.csv', 'stock', 'huabao')


if __name__ == "__main__":
    main()
