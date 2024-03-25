# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-03-21
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-22

from WindPy import w
import pandas as pd
import xlwings as xw
import json
import os

def export_treasury_daily_data_to_csv():
    f = open('./api/windapi/fut-treasury-dict.json', 'r', encoding='utf-8')
    content = f.read()
    code_dict_list = json.loads(content)
    f.close()
    code_list = []
    for i in range(0, len(code_dict_list)):
        code_dict = code_dict_list[i]
        code_list += code_dict['treasury_code']
    
    code_list = list(set(code_list))
    code_list.sort()
    
    start_date = '2020-01-01'
    end_date = '2024-03-21'
    for i in range(0, len(code_list)):
        code = code_list[i]
        data = w.wsd(code, "carrydate,maturitydate", "2024-03-19", "2024-03-19", "contractType=NQ1;returnType=1;bondPriceType=1;PriceAdj=CP")
        if data.ErrorCode != 0:
            print(code, data.ErrorCode)
            continue
        
        start_date = max(str(data.Data[0][0])[:10], start_date)
        end_date = min(str(data.Data[1][0])[:10], end_date)
        
        data = w.wsd(code, "tbf_cvf2,tbf_spread,cleanprice,volume,ytm_b,tbf_IRR2", start_date, end_date, "contractType=NQ1;returnType=1;bondPriceType=1;PriceAdj=CP", usedf=True)
        data = data[-1]
        data.insert(0, 'trade_date', data.index)
        data.columns = ['trade_date', 'cvf', 'spread', 'clean_price', 'amount', 'ytm', 'irr']
        data.to_csv('./doc/daily-kline/treasury/{}.csv'.format(code), index=False)

        print("{} 国债现券日行情数据导出成功，进度：{}%".format(code, round((i + 1) / len(code_list) * 100, 2)))
        
    w.stop()

def myCallback(indata: w.WindData):
    if indata.ErrorCode!=0:
        print('error code:'+str(indata.ErrorCode)+'\n')
        return
    
    print(indata)

def subscribe_treasury_market_data():
    wsq_ret = w.wsq("230026.IB", "rt_last_cp,rt_last_ytm,rt_delivery_spd,rt_irr,rt_basis,rt_bid_price1ytm,rt_ask_price1ytm,rt_vol,rt_amt", func=myCallback)

    if wsq_ret.ErrorCode != 0:
        print("Error Code:", wsq_ret.ErrorCode)

    ext = ''
    while ext != 'q':
      ext = input('Enter "q" to exit')

    w.cancelRequest(0)

def get_treasury_code_list(file):
    file_path = './treasury-fut/{}'.format(file)
    data = pd.read_csv(file_path, encoding='gb18030', dtype={'银行间国债代码': str})
    data = pd.DataFrame(data)
    code_list = data['银行间国债代码'].tolist()
    for i in range(0, len(code_list)):
        code = code_list[i]
        code = code + '.IB'
        code_list[i] = code
    return code_list

def write_treasury_fut_data_to_json():
    path = './treasury-fut'
    files = os.listdir(path)
    code_list = []
    treasury_fut_dict_list = []
    for i in range(0, len(files)):
        file = files[i]
        sub_code_list = get_treasury_code_list(file)
        code_list += sub_code_list
        treasury_fut_dict = {}
        treasury_fut_dict['ts_code'] = file[:file.index('.')]
        treasury_fut_dict['treasury_code'] = sub_code_list
        treasury_fut_dict_list.append(treasury_fut_dict)
        print("{} 国债期货可交割现券数据写入完毕，进度：{}%".format(file[:file.index('.')], round((i + 1) / len(files) * 100, 2)))
        
    code_list = list(set(code_list))
    code_list.sort()
    
    f = open('./fut-treasury-dict.json', 'w')
    content = json.dumps(treasury_fut_dict_list, indent=2)
    f.write(content)
    f.close()
    print('国债期货可交割现券配置文件更新完毕！')

def main():
    w.start() # 默认命令超时时间为120秒，如需设置超时时间可以加入waitTime参数，例如waitTime=60,即设置命令超时时间为60秒  
    w.isconnected() # 判断WindPy是否已经登录成功
    
    export_treasury_daily_data_to_csv()
    
    # subscribe_treasury_market_data()
    
    # write_treasury_fut_data_to_json()


if __name__ == "__main__":
    main()
