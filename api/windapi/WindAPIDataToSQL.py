# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-03-21
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-21

from WindPy import w
import pandas as pd
import xlwings as xw
import json
import os

def export_treasury_daily_data_to_csv():
    finish_list = ["020005.IB","030014.IB","050004.IB","060009.IB","070006.IB","070013.IB","080006.IB","080013.IB","080020.IB","090002.IB","090005.IB",
                 "090011.IB","090020.IB","090025.IB","090030.IB","100003.IB","100009.IB","100014.IB","100018.IB","100023.IB","100026.IB","100029.IB"]
    code_list = ["100037.IB","100040.IB","110005.IB","110010.IB","110010X.IB","110012.IB","110016.IB","110016X.IB","110023.IB","120006.IB","120008.IB",
                 "120012.IB","120013.IB","120018.IB","120020.IB","130009.IB","130010.IB","130016.IB","130019.IB","130024.IB","130025.IB","140009.IB",
                 "140010.IB","140012.IB","140012X.IB","140012X2.IB","140016.IB","140017.IB","140021.IB","140021X.IB","140021X2.IB","140025.IB","140027.IB",
                 "140029.IB","140029X.IB","140029X2.IB","150005.IB","150005X.IB","150005X2.IB","150008.IB","150010.IB","150016.IB","150016X.IB","150016X2.IB",
                 "150017.IB","150021.IB","150023.IB","150023X.IB","150023X2.IB","150023X3.IB","150025.IB","150028.IB","160004.IB","160004X.IB","160004X2.IB",
                 "160008.IB","160008X.IB","160008X2.IB","160010.IB","160010X.IB","160010X2.IB","160013.IB","160017.IB","160017X.IB","160017X2.IB","160019.IB",
                 "160019X.IB","160019X2.IB","160023.IB","160023X.IB","160023X2.IB","160026.IB","1700001.IB","1700002.IB","170004.IB","170004X.IB","170004X2.IB",
                 "170005.IB","170005X.IB","170005X2.IB","170010.IB","170010X.IB","170010X2.IB","170011.IB","170013.IB","170013X.IB","170013X2.IB","170015.IB",
                 "170015X.IB","170015X2.IB","170018.IB","170018X.IB","170018X2.IB","170020.IB","170020X.IB","170020X2.IB","170022.IB","170022X.IB","170022X2.IB",
                 "170025.IB","170025X.IB","170025X2.IB","170026.IB","170027.IB","170027X.IB","170027X2.IB","180004.IB","180004X.IB","180004X2.IB","180005.IB",
                 "180005X.IB","180005X2.IB","180006.IB","180006X.IB","180006X2.IB","180011.IB","180011X.IB","180011X2.IB","180012.IB","180013.IB","180013X.IB",
                 "180013X2.IB","180017.IB","180017X.IB","180017X2.IB","180019.IB","180019X.IB","180019X2.IB","180020.IB","180020X.IB","180020X2.IB","180024.IB",
                 "180024X.IB","180024X2.IB","180024X3.IB","180024X4.IB","180024X5.IB","180025.IB","180027.IB","180027X.IB","180027X2.IB","180027X3.IB",
                 "180027X4.IB","180027X5.IB","180028.IB","180028X.IB","180028X2.IB","180028X3.IB","180028X4.IB","180028X5.IB","190004.IB","190004X.IB",
                 "190004X2.IB","190004X3.IB","190004X4.IB","190004X5.IB","190006.IB","190006X.IB","190006X2.IB","190006X3.IB","190006X4.IB","190006X5.IB",
                 "190007.IB","190007X.IB","190007X2.IB","190007X3.IB","190007X4.IB","190007X5.IB","190008.IB","190008X.IB","190008X2.IB","190010.IB","190010X.IB",
                 "190010X2.IB","190010X3.IB","190010X4.IB","190010X5.IB","190013.IB","190013X.IB","190013X2.IB","190013X3.IB","190013X4.IB","190013X5.IB",
                 "190015.IB","190015X.IB","190015X2.IB","190015X3.IB","190015X4.IB","190015X5.IB","190016.IB","190016X.IB","190016X2.IB","190016X3.IB",
                 "190016X4.IB","190016X5.IB","2000001.IB","2000001X.IB","2000001X2.IB","2000001X3.IB","2000002.IB","2000002X.IB","2000003.IB","2000003X.IB",
                 "2000003X2.IB","2000003X3.IB","2000003X4.IB","2000004.IB","2000004X.IB","2000004X2.IB","2000004X3.IB","2000004X4.IB","200004.IB","200004X.IB",
                 "200004X2.IB","200004X3.IB","200004X4.IB","200004X5.IB","200005.IB","200005X.IB","200005X2.IB","200005X3.IB","200005X4.IB","200006.IB",
                 "200006X.IB","200006X2.IB","200006X3.IB","200006X4.IB","200006X5.IB","200007.IB","200007X.IB","200007X2.IB","200008.IB","200008X.IB",
                 "200008X2.IB","200008X3.IB","200008X4.IB","200012.IB","200012X.IB","200012X2.IB","200012X3.IB","200012X4.IB","200012X5.IB","200013.IB",
                 "200013X.IB","200013X2.IB","200013X3.IB","200013X4.IB","200016.IB","200016X.IB","200016X2.IB","200016X3.IB","200016X4.IB","200016X5.IB",
                 "200017.IB","200017X.IB","200017X2.IB","200017X3.IB","200017X4.IB","210002.IB","210002X.IB","210002X2.IB","210002X3.IB","210002X4.IB",
                 "210003.IB","210003X.IB","210003X2.IB","210003X3.IB","210004.IB","210004X.IB","210004X2.IB","210004X3.IB","210004X4.IB","210005.IB",
                 "210005X.IB","210005X2.IB","210005X3.IB","210005X4.IB","210005X5.IB","210007.IB","210007X.IB","210007X2.IB","210007X3.IB","210007X4.IB",
                 "210009.IB","210009X.IB","210009X2.IB","210009X3.IB","210009X4.IB","210009X5.IB","210011.IB","210011X.IB","210011X2.IB","210011X3.IB",
                 "210011X4.IB","210012.IB","210012X.IB","210012X2.IB","210012X3.IB","210012X4.IB","210013.IB","210013X.IB","210013X2.IB","210013X3.IB",
                 "210013X4.IB","210014.IB","210014X.IB","210014X2.IB","210014X3.IB","210014X4.IB","210014X5.IB","210017.IB","210017X.IB","210017X2.IB",
                 "2200001.IB","220002.IB","220002X.IB","220002X2.IB","220003.IB","220003X.IB","220003X2.IB","220004.IB","220004X.IB","220004X2.IB","220006.IB",
                 "220006X.IB","220006X2.IB","220007.IB","220007X.IB","220007X2.IB","220008.IB","220008X.IB","220008X2.IB","220008X3.IB","220008X4.IB",
                 "220008X5.IB","220010.IB","220010X.IB","220010X2.IB","220011.IB","220011X.IB","220011X2.IB","220012.IB","220012X.IB","220012X2.IB","220013.IB",
                 "220013X.IB","220013X2.IB","220015.IB","220015X.IB","220015X2.IB","220015X3.IB","220016.IB","220016X.IB","220016X2.IB","220017.IB","220017X.IB",
                 "220017X2.IB","220018.IB","220018X.IB","220018X2.IB","220019.IB","220019X.IB","220019X2.IB","220020.IB","220020X.IB","220020X2.IB","220021.IB",
                 "220021X.IB","220021X2.IB","220022.IB","220022X.IB","220022X2.IB","220024.IB","220024X.IB","220024X2.IB","220024X3.IB","220024X4.IB",
                 "220024X5.IB","220025.IB","220025X.IB","220025X2.IB","220026.IB","220026X.IB","220026X2.IB","220027.IB","220027X.IB","220027X2.IB","220028.IB",
                 "220028X.IB","220028X2.IB","230002.IB","230002X.IB","230002X2.IB","230003.IB","230003X.IB","230003X2.IB","230004.IB","230004X.IB","230004X2.IB",
                 "230005.IB","230005X.IB","230005X2.IB","230006.IB","230006X.IB","230006X2.IB","230007.IB","230007X.IB","230007X2.IB","230007X3.IB","230008.IB",
                 "230008X.IB","230008X2.IB","230009.IB","230009X.IB","230009X2.IB","230009X3.IB","230009X4.IB","230009X5.IB","230010.IB","230010X.IB",
                 "230010X2.IB","230011.IB","230011X.IB","230011X2.IB","230012.IB","230012X.IB","230012X2.IB","230013.IB","230013X.IB","230013X2.IB","230014.IB",
                 "230014X.IB","230014X2.IB","230015.IB","230015X.IB","230015X2.IB","230016.IB","230016X.IB","230016X2.IB","230017.IB","230017X.IB","230017X2.IB",
                 "230018.IB","230018X.IB","230018X2.IB","230019.IB","230019X.IB","230019X2.IB","230020.IB","230020X.IB","230020X2.IB","230021.IB","230022.IB",
                 "230022X.IB","230022X2.IB","230023.IB","230023X.IB","230023X2.IB","230023X3.IB","230023X4.IB","230023X5.IB","230024.IB","230024X.IB",
                 "230024X2.IB","230025.IB","230025X.IB","230025X2.IB","230026.IB","230026X.IB","230026X2.IB","230027.IB","230027X.IB","230027X2.IB","230028.IB",
                 "230028X.IB","230028X2.IB","239959.IB","239964.IB","239966.IB","239972.IB","239973.IB","239976.IB","239982.IB","239983.IB","240001.IB",
                 "240001X.IB","240002.IB","240002X.IB","240003.IB","240003X.IB","240004.IB","240004X.IB","240005.IB","249902.IB","249903.IB","249904.IB",
                 "249905.IB","249906.IB","249908.IB","249909.IB","249910.IB","249911.IB","249912.IB","249913.IB","249914.IB","249915.IB","249916.IB","9802.IB"]
    
    end_date = '2024-03-20'
    for i in range(0, len(code_list)):
        code = code_list[i]
        if len(code) != 9 or code[:2] < '05':
            continue
        data = w.wsd(code, "carrydate,maturitydate", "2024-03-19", "2024-03-19", "contractType=NQ1;returnType=1;bondPriceType=1;PriceAdj=CP")
        if data.ErrorCode != 0:
            continue
        
        start_date = str(data.Data[0][0])[:10]
        end_date = min(str(data.Data[1][0])[:10], end_date)
        
        data = w.wsd(code, "tbf_cvf2,tbf_spread,cleanprice,volume,ytm_b,tbf_IRR2", start_date, end_date, "contractType=NQ1;returnType=1;bondPriceType=1;PriceAdj=CP", usedf=True)
        data = data[-1]
        data.insert(0, 'trade_date', data.index)
        data.columns = ['trade_date', 'cvf', 'spread', 'clean_price', 'amount', 'ytm', 'irr']
        data.to_csv('./treasury-daily-data/{}.csv'.format(code), index=False)

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
    # w.start() # 默认命令超时时间为120秒，如需设置超时时间可以加入waitTime参数，例如waitTime=60,即设置命令超时时间为60秒  
    # w.isconnected() # 判断WindPy是否已经登录成功
    
    # export_treasury_daily_data_to_csv()
    # subscribe_treasury_market_data()
    
    write_treasury_fut_data_to_json()


if __name__ == "__main__":
    main()
