import sys

import requests
import json
import pandas as pd
import numpy as np
import h5py
import os
import time
# from pandas.io.json import json_normalize
# from pandas import json_normalize
from collections import OrderedDict
from datetime import datetime, timedelta
os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"

init_start_date = "2000-01-01"  # 默认起始时间
_end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
algo_defalut = "b"
adjust_days_overwrite = 20000
baseurl = "https://fengyun-pta.thanf.com/ta/"
# baseurl = "http://localhost:64931/TA/"
token = "neP3DYmN6BMoejlG"



class FY_API():
    def __init__(self, fileUrl=''):
        self.username = token
        self.pwd = 'pwd'
        self.fileUrl = fileUrl

    def Get_Data(self, symbol, start_date='', end_date='', algo=algo_defalut):
        if start_date == '':
            start_date = init_start_date
        if end_date == '':
            end_date = _end_date

        params = {'username': self.username, 'pwd': self.pwd, 'symbol': symbol, 'begin': start_date, 'end': end_date,
                  'algo': algo}
        r = requests.post(baseurl + 'fyapi.aspx', params)
        r.encoding = 'utf-8'
        content = r.text
        if content == '':
            return pd.DataFrame(columns=['指标名称', '指标时间', '指标值'])
        try:
            content = json.loads(r.text, object_pairs_hook=OrderedDict)
            if "errcode" in content:
                return content
            df_rtn = pd.DataFrame(content)
            df_rtn.drop_duplicates(subset=['指标名称', '指标时间'], keep='first', inplace=True)
            return df_rtn
        except Exception as e:
            if content == '':
                return e
            else:
                return content

    def _Get_Data_4_H5(self, symbol, start_date, end_date='', algo=algo_defalut):
        data = self.Get_Data(symbol, start_date, end_date, algo)
        try:
            if data.shape[0] > 0:
                data['指标时间'] = data['指标时间'].apply(lambda x: x.encode())
                data['指标名称'] = data['指标名称'].apply(lambda x: x.encode())
                data['指标值'] = data['指标值'].apply(lambda x: self.__ConvertString2Float(x))
                data = np.array(data)
                return data
        except Exception as e:
            print(e)
            return None
        return None

    def __update_symbol_data(self, symbol, adjust_days, algo, f, log_index):
        _adjust_days = 0 - int(adjust_days)
        local_key = "/tfqh/" + symbol
        end_date = _end_date
        # 获取组
        tfqh_group = f.get("tfqh")
        if local_key in tfqh_group.keys():
            # 根据指标名称获取本地该指标最新日期
            local_time = f[local_key][-1, 1]
            local_time = local_time.decode()
            local_time = datetime.strptime(local_time, '%Y-%m-%d %H:%M:%S')
            update_date = local_time + timedelta(days=_adjust_days)
            local_time_update = update_date.strftime('%Y-%m-%d %H:%M:%S')
            update_date = update_date.strftime('%Y-%m-%d')
            # 根据本地最新日期和调整时限，从服务器获取数据
            cur_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(str(log_index) + " 开始 " + cur_time + ":---" + symbol + "---增量下载数据,从"
                  + update_date + " 到 " + end_date)
            down_data = self._Get_Data_4_H5(symbol, update_date, end_date, algo)
            if down_data is None:
                print(str(log_index) + " 结束 " +
                      cur_time + ":---" + symbol + "---增量下载完成,从" + update_date + " 到 "
                      + end_date + " 共下载0个数据")
                return
            # 覆盖本地数据
            data_start_date = down_data[0, 1].decode()[0:10]
            data_end_date = down_data[-1, 1].decode()[0:10]
            down_count = down_data.shape[0]
            # 1:2 第二列是日期
            re = np.where(f[local_key][..., 1:2] >= local_time_update.encode())
            # rc = len(re)
            rc = len(re[0])
            if rc > 0:
                local_row_start = re[0][0]
                len_old = local_row_start
                len_new = len_old + down_data.shape[0]
                shape_list = list(down_data.shape)
                shape_list[0] = len_new
                f[local_key].resize(tuple(shape_list))
                down_data = down_data.tolist()
                f[local_key][len_old:len_new] = down_data
            else:
                shape_list = list(down_data.shape)
                f[local_key].resize(tuple(shape_list))
                down_data = down_data.tolist()
                f[local_key][...] = down_data
            print(str(log_index) + " 结束 " + cur_time + ":---" + symbol + "---增量下载完成,从" + data_start_date
                  + " 到 "
                  + data_end_date + " 共下载" + str(down_count) + "个数据")
        else:
            # 从服务器获取数据
            cur_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(str(log_index) + " 开始 " + cur_time + ":---" + symbol + "---初次下载数据,从"
                  + init_start_date + " 到 " + end_date)
            down_data = self._Get_Data_4_H5(symbol, init_start_date, end_date, algo)
            cur_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            if down_data is None:
                print(str(log_index) + " 结束 " +
                      cur_time + ":---" + symbol + "---初次下载完成,从"
                      + init_start_date + " 到 " + end_date + " 共下载0个数据")
            else:
                down_count = down_data.shape[0]
                # print(down_data)
                data_start_date = down_data[0, 1].decode()[0:10]
                data_end_date = down_data[-1, 1].decode()[0:10]
                down_data = down_data.tolist()
                tfqh_group.create_dataset(symbol, data=down_data, maxshape=(None, 3), chunks=True)
                print(str(log_index) + " 结束 " + cur_time + ":---" + symbol + "---初次下载完成,从" +
                      data_start_date + " 到 " + data_end_date + " 共下载" + str(down_count) + "个数据")

    def __get_local_symbol_start_data(self, symbol, adjust_days, f):
        _adjust_days = 0 - int(adjust_days)
        local_key = "/tfqh/" + symbol
        # 获取组
        tfqh_group = f.get("tfqh")
        if local_key in tfqh_group.keys():
            # 根据指标名称获取本地该指标最新日期
            local_time = f[local_key][-1, 1]
            local_time = local_time[0]
            local_time = local_time.decode()
            if local_time == '':
                dataset = f[local_key][()]
                if dataset.shape[0] > 0:
                    rows = dataset.shape[0]
                    for i in range(rows - 1, -1, -1):
                        local_time = f[local_key][i, 1]
                        local_time = local_time[0]
                        local_time = local_time.decode()
                        if local_time != '':
                            break
            if local_time == '':
                return init_start_date
            local_time = datetime.strptime(local_time, '%Y-%m-%d %H:%M:%S')
            update_date = local_time + timedelta(days=_adjust_days)
            local_time_update = update_date.strftime('%Y-%m-%d %H:%M:%S')
            update_date = update_date.strftime('%Y-%m-%d')
            return update_date
        return init_start_date

    def __ConvertString2Float(self, strValue):
        try:
            return float(strValue)
        except:
            return float('nan')

    # 返回服务器中所有指标及该指标对应的最新日期、调整时限
    def Get_Symbols_Adjust(self):
        params = {'post_name': 'GetIndexAdjust', 'username': self.username}
        try:
            r = requests.post(baseurl + 'fyapi.aspx', params)
            r.encoding = 'utf-8'
            content = r.text
            if content == '':
                print('Get_Symbols_Adjust:服务器返回数据为空，可能是网络波动，可稍等1分钟后重试，若还是不行，请联系天风技术人员')
                sys.exit()
            result = pd.DataFrame(json.loads(content, object_pairs_hook=OrderedDict))
            return result.set_index("指标名称").T.to_dict("list")
        except Exception as e:
            print('Get_Symbols_Adjust 报错信息：', e, '可能是网络波动，可稍等1分钟后重试，若还是不行，请联系天风技术人员')
            sys.exit()

    def Get_Symbols_By_Vari(self, vari):
        params = {'post_name': 'GetIndexByVari', 'vari': vari, 'username': self.username}
        r = requests.post(baseurl + 'fyapi.aspx', params)
        r.encoding = 'utf-8'
        content = r.text
        if content == '':
            return content
        try:
            result = pd.DataFrame(json.loads(content, object_pairs_hook=OrderedDict))
            return result["指标名称"].tolist()
        except Exception as e:
            if content == '':
                return str(e)
            else:
                return content

    # 从本地H5文件读取数据
    # symbols:需要的指标名称,symbols=["指标1", "指标2"]
    # start_date：起始时间yyyy-mm-dd，不填默认从2010-01-01开始
    # end_date：终止时间yyyy-mm-dd，不填默认终止为当前日期
    # return：返回以时间为索引（yyyy-mm-dd hh:mm:ss）， 指标名称，指标值为列的dataframe
    def Get_Local_Data(self, symbols, start_date='', end_date=''):
        if start_date == '':
            start_date = init_start_date
        if end_date == '':
            end_date = _end_date
        if self.fileUrl == '':
            f = h5py.File('tfyh_data.h5', 'a')
        else:
            f = h5py.File(self.fileUrl, 'a')
        tfqh_group = f.get("tfqh")
        if tfqh_group is None:
            return None
        result = []
        symbols = list(set(symbols))
        symbols_h5_exist = []
        f_keys = f.keys()
        print('开始遍历h5获取数据---', time.time())
        for symbol in symbols:
            local_key = "/tfqh/" + symbol
            if local_key in f_keys:
                symbols_h5_exist.append(symbol)
                re = np.where(
                    (f[local_key][..., 1:2] >= start_date.encode()) & (f[local_key][..., 1:2] <= end_date.encode()))
                rc = len(re[0])
                if rc > 0:
                    local_row_start = re[0][0]
                    # local_row_end = re[0][len(re[0]) - 1]
                    local_row_end = re[0][-1]
                    result.append(f[local_key][local_row_start:local_row_end + 1, ...])
        print('获取结束---', time.time())
        if not len(result):
            return None
        result = pd.DataFrame(np.concatenate(result), columns=["指标名称", "指标时间", "指标值"])
        result['指标时间'] = result['指标时间'].str.decode(encoding='UTF-8')
        result['指标名称'] = result['指标名称'].str.decode(encoding='UTF-8')
        result['指标值'] = pd.to_numeric(result['指标值'], errors='coerce')
        result.drop_duplicates(subset=['指标名称', '指标时间'], keep='first', inplace=True)
        result = result.pivot(index="指标时间", columns="指标名称", values="指标值")  # .reset_index()
        # 指标h5文件存在，结果集就返回该列，筛选为空的指标，填充nan
        result = result.reindex(symbols_h5_exist, axis=1)
        result.columns.name = None
        result.index = pd.DatetimeIndex(result.index)
        return result

    # 从服务器下载数据并保存在本地（不带日志输出，性能稍好）
    # symbols:想要下载的指标名称,symbols=["指标1", "指标2"]，不填则下载所有指标
    # algos:指标标准化算法（目前只支持b算法）,algos=["b", "b"]，需要与symbols一一对应，不填则默认b
    # 如果本地存有该指标数据，则以该指标在本地最后的日期减去调整时限作为本次获取数据的时间起点，做增量更新
    # 如果本地没有该指标数据，则以  init_start_date 为起点下载数据
    def Init_local_Data(self, symbols='', algos=''):
        post_name = 'AddInitLocal'
        post_symbols = ""
        post_start_dates = ""
        post_algos = ""
        # 从服务器获取所有指标及调整时限
        all_symbols_adjust = self.Get_Symbols_Adjust()
        # 打开文件，不存在则创建
        if self.fileUrl == '':
            f = h5py.File('tfyh_data.h5')
        else:
            f = h5py.File(self.fileUrl)
        # 获取组
        tfqh_group = f.get("tfqh")
        if tfqh_group is None:
            # 不存在tfqh组则创建
            tfqh_group = f.create_group("tfqh")
        keysForLoop = symbols
        algosForLoop = algos
        if symbols == '':
            keysForLoop = all_symbols_adjust
            for key in all_symbols_adjust:
                # symbol = key  # row['指标名称']
                symbol = key
                algo = algo_defalut
                adjust_days = all_symbols_adjust[symbol][0]  # row['调整时限']
                start_date = self.__get_local_symbol_start_data(symbol, adjust_days, f)
                post_symbols = post_symbols + "," + symbol
                post_start_dates = post_start_dates + "," + start_date
                post_algos = post_algos + "," + algo
        else:
            if isinstance(symbols, str):
                symbols = [symbols]
            if algos == "":
                algosForLoop = len(symbols) * [algo_defalut]
            else:
                if isinstance(algos, str):
                    algosForLoop = [algos]
                elif isinstance(algos, list):
                    algosForLoop = algos
            for i in range(0, len(symbols)):
                try:
                    symbol = symbols[i]
                    algo = algosForLoop[i]
                    adjust_days = all_symbols_adjust[symbol][0]
                    start_date = self.__get_local_symbol_start_data(symbol, adjust_days, f)
                    post_symbols = post_symbols + "," + symbol
                    post_start_dates = post_start_dates + "," + start_date
                    post_algos = post_algos + "," + algo
                except Exception as e:
                    print(e)
        f.close()
        if len(post_symbols) > 0 and len(post_start_dates) > 0:
            post_symbols = post_symbols[1:]
            post_start_dates = post_start_dates[1:]
            post_algos = post_algos[1:]
        params = {'username': self.username, 'pwd': self.pwd, 'post_name': post_name, 'symbol': post_symbols,
                  'begin': post_start_dates, 'algo': post_algos}
        r = requests.post(baseurl + 'fyapi.aspx', params)
        r.encoding = 'utf-8'
        content = r.text
        if content == '':
            print("no data")
        try:
            down_data = pd.DataFrame(json.loads(content, object_pairs_hook=OrderedDict))
            print("down data.counts = " + str(down_data.shape[0]))
            if down_data.shape[0] == 0:
                return
            down_data['指标时间'] = down_data['指标时间'].apply(lambda x: x.encode())
            down_data['指标名称'] = down_data['指标名称'].apply(lambda x: x.encode())
            down_data['指标值'] = down_data['指标值'].apply(lambda x: self.__ConvertString2Float(x))
            if self.fileUrl == '':
                f = h5py.File('tfyh_data.h5', 'r+')
            else:
                f = h5py.File(self.fileUrl, 'r+')
            # 获取组
            tfqh_group = f.get("tfqh")
            if tfqh_group is None:
                # 不存在tfqh组则创建
                tfqh_group = f.create_group("tfqh")
            for key in keysForLoop:
                local_key = "/tfqh/" + key
                keyForQuery = key.encode()
                temp_data = down_data.loc[down_data["指标名称"] == keyForQuery]
                if temp_data.shape[0] > 0:
                    if local_key in f.keys():
                        start_time_update = temp_data.iloc[0, 1]
                        re = np.where(f[local_key][..., 1:2] >= start_time_update)
                        rc = len(re)
                        if rc > 0:
                            local_row_start = re[0][0]
                            len_old = local_row_start
                            len_new = len_old + temp_data.shape[0]
                            shape_list = list(temp_data.shape)
                            shape_list[0] = len_new
                            f[local_key].resize(tuple(shape_list))
                            temp_data = np.array(temp_data)
                            temp_data = temp_data.tolist()
                            f[local_key][len_old:len_new] = temp_data
                        else:
                            shape_list = list(temp_data.shape)
                            f[local_key].resize(tuple(shape_list))
                            temp_data = np.array(temp_data)
                            temp_data = temp_data.tolist()
                            f[local_key][...] = temp_data
                    else:
                        temp_data = np.array(temp_data)
                        temp_data = temp_data.tolist()
                        tfqh_group.create_dataset(key, data=temp_data, maxshape=(None, 3), chunks=True)
        except Exception as e:
            print(e)
            if content == '':
                return e
            else:
                return content

    # 从服务器下载数据并保存在本地(带日志输出)
    # symbols:想要下载的指标名称,symbols=["指标1", "指标2"]，不填则下载所有指标
    # algos:指标标准化算法（目前只支持b算法）,algos=["b", "b"]，需要与symbols一一对应，不填则默认b
    # 如果本地存有该指标数据，则以该指标在本地最后的日期减去调整时限作为本次获取数据的时间起点，做增量更新
    # 如果本地没有该指标数据，则以 init_start_date 为起点下载数据
    def Init_Local_Data_WithLog(self, symbols='', algos='', overwrite=False):
        post_name = 'AddInitLocal'
        post_symbols = ""
        post_start_dates = ""
        post_algos = ""
        # 从服务器获取所有指标及调整时限
        all_symbols_adjust = self.Get_Symbols_Adjust()
        # 打开文件，不存在则创建
        if self.fileUrl == '':
            f = h5py.File('tfyh_data.h5', 'a')
        else:
            f = h5py.File(self.fileUrl, 'a')
        # 获取组
        tfqh_group = f.get("tfqh")
        if tfqh_group is None:
            # 不存在tfqh组则创建
            tfqh_group = f.create_group("tfqh")
        print_log_index = 0
        if symbols == '':
            for key in all_symbols_adjust:
                symbol = key  # row['指标名称']
                algo = algo_defalut
                adjust_days = all_symbols_adjust.get(symbol, [5])[0]
                print_log_index = print_log_index + 1
                if overwrite:
                    adjust_days = adjust_days_overwrite
                self.__update_symbol_data(symbol, adjust_days, algo, f, print_log_index)
        else:
            if isinstance(symbols, str):
                symbols = symbols.split(',')
            if algos == "":
                algosForLoop = len(symbols) * [algo_defalut]
            else:
                if isinstance(algos, str):
                    algosForLoop = [algos]
                elif isinstance(algos, list):
                    algosForLoop = algos
            try:
                for i in range(0, len(symbols)):
                    symbol = symbols[i]
                    algo = algosForLoop[i]
                    adjust_days = all_symbols_adjust.get(symbol, [5])[0]
                    print_log_index = print_log_index + 1
                    if overwrite:
                        adjust_days = adjust_days_overwrite
                    self.__update_symbol_data(symbol, adjust_days, algo, f, print_log_index)
            except Exception as e:
                print(e)
        f.close()

    def GetIndexInfo(self, vari='all'):
        params = {"vari": vari, "username": self.username}
        r = requests.post(baseurl + 'datainfo.ashx', params)
        content = json.loads(r.text, object_pairs_hook=OrderedDict)
        if "errcode" in content:
            return content
        result = pd.DataFrame(content)
        result['index_name'] = result['index_name'].str.strip()
        return result

    def GetTop10Index(self):
        params = {"username": self.username}
        r = requests.post(baseurl + 'datainfo_top10.ashx', params)
        content = json.loads(r.text, object_pairs_hook=OrderedDict)
        if "errcode" in content:
            return content
        result = pd.DataFrame(content)
        return result

    def GetSettlementInfo(self, exchange, start_date='', end_date='', vari_code='', contract_code=''):
        if start_date == '':
            start_date = init_start_date
        if end_date == '':
            end_date = _end_date
        params = {"exchange": exchange,
                  "begin": start_date,
                  "end": end_date,
                  "vari_code": vari_code,
                  "contract_code": contract_code,
                  "username": self.username
                  }
        r = requests.post(baseurl + 'settlement_info.ashx', params)
        content = json.loads(r.text, object_pairs_hook=OrderedDict)
        if "errcode" in content:
            return content
        result = pd.DataFrame(content)
        if len(result):
            result['trade_date'] = pd.to_datetime(result['trade_date'])
            result['trade_date'] = result['trade_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        return result

    def GetFutBasicInfo(self, exchange='', vari_code='', contract_code=''):
        params = {"exchange": exchange,
                  "vari_code": vari_code,
                  "contract_code": contract_code,
                  "username": self.username
                  }
        r = requests.post(baseurl + 'fut_basic.ashx', params)
        content = json.loads(r.text, object_pairs_hook=OrderedDict)
        if "errcode" in content:
            return content
        result = pd.DataFrame(content)
        return result

    def GetSymbolHisRecord(self, symbols, update_time_begin='1900-01-01'
                           , update_time_end=_end_date, index_time_begin='1900-01-01', index_time_end=_end_date):
        df_result = []
        if isinstance(symbols,list):
            for i in range(0, len(symbols)):
                symbol = symbols[i]
                params = {"symbol": symbol,
                          "username": self.username,
                          "update_time_begin": update_time_begin,
                          "update_time_end": update_time_end,
                          "index_time_begin": index_time_begin,
                          "index_time_end": index_time_end,
                          }
                r = requests.post(baseurl + 'symbol_his_record.ashx', params)
                content = json.loads(r.text, object_pairs_hook=OrderedDict)
                if "errcode" in content:
                    return content
                if len(content):
                    df_result.append(pd.DataFrame(content))
            if len(df_result):
                return pd.concat(df_result)
        return df_result

    def GetIndexInfoChangeRecord(self, begin_date='2000-01-01', end_date='2099-01-01'):
        params = {"begin": begin_date,
                  "end": end_date,
                  "username": self.username
                  }
        r = requests.post(baseurl + 'index_change.ashx', params)
        content = json.loads(r.text, object_pairs_hook=OrderedDict)
        if "errcode" in content:
            return content
        result = pd.DataFrame(content)
        return result

    def GetFutHolding(self, begin_date, end_date=datetime.now().strftime('%Y%m%d'), symbol=''):
        params = {"begin": begin_date,
                  "end": end_date,
                  "symbol": symbol,
                  "username": self.username
                  }
        r = requests.post(baseurl + 'fut_holding.ashx', params)
        content = json.loads(r.text, object_pairs_hook=OrderedDict)
        if "errcode" in content:
            return content
        result = pd.DataFrame(content)
        return result

    def GetCftcReports(self, report_date_begin=datetime.now() + timedelta(days=-10), report_date_end=_end_date):
        params = {"begin": report_date_begin,
                  "end": report_date_end,
                  "username": self.username,
                  }
        r = requests.post(baseurl + 'cftc_DisaggregatedFuturesOnlyReports.ashx', params)
        content = json.loads(r.text, object_pairs_hook=OrderedDict)
        if "errcode" in content:
            return content
        result = pd.DataFrame(content)
        return result

    def GetStrategyInfo(self, update_date_begin=datetime.now() + timedelta(days=-10), update_date_end=_end_date):
        params = {"begin": update_date_begin,
                  "end": update_date_end,
                  "username": self.username,
                  }
        r = requests.post(baseurl + 'strategy_info.ashx', params)
        content = json.loads(r.text, object_pairs_hook=OrderedDict)
        if "errcode" in content:
            return content
        result = pd.DataFrame(content)
        return result

    def GetWeatherCurrent(self, area, begin_date, end_date=_end_date, data_type='', area_from=''):
        params = {"begin": begin_date,
                  "end": end_date,
                  "area": area,
                  "data_type": data_type,
                  "area_from": area_from,
                  "username": self.username,
                  }
        r = requests.post(baseurl + 'weather_current.ashx', params)
        content = json.loads(r.text, object_pairs_hook=OrderedDict)
        if "errcode" in content:
            return content
        result = pd.DataFrame(content)
        return result

    def GetWeatherFuture(self, area, begin_date, end_date=_end_date, data_type='', area_from=''):
        params = {"begin": begin_date,
                  "end": end_date,
                  "area": area,
                  "data_type": data_type,
                  "area_from": area_from,
                  "username": self.username,
                  }
        r = requests.post(baseurl + 'weather_future15.ashx', params)
        content = json.loads(r.text, object_pairs_hook=OrderedDict)
        if "errcode" in content:
            return content
        result = pd.DataFrame(content)
        return result

    def GetBalanceData(self, instrument_name, balance_item='', begin_publish_date=''):
        params = {"instrument_name": instrument_name,
                  "balance_item": balance_item,
                  "begin_publish_date": begin_publish_date,
                  "username": self.username,
                  }
        r = requests.post(baseurl + 'balance_week.ashx', params)
        content = json.loads(r.text, object_pairs_hook=OrderedDict)
        if "errcode" in content:
            return content
        result = pd.DataFrame(content)
        return result

    def GetFutCloseData(self, symbol, begin_date='', end_date=''):
        params = {"symbol": symbol,
                  "begin_date": begin_date,
                  "end_date": end_date,
                  "username": self.username,
                  }
        r = requests.post(baseurl + 'fut_close.ashx', params)
        content = json.loads(r.text, object_pairs_hook=OrderedDict)
        if "errcode" in content:
            return content
        result = pd.DataFrame(content)
        return result
