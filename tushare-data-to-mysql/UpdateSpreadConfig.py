# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-10-18
# @Last Modified by:   Yansea
# @Last Modified time: 2023-10-18

import pandas as pd
from sqlalchemy import create_engine
import json
from DatabaseTools import *

def update_spread_config():
    f = open('./productOps.json', 'r')
    content = f.read()
    ops_json = json.loads(content)
    f.close()

    engine_ts = creat_engine_with_database('futures')
    for i in range(0, len(ops_json)):
        fut_code = ops_json[i]['ProductID']
        sql = "select distinct spread_type from fut_spread_daily where fut_code = '{}' order by spread_type".format(fut_code)
        spread_type_df = read_data(engine_ts, 'fut_spread_daily', sql)
        spread_dict = {}
        for j in range(0, len(spread_type_df)):
            spread_type = spread_type_df.loc[j]['spread_type']
            sql = "select close from fut_spread_daily where fut_code = '{}' and spread_type = '{}' order by close".format(fut_code, spread_type)
            close_df = read_data(engine_ts, 'fut_spread_daily', sql)
            num = len(close_df)
            spread_dict[spread_type] = close_df.loc[max(round(num * 0.1), 1) - 1]['close']
        ops_json[i]["RecommendSpread"] = spread_dict
        print('{} 价差配置写入成功，文件更新进度：{}%'.format(fut_code, format(i / len(ops_json) * 100, '.2f')))
    
    f = open('./productOps.json', 'w')
    content = json.dumps(ops_json, indent=2)
    f.write(content)
    f.close()
    print('价差配置文件更新完毕！')

def main():
    update_spread_config()


if __name__ == "__main__":
    main()
