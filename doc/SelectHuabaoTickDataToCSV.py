# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-03-27
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-28

import pandas as pd
import os

def read_csv():
    df = pd.DataFrame()
    reader = pd.read_csv("./doc/202403/Level1HisData20240306.tar/Level1MD20240306.csv", chunksize=5000)
    for chunk in reader:
        chunk = chunk[(chunk.InstrumentID == 123167) | (chunk.InstrumentID == 113672) | (chunk.InstrumentID == 127080) |
                      (chunk.InstrumentID == 123210) | (chunk.InstrumentID == 127101) | (chunk.InstrumentID == 127077) |
                      (chunk.InstrumentID == 110045) | (chunk.InstrumentID == 110088) | (chunk.InstrumentID == 113044) |
                      (chunk.InstrumentID == 300975) | (chunk.InstrumentID == 603327) | (chunk.InstrumentID == 3004) |
                      (chunk.InstrumentID == 300454) | (chunk.InstrumentID == 1283) | (chunk.InstrumentID == 2645) |
                      (chunk.InstrumentID == 600398) | (chunk.InstrumentID == 600985) | (chunk.InstrumentID == 601006)].copy()
        df = pd.concat([df, chunk])
        print(df.iloc[-1]['UpdateTime'])
    df.sort_values(by='LocalTime', ascending=True, inplace=True)
    df.to_csv('select-20240306.csv', index=False)
    print(df)
    
def merge_csv():
    df = pd.DataFrame()
    files = os.listdir("./doc/tick-data/select-stock")
    for i in range(0, len(files)):
        file = files[i]
        reader = pd.read_csv("./doc/tick-data/select-stock/{}".format(file))
        df = pd.concat([df, reader])
    df.sort_values(by='LocalTime', ascending=True, inplace=True)
    df.to_csv('./doc/tick-data/select-stock/SelectedLevel1MD20240301-20240308.csv', index=False)
    print(df)

def main():
    # read_csv()
    merge_csv()


if __name__ == "__main__":
    main()
