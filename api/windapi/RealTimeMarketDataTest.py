# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2024-03-21
# @Last Modified by:   Yansea
# @Last Modified time: 2024-03-21

from WindPy import w

def myCallback(indata: w.WindData):
    if indata.ErrorCode!=0:
        print('error code:'+str(indata.ErrorCode)+'\n')
        return
    
    print(indata)
    
start_ret = w.start()

if start_ret.ErrorCode != 0:
    print("Start failed")
    print("Error Code:", start_ret.ErrorCode)
    print("Error Message:", start_ret.Data[0])
else:
    # Subscribe market quotation data
    wsq_ret = w.wsq("230026.IB", "rt_last_cp,rt_last_ytm,rt_delivery_spd,rt_irr,rt_basis,rt_bid_price1ytm,rt_ask_price1ytm,rt_vol,rt_amt", func=myCallback)

    if wsq_ret.ErrorCode != 0:
        print("Error Code:", wsq_ret.ErrorCode)

    ext = ''
    while ext != 'q':
      ext = input('Enter "q" to exit')

    w.cancelRequest(0)