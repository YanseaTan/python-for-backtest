# -*- coding: utf-8 -*-
# @Author: Yansea
# @Date:   2023-09-28
# @Last Modified by:   Yansea
# @Last Modified time: 2023-10-17

from EmQuantAPI import *
import xlwings as xw
import datetime
    
# 读取收益率 excel 文件，获取上一交易日的总资金、剩余资金以及持仓数据
app = xw.App(visible = True, add_book = False)
app.display_alerts = False
app.screen_updating = False
workbook = app.books.open('./收益率.xlsx')
worksheet = workbook.sheets.active
initFund = worksheet.range("B17").value
feeRate = worksheet.range("D17").value
rng = worksheet.range("G20").expand("table")
nRows = rng.rows.count
rng = worksheet.range("G" + str(nRows + 16)).expand("table")
nColumns = rng.columns.count
preLastFund = worksheet.range("D" + str(nRows + 16)).value
remainFund = worksheet.range("E" + str(nRows + 16)).value
preCodeSet = set()
preCodeName = {}
preClosePrice = {}
prePosition = {}
if nRows > 4:
    for i in range(0, nColumns):
        if rng.columns[0 + i][3].value > 0:
            preCodeSet.add(rng.columns[0 + i][0].value)
            preCodeName[rng.columns[0 + i][0].value] = rng.columns[0 + i][1].value
            preClosePrice[rng.columns[0 + i][0].value] = rng.columns[0 + i][2].value
            prePosition[rng.columns[0 + i][0].value] = rng.columns[0 + i][3].value
workbook.close()
app.quit()

# 读取万得正股 excel 文件，获取当前最新可转债信息
app = xw.App(visible = True, add_book = False)
app.display_alerts = False
app.screen_updating = False
workbook = app.books.open('./可转债.xlsx')
worksheet = workbook.sheets.active
rng = worksheet.range("A2").expand("table")
nRows = rng.rows.count
codeSet = set()
codeName = {}
for i in range(2, nRows + 2):
    codeAddr = "A" + str(i)
    codeSet.add(str(worksheet.range(codeAddr).value))
    nameAddr = "B" + str(i)
    codeName[str(worksheet.range(codeAddr).value)] = str(worksheet.range(nameAddr).value)
workbook.close()
app.quit()

# 得到新老合约集的交集与并集
remainCode = preCodeSet & codeSet
deleteCode = preCodeSet - remainCode
addCode = codeSet - remainCode
allCode = preCodeSet | codeSet

strAllCode = ""
for code in allCode:
    strAllCode += (code + ",")

# 登录 choice 接口
loginResult = c.start()
if loginResult.ErrorCode != 0:
    print(loginResult.ErrorMsg)
    exit(1)
    
# 获取上一交易日日期
def getLastTradeDate():
   today = datetime.date.today()
   oneday = datetime.timedelta(days=1)
   threeWeekAgo = today - oneday * 21
   todayStr = today.strftime('%Y-%m-%d')
   threeWeekAgoStr = threeWeekAgo.strftime('%Y-%m-%d')
   tradeDate = c.tradedates(threeWeekAgoStr,todayStr,"period=1,order=1,market=CNSESH")
   strDate = tradeDate.Data[-2].replace('/', '-')
   return strDate

# 获取所有可转债（包含准备被剔除的）的昨日收盘价
lastTradeDate = getLastTradeDate()
choiceData = c.csd(strAllCode,"CLOSE",lastTradeDate,lastTradeDate,"type=1,period=1,adjustflag=1,curtype=1,order=1,market=CNSESH")

closePrice = {}
if(choiceData.ErrorCode != 0):
    print("request csd Error, ", choiceData.ErrorMsg)
else:
    for code in choiceData.Codes:
        for i in range(0, len(choiceData.Indicators)):
            for j in range(0, len(choiceData.Dates)):
                if choiceData.Data[code][i][j] != None:
                    closePrice[code] = choiceData.Data[code][i][j]

# 卖出将要剔除的合约持仓
for code in deleteCode:
    remainFund += closePrice[code] * prePosition[code] * (1 - feeRate)

# 计算当前的总资金
lastFund = remainFund
for code in remainCode:
    lastFund += closePrice[code] * prePosition[code]

# 得到新的合约持仓数据，并买入新的仓位（已有合约多退少补，新合约买入）
perFund = lastFund / len(codeSet)
position = {}
for code, price in closePrice.items():
    if code in codeSet:
        position[code] = int(perFund // (price * (1 + feeRate)))
        if code in remainCode:
            remainFund -= (position[code] - prePosition[code]) * price * (1 + feeRate)
        else :
            remainFund -= position[code] * price * (1 + feeRate)

# 将合约的相关信息进行排序，方便后续在 excel 中进行插入
for code in deleteCode:
    codeName[code] = preCodeName[code]
    position[code] = 0
sortedCode = sorted(position.keys())
sortedCodeName = []
sortedClosePrice = []
sortedPosition = []
for code in sortedCode:
    sortedCodeName.append(codeName[code])
    sortedClosePrice.append(closePrice[code])
    sortedPosition.append(position[code])
            
# 计算最新总资金、涨幅以及累计收益率
lastFund = remainFund
for code in codeSet:
    lastFund += closePrice[code] * position[code]

rise = (lastFund - preLastFund) / preLastFund
totalRise = (lastFund - initFund) / initFund

# 将数据保存入收益率 excel 中
app = xw.App(visible = True, add_book = False)
app.display_alerts = False
app.screen_updating = False
workbook = app.books.open('./收益率.xlsx')
worksheet = workbook.sheets.active
rng = worksheet.range("G20").expand("table")
nRows = rng.rows.count
worksheet.range("A" + str(nRows + 20)).value = lastTradeDate
worksheet.range("B" + str(nRows + 20)).value = rise
worksheet.range("C" + str(nRows + 20)).value = totalRise
worksheet.range("D" + str(nRows + 20)).value = lastFund
worksheet.range("D" + str(nRows + 20)).api.NumberFormat = "0.00"
worksheet.range("E" + str(nRows + 20)).value = remainFund
worksheet.range("E" + str(nRows + 20)).api.NumberFormat = "0.00"
worksheet.range("F" + str(nRows + 20)).value = len(codeSet)

worksheet.range("A" + str(nRows + 20) + ":A" + str(nRows + 23)).api.Merge()
worksheet.range("A" + str(nRows + 20) + ":A" + str(nRows + 23)).api.VerticalAlignment = -4108
if rise > 0:
    worksheet.range("B" + str(nRows + 20)).color = worksheet.range("H17").color
elif rise < 0:
    worksheet.range("B" + str(nRows + 20)).color = worksheet.range("G17").color
worksheet.range("B" + str(nRows + 20) + ":B" + str(nRows + 23)).api.Merge()
worksheet.range("B" + str(nRows + 20) + ":B" + str(nRows + 23)).api.VerticalAlignment = -4108
worksheet.range("C" + str(nRows + 20) + ":C" + str(nRows + 23)).api.Merge()
worksheet.range("C" + str(nRows + 20) + ":C" + str(nRows + 23)).api.VerticalAlignment = -4108
worksheet.range("D" + str(nRows + 20) + ":D" + str(nRows + 23)).api.Merge()
worksheet.range("D" + str(nRows + 20) + ":D" + str(nRows + 23)).api.VerticalAlignment = -4108
worksheet.range("E" + str(nRows + 20) + ":E" + str(nRows + 23)).api.Merge()
worksheet.range("E" + str(nRows + 20) + ":E" + str(nRows + 23)).api.VerticalAlignment = -4108
worksheet.range("F" + str(nRows + 20) + ":F" + str(nRows + 23)).api.Merge()
worksheet.range("F" + str(nRows + 20) + ":F" + str(nRows + 23)).api.VerticalAlignment = -4108

worksheet.range("G" + str(nRows + 20)).value = list(sortedCode)
worksheet.range("G" + str(nRows + 21)).value = sortedCodeName
worksheet.range("G" + str(nRows + 22)).value = sortedClosePrice
worksheet.range("G" + str(nRows + 23)).value = sortedPosition

# 格式化单元格样式
rng = worksheet.range("G" + str(nRows + 20)).expand("table")
nColumns = rng.columns.count
for i in range(0, nColumns):
    code = rng.columns[0 + i][0].value
    closePrice = rng.columns[0 + i][2].value
    rng.columns[0 + i][2].api.NumberFormat = "0.00"
    
    if code in addCode:
        rng.columns[0 + i][0].color = worksheet.range("E17").color
        rng.columns[0 + i][1].color = worksheet.range("E17").color
    elif code in deleteCode:
        rng.columns[0 + i][0].color = worksheet.range("F17").color
        rng.columns[0 + i][1].color = worksheet.range("F17").color
    
    if code in preCodeSet:
        if closePrice > preClosePrice[code]:
            rng.columns[0 + i][2].color = worksheet.range("H17").color
        elif closePrice < preClosePrice[code]:
            rng.columns[0 + i][2].color = worksheet.range("G17").color

workbook.save()
workbook.close()
app.quit()

print('脚本运行结束，收益率计算完成！')