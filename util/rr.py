#!/usr/bin/env python
# coding=utf-8
"""
@Description: Do not edit
@Date: 2021-06-19 16:18:31
@LastEditors: wanghaijie01
@LastEditTime: 2021-06-28 15:21:29
"""

import datetime


def xirr(cash_flow):
    """根据现金流计算内部收益率
    原理：现金流按irr折算成净现值，和为0
    公式：sum(Pt/(1+irr)^((dt-d0)/365)) = 0；t表示发生现金流的时间点，Pt表示现金流，dt表示日期
    高次方程没有精确解，以下仅仅是一个近似解（可用excel的xirr公式验证）
    args:
        cash_flow: list, element格式：(date, cash)，示例：(datetime.date(2021, 1, 2), -100)
            cash 为负表示买入，为正表示卖出
    """    
    years = [(ta[0] - cash_flow[0][0]).days / 365. for ta in cash_flow]
    residual = 1.0
    step = 0.04
    guess = 0.01
    epsilon = 0.000001
    limit = 100000
    while abs(residual) > epsilon and limit > 0:
        limit -= 1
        residual = 0.0
        for i, trans in enumerate(cash_flow):
            residual += trans[1] / pow(guess, years[i])
        if abs(residual) > epsilon:
            if residual > 0:
                guess += step
            else:
                guess -= step
                step /= 2.0
    return guess - 1


def roa(cash_flow):
    """roa: rate of asset
    总的资产收益率
    args:
        cash_flow: list, element格式：(date, cash)，示例：(datetime.date(2021, 1, 2), -100)
            cash 为负表示买入，为正表示卖出
            cash_flow 最后一条数据为
    """    
    profit = sum([item[1] for item in cash_flow])
    fv = cash_flow[-1][1]
    return profit/float(fv - profit)

if __name__ == "__main__":
    cash_flow = [(datetime.date(2021, 1, 2), -100.0), (datetime.date(2021, 2, 5), -400), (datetime.date(2021, 3, 6), -100), (datetime.date(2021, 3, 29), -300),  (datetime.date(2021, 4, 30), 950)]
    print(xirr(cash_flow))
    print(roa(cash_flow))
