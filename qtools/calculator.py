#!/usr/bin/env python
# coding=utf-8
"""
@Description: Do not edit
@Date: 2021-06-18 13:36:11
@LastEditors: wanghaijie01
@LastEditTime: 2021-09-30 18:10:43
"""

import math
import os
import sys

import numpy_financial as npf

sys.path.append(os.getcwd())
from util import rr


def IRR(start_amount, year, frequently, cash_flow, end_amount):
    """ irr计算
    """    
    n = year
    if frequently == "m":
        n *= 12
    elif frequently == "w":
        n = year * 365 / 7
    cash_list = [cash_flow for x in range(int(n))]
    cash_list[0] += start_amount
    cash_list.append(-end_amount)
    irr = npf.irr(cash_list)
    if frequently == "m":
        irr = math.pow(1+irr, 12) - 1
    elif frequently == "w":
        irr = math.pow(1+irr, float(365)/7) - 1
    return irr


def cf(start_amount, year, irr, frequently, end_amount):
    """现金流计算
    """    
    start_amount_fv = start_amount*math.pow(float(1+irr), year)
    n = year
    if frequently == 'm':
        n = n * 12
        irr = math.pow(1+irr, 1/12) - 1
    elif frequently == 'w':
        n = n * 365 / 7
        irr = math.pow(1+irr, 7/365) - 1
    
    cash_flow = npf.pmt(irr, n, 0, end_amount-start_amount_fv, when="begin")
    return -cash_flow


def sa(year, irr, frequently, cash_flow, end_amount):
    """初始金额
    """    
    start_amount_fv = end_amount - nfv(0, year, irr, frequently, cash_flow)
    start_amount = start_amount_fv / math.pow(float(1+irr), year)
    return start_amount


def nper(start_amount, irr, frequently, cash_flow, end_amount):
    """投资年限
    """    
    if frequently == "m":
        irr = math.pow(1+irr, 1/12) - 1
    elif frequently == "w":
        irr = math.pow(1+irr, 7/365) - 1
    year = npf.nper(irr, -cash_flow, -start_amount, end_amount, when="begin")
    if frequently == "m":
        year = year / 12
    elif frequently == "w":
        year = year * 7 / 365
    return year


def nfv(start_amount, year, irr, frequently, cash_flow):
    """期末净值
    """    
    start_amount_fv = start_amount*math.pow(float(1+irr), year)
    n = year
    if frequently == 'm':
        n = n * 12
        irr = math.pow(1+irr, 1/12) - 1
    elif frequently == 'w':
        n = n * 365 / 7
        irr = math.pow(1+irr, 7/365) - 1
    if irr > 0:
        cash_flow_fv = cash_flow * (math.pow((1+irr), n+1) - (1+irr))/irr
    else:
        cash_flow_fv = cash_flow * n
    return round(cash_flow_fv + start_amount_fv, 2)


def generate_investment_data(start_amount, year, irr, frequently, cash_flow, end_amount):
    n = int(year)
    ori_irr = irr
    if frequently == 'm':
        n = n * 12
        irr = math.pow(1+irr, 1/12) - 1
    elif frequently == 'w':
        n = int(n * 365 / 7)
        irr = math.pow(1+irr, 7/365) - 1
    total_contribute = start_amount + cash_flow * n
    x = [i for i in range(1, n+1)]
    y_contribute = [round(start_amount + i*cash_flow, 2) for i in range(1, n+1)]
    if irr > 0:
        f = lambda i: round(start_amount*math.pow(1+irr, i) + cash_flow * (math.pow((1+irr), i+1) - (1+irr))/irr, 2)
    else:
        f = lambda i: round(start_amount*math.pow(1+irr, i) + cash_flow * i, 2)
    y_total = [f(i) for i in range(1, n+1)]
    cordinate = {
        "x": x,
        "y_contribute": y_contribute,
        "y_total": y_total
    }
    summary = {
        "start_amount": round(start_amount, 2),
        "total_contribute": round(total_contribute, 2),
        "end_amount": round(end_amount, 2),
        "interest": round(end_amount-total_contribute, 2),
        "cycle": n,
        "irr": round(ori_irr, 4),
        "cash_flow": round(cash_flow, 2),
        "year": round(year, 2)
    }
    return summary, cordinate

def get_cash_flow(invest_note):
    """根据投资记录生成现金流明细
    args:
        invest_note 表示资金进出记录，格式为[{
            "date": d,
            "quantity": cash
        }...]，cash为负表示买入，为正表示卖出
    return:
        cash_flow: [(date, cash)...], cash为负表示买入，为正表示卖出
    """
    cash_flow = [(t["date"], t["quantity"]) for t in invest_note]
    return cash_flow


def compute_rr(invest_note):
    """根据投资记录 invest_note 和投资结果 invest_res，
    计算年化收益率 irr 和实际收益率 roa
    args:
        invest_note 表示资金进出记录，格式为[(date, point, cash)...]，cash为负表示买入，为正表示卖出
        invest_res 表示期末情况，格式为(date, point, cash)，此处cash表示资产总额
    return:
        irr: 年化收益率
        row: 总收益率
    """
    cash_flow = get_cash_flow(invest_note)
    irr = rr.xirr(cash_flow)
    roa = rr.roa(cash_flow)
    return round(irr, 4), round(roa, 4)


def summary(invest_note, invest_res):
    """根据投资记录 invest_note 和投资结果 invest_res，
    计算相关指标，并打印摘要
    args:
        invest_note 表示资金进出记录，格式为[(date, point, cash)...]，cash为负表示买入，为正表示卖出
        invest_res 表示期末情况，格式为(date, point, cash)，此处cash表示资产总额
    """
    invest_sum = sum([x[2] for x in invest_note])
    irr, real_rr = compute_rr(invest_note, invest_res)
    print("投资结果如下:")
    print("投资期数:", len(invest_note))
    print("投资总额:", invest_sum)
    print("期末净资产:", invest_res[2])
    print("实际收益率:", real_rr)
    print("年化收益率:", irr)


if __name__ == "__main__":
   print(nfv(1000, 10, 0.12, 'w', 1000))
