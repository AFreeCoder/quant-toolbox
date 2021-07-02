#!/usr/bin/env python
# coding=utf-8
"""
@Description: Do not edit
@Date: 2021-06-18 13:36:11
@LastEditors: wanghaijie01
@LastEditTime: 2021-07-02 23:25:20
"""

import os
import sys

from util import rr

sys.path.append(os.getcwd())


def get_cash_flow(invest_note, invest_res):
    """根据投资记录生成现金流明细
    args:
        invest_note 表示资金进出记录，格式为[(date, point, cash)...]，cash为负表示买入，为正表示卖出
        invest_res 表示期末情况，格式为(date, point, cash)，此处cash表示资产总额
    return:
        cash_flow: [(date, cash)...], cash为负表示买入，为正表示卖出
    """
    cash_flow = [(t[0].date(), t[2]) for t in invest_note]
    cash_flow.append((invest_res[0].date(), invest_res[2]))
    return cash_flow


def compute_rr(invest_note, invest_res):
    """根据投资记录 invest_note 和投资结果 invest_res，
    计算年化收益率 irr 和实际收益率 roa
    args:
        invest_note 表示资金进出记录，格式为[(date, point, cash)...]，cash为负表示买入，为正表示卖出
        invest_res 表示期末情况，格式为(date, point, cash)，此处cash表示资产总额
    return:
        irr: 年化收益率
        row: 总收益率
    """
    cash_flow = get_cash_flow(invest_note, invest_res)
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
    pass
