#!/usr/bin/env python
# coding=utf-8
"""
@Description: Do not edit
@Date: 2021-08-15 13:01:11
@LastEditors: wanghaijie01
@LastEditTime: 2021-08-29 19:49:43
"""

from flask import Flask, request

from qtools import strategy
from qtools.calculator import IRR, cf, generate_investment_data, nfv, sa, nper

app = Flask(__name__)

@app.route('/calculator/investment/fixed', methods=["get"])
def fix_investment_calculator():
    f = lambda key, default: request.args.get(key) if (request.args.get(key) != None and request.args.get(key) != "") else default
    start_amount = float(f("start_amount", 0))
    year = int(f("year", 0))
    irr = float(f("irr", 0))/100
    frequently = f("frequently", "m")
    cash_flow = float(f("cash_flow", 0))
    end_amount = float(f("end_amount", 0))
    cal_type = f("cal_type", "end_amount_cal")
    if cal_type == "end_amount_cal":
        end_amount = nfv(start_amount, year, irr, frequently, cash_flow)
    elif cal_type == "irr_cal":
        irr = IRR(start_amount, year, frequently, cash_flow, end_amount)
    elif cal_type == "cash_flow_cal":
        cash_flow = cf(start_amount, year, irr, frequently, end_amount)
    elif cal_type == "start_amount_cal":
        start_amount = sa(year, irr, frequently, cash_flow, end_amount)
    elif cal_type == "year_cal":
        year = nper(start_amount, irr, frequently, cash_flow, end_amount)
    summary, cordinate = generate_investment_data(start_amount, year, irr, frequently, cash_flow, end_amount)
    res = {
            "invest_res": summary,
            "cordinate": cordinate
        }
    return res


def filterParams(request, key, default):
    value = request.get(key)
    if value == None or value == "":
        value = default
    return value

if __name__ == '__main__':
    app.run(host="127.0.0.1", port="8000")
