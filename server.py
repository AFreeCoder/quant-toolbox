#!/usr/bin/env python
# coding=utf-8
"""
@Description: Do not edit
@Date: 2021-08-15 13:01:11
@LastEditors: wanghaijie01
@LastEditTime: 2021-08-15 20:00:15
"""

from flask import Flask, request

from qtools import strategy
from qtools.calculator import generate_investment_data, nfv, summary

app = Flask(__name__)

@app.route('/calculator/investment/fixed', methods=["get"])
def fix_investment_calculator():
    start_amount = float(request.args.get("start_amount", 0))
    year = int(request.args.get("year", 1))
    irr = float(request.args.get("irr", 0))/100
    frequently = request.args.get("frequently", "m")
    cash_flow = float(request.args.get("cash_flow", 0))
    end_amount = float(request.args.get("end_amount", 0))
    cal_type = request.args.get("cal_type", "end_amount_cal")
    if cal_type == "end_amount_cal":
        end_amount = nfv(start_amount, year, irr, frequently, cash_flow)
        summary, cordinate = generate_investment_data(start_amount, year, irr, frequently, cash_flow, end_amount)
        res = {
            "invest_res": summary,
            "cordinate": cordinate
        }
        return res
    return 'Hello World!'

if __name__ == '__main__':
    app.run(host="172.96.244.212", port="8000")
