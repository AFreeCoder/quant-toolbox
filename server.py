#!/usr/bin/env python
# coding=utf-8
"""
@Description: Do not edit
@Date: 2021-08-15 13:01:11
@LastEditors: wanghaijie01
@LastEditTime: 2021-09-26 00:20:53
"""

from datetime import datetime

from flask import Flask, request

from qtools.calculator import IRR, cf, generate_investment_data, nfv, nper, sa
from qtools.index import compute_percentile

app = Flask(__name__)


@app.route('/calculator/investment/fixed', methods=["get"])
def fix_investment_calculator():
    f = lambda key, default: request.args.get(key) if (request.args.get(key) != None and request.args.get(key) != "") else default
    start_amount = float(f("start_amount", 0))
    year = float(f("year", 0))
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


@app.route('/metric/index/pt', methods=["get"])
def index_value_search():
    """
    @description: 获取指数估值
    @param:
        code: 指数code
        date: 日期，示例格式 2000-01-01
    @return:
        {
            "date": "2020-01-01" ,
            "pe_ttm": 1.1,
            "pe_ttm_percentile": 0.2,
            "pb": 1.1,
            "pb_percentil": 0.3
        }
    """    
    f = lambda key, default: request.args.get(key) if (request.args.get(key) != None and request.args.get(key) != "") else default
    stock_codes = f("stock_codes", "")
    today = datetime.strftime(datetime.today(), "%Y-%m-%d")
    date = f("date", today)
    pt_res = compute_percentile(date, stock_codes)
    res = {
        "errno":0,
        "errmsg": "",
        "data": pt_res
    }
    return res

if __name__ == '__main__':
    app.run(host="127.0.0.1", port="8000")
