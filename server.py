#!/usr/bin/env python
# coding=utf-8
"""
@Description: Do not edit
@Date: 2021-08-15 13:01:11
@LastEditors: wanghaijie01
@LastEditTime: 2021-09-26 00:20:53
"""
from flask import Flask, make_response, request

from qtools.calculator import IRR, cf, generate_investment_data, nfv, nper, sa
from fund_company import company, scale, company_yield, work_year, awards, scale_ratio
from cron import crontask
from qtools import index as qindex
from qtools import debt as qdebt

app = Flask(__name__)


@app.route('/pyecharts/stacked_area_chart')
def root():
    app.static_folder = "."
    return app.send_static_file('stacked_area_chart.html')


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


@app.route("/finance/fund-company/base", methods=["get"])
def get_company_base_info():
    orderby = request.args.get("orderBy")
    orderdir = request.args.get("orderDir")
    data = company.get_company_base(orderby, orderdir)
    res = {
        "errno": 0,
        "message": "success",
        "data": {
            "items": data
        }
    }
    resp = make_response(res)
    resp.headers["Access-Control-Allow-Credentials"] = "true"
    resp.headers["Access-Control-Allow-Origin"] = "http://localhost:3000"
    return resp


@app.route("/finance/fund-company/scale", methods=["get"])
def get_scale_info():
    fund_type = request.args.get("fund_type")
    orderby = request.args.get("orderBy")
    orderdir = request.args.get("orderDir")
    data = scale.format_scale_info(fund_type, orderby, orderdir)
    res = {
        "errno": 0,
        "message": "success",
        "data": {
            "items": data
        }
    }
    resp = make_response(res)
    resp.headers["Access-Control-Allow-Credentials"] = "true"
    resp.headers["Access-Control-Allow-Origin"] = "http://localhost:3000"
    return resp


@app.route("/finance/fund-company/yield", methods=["get"])
def get_yield_info():
    fund_type = request.args.get("fund_type")
    orderby = request.args.get("orderBy")
    orderdir = request.args.get("orderDir")
    data = company_yield.get_yield_rank(fund_type, orderby, orderdir)
    res = {
        "errno": 0,
        "message": "success",
        "data": {
            "items": data
        }
    }
    resp = make_response(res)
    resp.headers["Access-Control-Allow-Credentials"] = "true"
    resp.headers["Access-Control-Allow-Origin"] = "http://localhost:3000"
    return resp

@app.route("/finance/fund-company/work-year", methods=["get"])
def get_work_year_info():
    orderdir = request.args.get("orderDir")
    data = work_year.get_work_year_info(orderdir)
    res = {
        "errno": 0,
        "message": "success",
        "data": {
            "items": data
        }
    }
    resp = make_response(res)
    resp.headers["Access-Control-Allow-Credentials"] = "true"
    resp.headers["Access-Control-Allow-Origin"] = "http://localhost:3000"
    return resp


@app.route("/finance/fund-company/awards", methods=["get"])
def get_awards_info():
    orderdir = request.args.get("orderDir")
    data = awards.get_awards_info(orderdir)
    res = {
        "errno": 0,
        "message": "success",
        "data": {
            "items": data
        }
    }
    resp = make_response(res)
    resp.headers["Access-Control-Allow-Credentials"] = "true"
    resp.headers["Access-Control-Allow-Origin"] = "http://localhost:3000"
    return resp


@app.route("/finance/fund-company/scale-ratio", methods=["get"])
def get_scale_ratio_info():
    company_code = request.args.get("company_code")
    data = scale_ratio.get_scale_ratio(company_code)
    res = {
        "errno": 0,
        "message": "success",
        "data": {
            "scale_ratio": data
        }
    }
    resp = make_response(res)
    resp.headers["Access-Control-Allow-Credentials"] = "true"
    resp.headers["Access-Control-Allow-Origin"] = "http://localhost:3000"
    return resp


@app.route("/finance/fund-company/yield-detail", methods=["get"])
def get_yield_detail():
    company_code = request.args.get("company_code")
    fund_type = request.args.get("fund_type")
    data = company_yield.get_yield_detail(company_code, fund_type)
    res = {
        "errno": 0,
        "message": "success",
        "data": {
            "yield_detail": data
        }
    }
    resp = make_response(res)
    resp.headers["Access-Control-Allow-Credentials"] = "true"
    resp.headers["Access-Control-Allow-Origin"] = "http://localhost:3000"
    return resp


@app.route("/index/fundmental/percentile", methods=["get"])
def get_index_fundmental_percentile():
    code = request.args.get("stock_code")
    index = qindex.Index()
    data = index.get_latest_index_fundmental_percentile_data(code)
    res = {
        "errno": 0,
        "message": "success",
        "data": data
    }
    resp = make_response(res)
    resp.headers["Access-Control-Allow-Credentials"] = "true"
    resp.headers["Access-Control-Allow-Origin"] = "http://localhost:3000"
    return resp


@app.route("/index/fundmental/percentile-list", methods=["get"])
def get_latest_index_fundmental_percentile_list():
    codes = request.args.get("stock_codes")
    granularity = request.args.get("granularity")
    stock = qindex.Index()
    data = stock.get_latest_index_fundmental_percentile_list(codes.split(","), granularity)
    res = {
        "errno": 0,
        "message": "success",
        "data": data
    }
    resp = make_response(res)
    resp.headers["Access-Control-Allow-Credentials"] = "true"
    resp.headers["Access-Control-Allow-Origin"] = "http://localhost:3000"
    return resp


@app.route("/index/fundmental/line-plot-data", methods=["get"])
def get_index_fundmental_line_plot_data():
    codes = request.args.get("stock_codes")
    metrics = request.args.get("metrics")
    index = qindex.Index()
    data = index.get_index_line_plot_data(codes, metrics)
    res = {
        "errno": 0,
        "message": "success",
        "data": data
    }
    resp = make_response(res)
    resp.headers["Access-Control-Allow-Credentials"] = "true"
    resp.headers["Access-Control-Allow-Origin"] = "http://localhost:3000"
    return resp


@app.route("/debt/mix-metrics/fed", methods=["get"])
def get_debt_fed_data():
    code = request.args.get("stock_code")
    debt = qdebt.Debt()
    data = debt.compute_fed(code)
    res = {
        "errno": 0,
        "message": "success",
        "data": data
    }
    resp = make_response(res)
    resp.headers["Access-Control-Allow-Credentials"] = "true"
    resp.headers["Access-Control-Allow-Origin"] = "http://localhost:3000"
    return resp


if __name__ == '__main__':
    crontask.start()
    app.config['JSON_AS_ASCII'] = False
    app.run(host="0.0.0.0", port="8000")
