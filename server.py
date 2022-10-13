#!/usr/bin/env python
# coding=utf-8
"""
@Description: Do not edit
@Date: 2021-08-15 13:01:11
@LastEditors: wanghaijie01
@LastEditTime: 2021-09-26 00:20:53
"""
from datetime import datetime
import logging

from flask import Flask, make_response, request

from qtools.calculator import IRR, cf, generate_investment_data, nfv, nper, sa
from fund_company import company, scale, company_yield, work_year, awards, scale_ratio, company_filter
from cron import crontask
from qtools import index as qindex
from qtools import debt as qdebt

app = Flask(__name__)

ACCESS_CONTROL_ALLOW_ORIGIN = "http://localhost:3000"

@app.after_request
def after_request(response):
    response.headers["Access-Control-Allow-Credentials"] = "true"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Origin"] = ACCESS_CONTROL_ALLOW_ORIGIN
    return response


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
    return resp


@app.route("/finance/fund-company/scale", methods=["get"])
def get_scale_info():
    fund_type = request.args.get("fund_type")
    orderby = request.args.get("orderBy")
    orderdir = request.args.get("orderDir")
    df = scale.format_scale_info(fund_type, orderby, orderdir)
    res = {
        "errno": 0,
        "message": "success",
        "data": {
            "items": df.to_dict("records")
        }
    }
    resp = make_response(res)
    return resp


@app.route("/finance/fund-company/yield", methods=["get"])
def get_yield_info():
    fund_type = request.args.get("fund_type")
    orderby = request.args.get("orderBy")
    orderdir = request.args.get("orderDir")
    df = company_yield.get_yield_rank(fund_type, orderby, orderdir)
    res = {
        "errno": 0,
        "message": "success",
        "data": {
            "items": df.to_dict("records")
        }
    }
    resp = make_response(res)
    return resp

@app.route("/finance/fund-company/work-year", methods=["get"])
def get_work_year_info():
    orderdir = request.args.get("orderDir")
    df = work_year.get_work_year_info(orderdir)
    res = {
        "errno": 0,
        "message": "success",
        "data": {
            "items": df.to_dict("records")
        }
    }
    resp = make_response(res)
    return resp


@app.route("/finance/fund-company/awards", methods=["get"])
def get_awards_info():
    orderdir = request.args.get("orderDir")
    df = awards.get_awards_info(orderdir)
    res = {
        "errno": 0,
        "message": "success",
        "data": {
            "items": df.to_dict("records")
        }
    }
    resp = make_response(res)
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
    return resp


@app.route("/finance/fund-company/filter-res", methods=["post"])
def get_fund_company_filter_res():
    json_body = request.get_json()
    df = company_filter.apply_condition_expression(json_body["conditions"])
    res = {
        "errno": 0,
        "message": "success",
        "data": {
            "items": df.to_dict("records")
        }
    }
    resp = make_response(res)
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
    return resp


@app.route("/debt/mix-metrics/fed", methods=["get"])
def get_debt_fed_data():
    code = request.args.get("stock_code")
    compute_method = request.args.get("compute_method")
    debt = qdebt.Debt()
    data = debt.compute_fed(code, compute_method)
    res = {
        "errno": 0,
        "message": "success",
        "data": data
    }
    resp = make_response(res)
    return resp


if __name__ == '__main__':
    # 设置日志
    today = datetime.now().strftime("%Y%m%d")
    logging.basicConfig(level=logging.INFO,#控制台打印的日志级别
                    filename='logs/service-' + today + '.log',
                    filemode='a',##模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志
                    #a是追加模式，默认如果不写的话，就是追加模式
                    format=
                    '%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    #日志格式
                    )
    crontask.start()
    app.config['JSON_AS_ASCII'] = False
    app.run(host="0.0.0.0", port="8000")
