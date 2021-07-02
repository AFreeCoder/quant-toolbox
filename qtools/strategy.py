#!/usr/bin/env python
# coding=utf-8
"""
@Description: Do not edit
@Date: 2021-06-18 13:36:11
@LastEditors: wanghaijie01
@LastEditTime: 2021-07-02 23:24:38
"""

from datetime import datetime, timedelta

import pandas as pd
from util import dateutil


def fix_investment(df: pd.DataFrame, start, end, frequent, quantity):
    """定投策略
    args:
        df: column=[ts_code,trade_date,close,open,high,low,pre_close,change,pct_chg,vol,amount]
            *注意*: trade_date 类型为datetime
        start: 字符串，示例：20200101
        end: 同start
        frequent: m表示月，w表示周，其余不支持
        quantity: 定投金额
    returns:
        返回值格式: (v1, v2)
        v1 表示资金进出记录，格式为[(date, point, cash)...]，cash为负表示买入，为正表示卖出
        v2 表示end当天的情况，格式为(date, point, cash)，此处cash表示资产总额
    """
    date_order = dateutil.gen_date_order(start, end, frequent)
    end_date = datetime.strptime(end, "%Y%m%d")
    df = df[["trade_date", "close"]]
    i = 0
    share = 0
    invest_note = []
    for _, row in df.iterrows():
        if i < len(date_order) and row["trade_date"] >= date_order[i]:
            share += quantity / row["close"]
            invest_note.append(
                (row["trade_date"], round(row["close"], 2), -quantity))
            i += 1
    df = df[df["trade_date"] <= end_date]
    current_point = df["close"][len(df)-1]
    total_amount = round(current_point * share, 2)
    return invest_note, (end_date, current_point, total_amount)


def evaluate_index_percent(df: pd.DataFrame, input: pd.Series):
    """对指数估值，返回输入数据在历史数据中的百分位
    args:
        df: 历史数据
        input: index至少包含[trade_date, pe]
    return:
        pt: 百分位
    注：暂时仅计算PE百分位
    """

    # 百分位的统计只能基于历史数据
    history_df = df[df["trade_date"] <= input["trade_date"]]
    ten_years_before = input["trade_date"] + timedelta(days=-3650)
    history_df = history_df[history_df["trade_date"] > ten_years_before]
    low_df = history_df[history_df["pe"] <= input["pe"]]
    pt = round(len(low_df) / len(history_df), 2)
    return pt


def fix_investment_plus_v1(df: pd.DataFrame, start, end, low_percent, high_percent, frequent, quantity):
    """定投策略加强版 v1
    根据估值进行定投，低于百分位p1, 买入; 高于百分位p2, 卖出; p1到p2之间，不操作, 持有现金
    args:
        df: column=[ts_code,trade_date,close,open,high,low,pre_close,change,pct_chg,vol,amount]
            *注意*: trade_date 类型为datetime
        percent_low: 买入百分位
        percent_high: 卖出百分位
        start: 字符串，示例：20200101
        end: 同start
        frequent: m表示月，w表示周，其余不支持
        quantity: 定投金额
    returns:
        返回值格式: (v1, v2)
        v1 表示资金进出记录，格式为[(date, point, cash)...]，cash为负表示买入，为正表示卖出
        v2 表示end当天的情况，格式为(date, point, cash)，此处cash表示资产总额

    note: 记录现金流的时候涉及两个账户，一个是现金账户，一个是指数账户，这里只计算现金账户的进出，不考虑现金账户和指数账户之间的转账
    """
    date_order = dateutil.gen_date_order(start, end, frequent)
    end_date = datetime.strptime(end, "%Y%m%d")
    df = df[["trade_date", "close", "pe"]]
    i = 0
    share = 0
    invest_note = []
    cash = 0
    for _, row in df.iterrows():
        # 定投日期（操作日期）
        if i < len(date_order) and row["trade_date"] >= date_order[i]:
            cash += quantity
            invest_note.append(
                (row["trade_date"], round(row["close"], 2), -quantity))
            # 根据估值判断是买点，还是卖点，还是不操作
            current_percent = evaluate_index_percent(df, row)
            if current_percent <= low_percent:
                # 买入
                share += cash / row["close"]
                cash = 0
            elif current_percent >= high_percent:
                # 卖出
                cash += share * row["close"]
                share = 0
            else:
                # 不操作
                pass
            i += 1
    df = df[df["trade_date"] <= end_date]
    current_point = df["close"][len(df)-1]
    total_amount = round(current_point * share + cash, 2)
    return invest_note, (end_date, current_point, total_amount)


if __name__ == "__main__":
    pass
