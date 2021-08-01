#!/usr/bin/env python
# coding=utf-8
"""
@Description: Do not edit
@Date: 2021-06-18 13:36:11
@LastEditors: wanghaijie01
@LastEditTime: 2021-08-01 17:52:39
"""

from datetime import datetime, timedelta
from typing import List

import pandas as pd
from numpy.lib.function_base import append
from util import datetools


class Bond:

    def __init__(self, daily_data) -> None:
        self.daily = daily_data
        self.share = 0
        self.operation_note = []

    def add_operation_note(self, date, point, quantity):
        self.operation_note.append({
            "date": date,
            "point": point,
            "quantity": quantity
        })

    def compute_percentile(self, input):
        """对指数估值，返回输入数据在历史数据中的百分位
        args:
            df: 历史数据
            input: index至少包含[trade_date, pe]
        return:
            pt: 百分位
        注：暂时仅计算PE百分位
        """

        # 百分位的统计只能基于历史数据
        history_df = self.daily[self.daily["trade_date"]
                                <= input["trade_date"]]
        ten_years_before = input["trade_date"] + timedelta(days=-3650)
        history_df = history_df[history_df["trade_date"] > ten_years_before]
        low_df = history_df[history_df["pe"] <= input["pe"]]
        pt = round(len(low_df) / len(history_df), 2)
        return pt

    def buy_on_day(self, date, quantity):
        daily = self.get_index_data_by_day(date)
        close = daily["close"]
        self.share += quantity / close
        self.add_operation_note(date, close, -quantity)
        return

    def sell_on_day(self, date, share):
        daily = self.get_index_data_by_day(date)
        close = daily["close"]
        self.share -= share
        self.add_operation_note(date, close, share * close)
        return share * close

    def sell_out_on_day(self, date):
        daily = self.get_index_data_by_day(date)
        close = daily["close"]
        sell_out = self.share * close
        self.add_operation_note(date, close, sell_out)
        self.share = 0
        return sell_out

    def sell_out_before_day(self, date):
        daily = self.get_index_data_before_day(date)
        close = daily["close"]
        sell_out = self.share * close
        self.add_operation_note(date, close, sell_out)
        self.share = 0
        return sell_out

    def get_index_data_by_day(self, date):
        """根据传入的日期，返回定投日数据
        """
        df = self.daily[self.daily["trade_date"] >= date]
        if len(df) == 0:
            return pd.Series()
        return df.iloc[0]

    def get_index_data_before_day(self, date):
        """根据传入的日期，返回最近一日数据
        """
        df = self.daily[self.daily["trade_date"] <= date]
        return df.iloc[-1]


def fix_investment(bond:Bond, start, end, frequent, quantity):
    """定投策略
    args:
        bond: Bond 实例
        start: 字符串，示例：20200101
        end: 同start
        frequent: m表示月，w表示周，其余不支持
        quantity: 定投金额
    returns:
        invest_note: 格式为 list({
            "date": date,
            "quantity": cash
        })
        cash为负表示买入，为正表示卖出
    """
    date_order = datetools.gen_date_order(start, end, frequent)
    end_date = datetime.strptime(end, "%Y%m%d")
    invest_note = []
    for d in date_order:
        bond.buy_on_day(d, quantity)
        invest_note.append({
            "date": d,
            "quantity": -quantity
        })
    total_amount = bond.sell_out_before_day(end_date)
    invest_note.append({
        "date": end_date,
        "quantity": total_amount
    })
    return invest_note


def fix_investment_plus_v1(bond:Bond, start, end, low_percent, high_percent, frequent, quantity):
    """定投策略加强版 v1
    根据估值进行定投，低于百分位p1, 买入; 高于百分位p2, 卖出; p1到p2之间，不操作, 持有现金
    args:
        bond: Bond 实例
        percent_low: 买入百分位
        percent_high: 卖出百分位
        start: 字符串，示例：20200101
        end: 同start
        frequent: m表示月，w表示周，其余不支持
        quantity: 定投金额
    returns:
        invest_note: 格式为 list({
            "date": date,
            "quantity": cash
        })
        cash为负表示买入，为正表示卖出
    """
    date_order = datetools.gen_date_order(start, end, frequent)
    end_date = datetime.strptime(end, "%Y%m%d")
    cash = 0
    invest_note = []
    for d in date_order:
        cash += quantity
        invest_note.append({
            "date": d,
            "quantity": -quantity
        })
        input = bond.get_index_data_by_day(d)
        percent = bond.compute_percentile(input)
        if percent <= low_percent:
            # 低估买入
            bond.buy_on_day(d, cash)
            cash = 0
        elif percent >= high_percent:
            # 高估卖出
            cash += bond.sell_out_on_day(d)
        else:
            pass
    # 截止日期卖出，计算账户净值
    cash += bond.sell_out_on_day(end_date)
    invest_note.append({
        "date": end_date,
        "quantity": cash
    })
    return invest_note


def fix_investment_plus_v2(bond_list:List[Bond], start, end, low_percent, high_percent, frequent, quantity):
    """定投策略加强版 v2
    对n只指数根据估值进行定投，低于百分位p1, 平分买入; 高于百分位p2, 都卖出; p1到p2之间，不操作, 持有现金
    这里假设任意一只指数的起始日期都大于定投起始日期
    args:
        df: column=[ts_code,trade_date,close,open,high,
            low,pre_close,change,pct_chg,vol,amount]
            注意: trade_date 类型为datetime
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
    date_order = datetools.gen_date_order(start, end, frequent)
    end_date = datetime.strptime(end, "%Y%m%d")
    invest_note = []
    cash = 0
    for d in date_order:
        under = list()
        over = list()
        # 判断定投日期时的估值水平
        for bond in bond_list:
            daily = bond.get_index_data_by_day(d)
            current_percent = bond.compute_percentile(daily)
            if current_percent <= low_percent:
                under.append(bond)
            elif current_percent >= high_percent:
                over.append(bond)
            else:
                pass
        cash += quantity
        invest_note.append({
            "date": d,
            "quantity": -quantity
        })
        # 先买入低估的
        for bond in under:
            # 将剩余现金全部平均买入低估指数
            bond.buy_on_day(d, cash / len(under))
        
        if len(under) > 0:
            cash = 0

        # 卖出高估的
        for bond in over:
            sales = bond.sell_out_on_day(d)
            # invest_note.append((d, 0, sales))
            cash += sales
    
    # 截止日期 end_date，全部卖出
    end_date = datetime.strptime(end, "%Y%m%d")
    for bond in bond_list:
        cash += bond.sell_out_before_day(end_date)
        
    total_amount = cash
    invest_note.append({
        "date": end_date,
        "quantity": total_amount
    })
    return invest_note


if __name__ == "__main__":
    pass
