#!/usr/bin/env python
# coding=utf-8
"""
@Description: 计算指数相关指标
@Date: 2021-09-21 15:30:49
@LastEditors: wanghaijie01
@LastEditTime: 2021-09-26 01:41:40
"""

import os
import sqlite3
import sys
from datetime import datetime, timedelta
from typing import List

import pandas as pd

sys.path.append(os.getcwd())
from conf import lixingren


class Index:

    def __init__(self, code:str) -> None:
        self.code = code
        self.index_conf = lixingren.index_conf[code]
        self.table_name = "index_" + self.code
        self.metric_type = self.index_conf["metric_type"]
        self.conn = sqlite3.connect("data/index.db")
        self.basic = self.load_basic()
        self.data = self.load_data()

    def load_basic(self):
        sql = "select * from `{}`".format("index_basic")
        data = pd.read_sql(sql, self.conn, index_col="stockCode", parse_dates=["launchDate"])
        basic = data.loc[self.code].to_dict()
        return basic

    def load_data(self):
        sql = "select * from `{}` where metric_type='{}'".format(self.table_name, self.metric_type)
        data = pd.read_sql(sql, self.conn, parse_dates=["date"])
        data.sort_values(by="date", ascending=True, inplace=True)
        return data

    def compute_percentile_by_date(self, date:str):
        """根据输入日期计算pe、pb百分位。计算周期为8年，不满8年，使用全部历史数据。
        args:
            date: 日期
        return:
            res: dict
                - pe_ttm
                - pe_ttm_percentile
                - pb
                - pb_percentile
        """
        date = datetime.strptime(date, "%Y-%m-%d")
        history_df = self.data[self.data["date"]<=date]
        day_data = history_df.iloc[-1]
        # 剔除8年前的数据
        # n_year_before = date + timedelta(days=-2920)
        n_year_before = date + timedelta(days=-3650)
        history_df = history_df[history_df["date"]>n_year_before]
        pe_ttm = day_data["pe_ttm"]
        pe_ttm_percentile = len(history_df[history_df["pe_ttm"]<day_data["pe_ttm"]])/len(history_df)
        pb = day_data["pb"]
        pb_percentile = len(history_df[history_df["pb"]<day_data["pb"]])/len(history_df)
        res = {
            "code": self.code,
            "name": self.basic["name"],
            "date": datetime.strftime(day_data["date"], "%Y-%m-%d") ,
            "pe_ttm": round(pe_ttm, 2),
            "pe_ttm_percentile": round(pe_ttm_percentile, 4),
            "pb": round(pb, 2),
            "pb_percentile": round(pb_percentile, 4)
        }
        pt = pe_ttm_percentile
        if self.index_conf["temperature_type"] == "pb":
            pt = pb_percentile
        res["pt"] = pt
        res["T"] = temperature_format(pt)
        res["level"] = temperature_level(pt)
        return res


    def compute_max_drawdown(self, date: str):
        date = datetime.strptime(date, "%Y-%m-%d")
        df = self.data[self.data["date"]>=date]
        i = 0; j = i+1
        hindex = 0
        lindex = 0
        max_drawdown = 0
        while i<=j<len(df):
            df.iloc[i]["cp"]
            if df.iloc[i]["cp"]-df.iloc[j]["cp"] > max_drawdown:
                max_drawdown = df.iloc[i]["cp"] - df.iloc[j]["cp"]
                hindex = i
                lindex = j
            if df.iloc[j]["cp"] > df.iloc[i]["cp"]:
                i=j
                j=j+1
            else:
                j=j+1
        ratio = max_drawdown / df.iloc[hindex]["cp"]
        print(df.iloc[hindex])
        print(df.iloc[lindex])
        return ratio


def compute_percentile(date: str, stock_code: List[str]):
    codes = lixingren.candidate_codes
    if len(stock_code) != 0:
        codes = stock_code.split(",")
    res = []
    for code in codes:
        index = Index(code)
        pt = index.compute_percentile_by_date(date)
        res.append(pt)
    res.sort(key=lambda x:x["pt"])
    return res


def temperature_format(pt: float):
    value = round(pt * 100, 2)
    temperature = str(value) + "℃"
    return temperature


def temperature_level(temperature: float):
    """温度等级：L, M, H
    """
    if temperature <= 0.3:
        return "L"
    elif temperature >=0.7:
        return "H"
    else:
        return "M"


if __name__ == "__main__":
    code = "000300"
    index = Index(code)
    res = index.compute_percentile_by_date("2021-09-22")
    # res = index.compute_max_drawdown("2020-09-21")
    print(res)
