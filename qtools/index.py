#!/usr/bin/env python
# coding=utf-8
"""
@Description: 计算指数相关指标
@Date: 2021-09-21 15:30:49
@LastEditors: wanghaijie01
@LastEditTime: 2021-09-26 01:41:40
"""

from dis import code_info
import os
import sys

import pandas as pd

sys.path.append(os.getcwd())
from source import index

code_metrics_info = {
    # 沪深300
    "000300": {
        "metrics_name": "pb",
        "metrics_type": "mcw"
    }, 
    # 中证500
    "000905": {
        "metrics_name": "pb",
        "metrics_type": "mcw"
    }, 
    # # 中证红利
    # "000922": {
    #     "metrics_name": "pb",
    #     "metrics_type": "mcw"
    # }, 
    # 创业板指
    "399006": {
        "metrics_name": "pb",
        "metrics_type": "mcw"
    }, 
    # 全指信息
    "000993": {
        "metrics_name": "pb",
        "metrics_type": "mcw"
    }, 
    # 中证消费
    "000932": {
        "metrics_name": "pb",
        "metrics_type": "mcw"
    }, 
    # 全指医药
    "000991": {
        "metrics_name": "pb",
        "metrics_type": "mcw"
    }, 
    # 中证养老
    "399812": {
        "metrics_name": "pb",
        "metrics_type": "mcw"
    },  
    # 中国互联网50
    "H30533": {
        "metrics_name": "ps_ttm",
        "metrics_type": "mcw"
    },
    # 中证1000
    "000852": {
        "metrics_name": "pb",
        "metrics_type": "mcw"
    }
}

class Index:

    def __init__(self) -> None:
        self.index_dao = index.Index()

    def get_latest_index_fundmental_percentile_list(self, codes: list, granularity: str):
        index_obj = index.Index()
        index_basic = index_obj.load_index_basic()
        percentile_list = []
        for code in codes:
            if code not in code_metrics_info:
                continue
            # 指标的权重计算需要和指数的权重计算方式保持一致，因为最终买的是指数
            # caculationMethod 说明：https://www.lixinger.com/wiki/index-point-calculation-method
            # 目前看，只有分级靠挡下PB加权是合理的（大部分指数都是）
            # 红利指数PB计算的问题：https://zhuanlan.zhihu.com/p/410016876
            code_name = index_basic.at[code, "name"]
            metrics_name = code_metrics_info[code]["metrics_name"]
            metrics_type = code_metrics_info[code]["metrics_type"]
            granularity = "y10"
            latest_data = index_obj.get_latest_index_fundmental_percentile(code, metrics_name, metrics_type, granularity)
            percentile_text = str(round(latest_data[7] * 100, 2)) + "%"
            percentile_list.append(
                {
                    "id": latest_data[0],
                    "code": latest_data[1],
                    "name": code_name,
                    "metrics_name": metrics_name,
                    "update_date": latest_data[5],
                    "cvpos": latest_data[7],
                    "percentile": percentile_text,
                    "recommend_position": str(recommend_positioin(latest_data[7])),
                    "recommend_operation": recommend_operation(latest_data[7])
                }
            )
        return percentile_list



    def get_latest_index_fundmental_percentile_data(self, code: str):
        data = self.get_latest_index_fundmental_percentile_list([code], "y10")[0]
        data["cvpos"] = round(data["cvpos"] *100, 2)
        return data


    def get_index_line_plot_data(self, codes: str, metrics: str):
        if metrics in ["cp", "pe_ttm", "pb", "ps_ttm"]:
            return self.get_index_fundmental_plot_data(codes, metrics)
        elif metrics in ["pb-percentile"]:
            return self.get_index_fundmental_percentile_plot_data(codes)
        else:
            return {}


    def get_index_fundmental_percentile_plot_data(self, codes: str):
        index_obj = index.Index()
        df = pd.DataFrame()
        for code in codes.split(","):
            metrics_name = code_metrics_info[code]["metrics_name"]
            metrics_type = code_metrics_info[code]["metrics_type"]
            df_temp = index_obj.load_index_fundmental_percentile(code, metrics_name, metrics_type, "y10")
            df_temp["cvpos"] = round(df_temp["cvpos"] * 100, 2)
            df_temp.rename(columns={
                "cvpos": code + "_y_data"
            }, inplace=True)
            if df.empty:
                df = df_temp
            else:
                df = pd.merge(df, df_temp, on="date", how="outer")
        df.sort_values(by="date", ascending=True, inplace=True)
        df.fillna(0, inplace=True)
        data = {
            "x_data": df["date"].tolist(),
            "count": len(df["date"])
        }
        for code in codes.split(","):
            data[code + "_y_data"] = df[code + "_y_data"].tolist()
        return data


    def get_index_fundmental_plot_data(self, codes: str, metrics: str):
        index_obj = index.Index()
        df = pd.DataFrame()
        for code in codes.split(","):
            metrics_type = code_metrics_info[code]["metrics_type"]
            df_temp = index_obj.load_index_fundmental(code, metrics_type)
            df_temp[metrics] = round(df_temp[metrics], 2)
            df_temp.rename(columns={
                metrics: code + "_y_data"
            }, inplace=True)
            if df.empty:
                df = df_temp
            else:
                df = pd.merge(df, df_temp, on="date", how="outer")
        df.sort_values(by="date", ascending=True, inplace=True)
        df.fillna(0, inplace=True)
        data = {
            "x_data": df["date"].tolist(),
            "count": len(df["date"])
        }
        for code in codes.split(","):
            data[code + "_y_data"] = df[code + "_y_data"].tolist()
        return data


def recommend_positioin(percentile: float):
    p_int = round(percentile * 100)
    if p_int >= 70:
        return [0, 0]
    elif p_int >= 60:
        return [0, 10]
    elif p_int >= 50:
        return [10, 20]
    elif p_int >= 40:
        return [20, 30]
    elif p_int >= 30:
        return [30, 40]
    elif p_int >= 20:
        return [40, 50]
    elif p_int >= 10:
        return [50, 60]
    elif p_int >= 5:
        return [60, 70]
    else:
        return [80, 100]


def recommend_operation(percentile: float):
    p_int = round(percentile * 100)
    if p_int >= 70:
        return "卖出，至少不要买入"
    elif p_int >= 45:
        return "谨慎买入"
    elif p_int >= 30:
        return "乐观买入"
    elif p_int >= 10:
        return "大胆买入"
    elif p_int >= 5:
        return "闭着眼买入"
    else:
        return "啥也别说了，冲！"


if __name__ == "__main__":
    pass
