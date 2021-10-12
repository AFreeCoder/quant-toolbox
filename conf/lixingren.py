#!/usr/bin/env python
# coding=utf-8
"""
@Description: Do not edit
@Date: 2021-09-21 17:00:40
@LastEditors: wanghaijie01
@LastEditTime: 2021-10-12 20:42:20
"""

# metricsType
## 市值加权 :mcw
## 等权 :ew
## 正数等权 :ewpvo
## 平均值 :avg
## 中位数 :median

index_conf = {
    # 沪深300
    "000300": {
        "metric_type": "mcw",
        "temperature_type": "pe_ttm"
    },
    # 中证500
    "000905": {
        "metric_type": "mcw",
        "temperature_type": "pe_ttm"
    },
    # 中证红利
    "000922": {
        "metric_type": "mcw",
        "temperature_type": "pe_ttm"
    },
    # 创业板指
    "399006": {
        "metric_type": "mcw",
        "temperature_type": "pe_ttm"
    },
    # 全指信息
    "000993": {
        "metric_type": "mcw",
        "temperature_type": "pe_ttm"
    },
    # 中证消费
    "000932": {
        "metric_type": "mcw",
        "temperature_type": "pe_ttm"
    },
    # 全指医药
    "000991": {
        "metric_type": "mcw",
        "temperature_type": "pe_ttm"
    },
    # 中证养老
    "399812": {
        "metric_type": "ew",
        "temperature_type": "pe_ttm"
    },
    # 中国互联网50 H30533
    "H30533": {
        "metric_type": "mcw",
        "temperature_type": "pe_ttm"
    }
}

candidate_codes = [
    "000300",
    "000905",
    "000922",
    "399006",
    "000993",
    "000932",
    "000991",
    "399812",
    "H30533"
]
