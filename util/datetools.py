#!/usr/bin/env python
# coding=utf-8
"""
@Description: date 相关小工具
@Date: 2021-06-19 17:58:06
@LastEditors: wanghaijie01
@LastEditTime: 2021-06-28 00:42:30
"""

from datetime import datetime

from dateutil import relativedelta


def gen_date_order(start, end, frequent):
    """生成起始日期为start，终止日期为end，定投频率为 frequent 的时间序列
    args:
        start: 起始日期，字符串类型，格式：%Y%m%d
        end: 终止日期，格式同 start
        frequent: 定投频率，字符串类型。m:月；w:周
    return:
        
    """    
    start_time = datetime.strptime(start, "%Y%m%d")
    end_time = datetime.strptime(end, "%Y%m%d")
    date_order = []
    i = 0
    while i>=0:
        if frequent == "m":
            week = 0; month = i
        elif frequent == "w":
            week = i; month = 0
        else:
            raise Exception("Invalid param: frequent!", frequent)
        temp = start_time + relativedelta.relativedelta(months=month, weeks=week)
        if temp > end_time:
            break
        date_order.append(temp)
        i += 1
    return date_order


if __name__ == "__main__":
    print(gen_date_order("20200102", "20200304", "w"))
