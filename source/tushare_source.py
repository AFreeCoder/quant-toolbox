#!/usr/bin/env python
# coding=utf-8

import os
import sys
import time

sys.path.append(os.getcwd())

import pandas as pd
import tushare as ts
from conf import tushare

data_dir = os.getcwd() + '/data/'

ts.set_token(tushare.token)


def gen_index_daily_filename(code):
    """生成记录指数日线行情数据的文件名
    """    
    filename = os.path.join(data_dir, "tushare_index_daily_" + code + ".csv")
    return filename


def gen_index_daily_basic_filename(code):
    """生成记录指数每日指标的文件名
    """ 
    filename = os.path.join(data_dir, "tushare_index_daily_basic_" + code + ".csv")
    return filename


def get_index_basic(code):
    """获取指数基本信息，如指数名称，全称，基日，基点，类别，发布日期等
    接口文档：https://waditu.com/document/2?doc_id=94
    """    
    pro = ts.pro_api()
    df = pro.index_basic(ts_code=code)
    return df


def batch_insert_index_daily(filename, code, start, end):
    """批量更新指数日线数据，追加方式写入csv
    分批更新是为了减小tushare的压力
    """    
    filename = gen_index_daily_filename(code)
    header = False
    if not os.path.exists(filename):
        header = True

    pro = ts.pro_api()
    res = pro.index_daily(ts_code=code,
                        start_date=str(start),
                        end_date=str(end))
    res.sort_values(by="trade_date", axis=0, ascending=True, inplace=True)
    res.to_csv(filename, mode="a", header=header, index=False)
    return res


def batch_insert_index_daily_basic(filename, code, start, end):
    """批量更新指数指标（当日流通情况，PE、PB、换手率等），追加方式写入csv
    分批更新是为了减小tushare的压力
    """    
    header = False
    if not os.path.exists(filename):
        header = True

    pro = ts.pro_api()
    res = pro.index_dailybasic(ts_code=code,
                        start_date=str(start),
                        end_date=str(end))
    res.sort_values(by="trade_date", axis=0, ascending=True, inplace=True)
    res.to_csv(filename, mode="a", header=header, index=False)
    return res


def get_last_date(code, filename):
    """获取最近一条历史数据的日期，如果没有，就返回指数基日
    """    
    if not os.path.exists(filename):
        basic_info = get_index_basic(code)
        return int(basic_info["base_date"])
    else:
        history_date = pd.read_csv(filename)['trade_date']
        return int(history_date.tail(1))


def update_index_daily(filename, code):
    """更新指数日线数据
    """    
    now = int(time.strftime("%Y%m%d", time.localtime(time.time())))
    start = get_last_date(code, filename)+1
    # 每次获取一年的数据
    # 这里日期都是用整数表示，所以加10000等于增加一年
    while True :
        end = start + 9999
        batch_insert_index_daily(filename, code, start, end)
        start = end + 1
        if start > now :
            break
    return


def update_index_daily_basic(filename, code):
    """更新指数每日指标数据
    """
    now = int(time.strftime("%Y%m%d", time.localtime(time.time())))
    start = get_last_date(code, filename)+1
    # 每次获取一年的数据
    # 这里日期都是用整数表示，所以加10000等于增加一年
    # for e in range(start+10000, int(now)+10000, 10000):
    #     batch_insert_index_daily_basic(code, e-10000, e)
    while True :
        end = start + 9999
        batch_insert_index_daily_basic(filename, code, start, end)
        start = end + 1
        if start > now :
            break
    return


if __name__ == "__main__":
    for code in tushare.index_codes:
        filename = gen_index_daily_filename(code)
        update_index_daily(filename, code)
    for code in tushare.index_daily_basic_codes:
        filename = gen_index_daily_basic_filename(code)
        update_index_daily_basic(filename, code)
