
#!/usr/bin/env python
# coding=utf-8
"""
@Description: 用于获取源数据，目标是获取尽可能全的数据
@Date: 2021-09-16 00:50:22
@LastEditors: wanghaijie01
@LastEditTime: 2021-09-30 18:11:54
"""

import logging
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import requests

sys.path.append(os.getcwd())
from conf import lixingren

lxr_token = os.getenv("LXR_TOKEN")
index_basic_table = "index_basic"

def get_db_conn():
    conn = sqlite3.connect("data/index.db")
    return conn

def create_index_table_if_not_exist(conn: sqlite3.Connection, table: str):
    create_sql = """
        CREATE TABLE IF NOT EXISTS "{}" (
        "id"	INTEGER NOT NULL UNIQUE,
        "date"	NUMERIC NOT NULL,
        "pe_ttm"	REAL NOT NULL,
        "pb"	REAL NOT NULL,
        "ps_ttm"	REAL NOT NULL,
        "dyr"	REAL NOT NULL,
        "cp"	REAL NOT NULL,
        "r_cp"	REAL,
        "cpc"	REAL NOT NULL,
        "r_cpc"	REAL,
        "mc"	REAL NOT NULL,
        "cmc"	REAL NOT NULL,
        "fb"	REAL,
        "sb"	REAL,
        "ha_shm"	NUMERIC,
        "metric_type"	TEXT NOT NULL,
        PRIMARY KEY("id" AUTOINCREMENT),
        UNIQUE("date","metric_type")
    )
    """.format(table)
    cursor = conn.cursor()
    cursor.execute(create_sql)
    cursor.close()


def is_table_empty(conn: sqlite3.Connection, table: str):
    sql = "select id from '{}' limit 1".format(table)
    cursor = conn.cursor()
    cursor.execute(sql)
    res = cursor.fetchall()
    cursor.close()
    if len(res) == 0:
        return True
    return False


def get_index_basic(conn: sqlite3.Connection):
    sql = "select * from '{}' ".format(index_basic_table)
    res = pd.read_sql(sql, conn, index_col="stockCode", parse_dates="launchDate")
    return res

def gen_index_params():
    params = {
        "token": lxr_token,
        "metricsName": [
            "pe_ttm",
            "pb",
            "ps_ttm",
            "dyr",
            "cp",
            "r_cp",
            "cpc",
            "r_cpc",
            "mc",
            "cmc",
            "fb",
            "sb",
            "ha_shm",
        ],
        "metricsList": [
            "pe_ttm.mcw",
            "pe_ttm.ew",
            "pe_ttm.ewpvo",
            "pe_ttm.avg",
            "pe_ttm.median",
            "pb.mcw",
            "pb.ew",
            "pb.ewpvo",
            "pb.avg",
            "pb.median",
            "ps_ttm.mcw",
            "ps_ttm.ew",
            "ps_ttm.ewpvo",
            "ps_ttm.avg",
            "ps_ttm.median",
            "dyr.mcw",
            "dyr.ew",
            "dyr.ewpvo",
            "dyr.avg",
            "dyr.median",
            "cp",
            "r_cp",
            "cpc",
            "r_cpc",
            "mc",
            "cmc",
            "fb",
            "sb",
            "ha_shm",
        ]
    }
    return params

def get_history_index(code: str, start: str, end: str):
    url = "https://open.lixinger.com/api/a/index/fundamental"
    params = gen_index_params()
    params["startDate"] = start
    params["endDate"] = end
    params["stockCodes"] = [code]
    r = requests.post(url, json=params)
    res = r.json()
    data = []
    for item in res['data']:
        try:
            data.append([item["date"].split("T")[0], item["pe_ttm"]["mcw"], item["pb"]["mcw"], item["ps_ttm"]["mcw"], item["dyr"]["mcw"], item.get("cp", 0), item.get("r_cp", 0), item.get("cpc", 0), item.get("r_cpc", 0), item.get("mc", 0), item.get("cmc", 0), item.get("fb",0), item.get("sb",0), item.get("ha_shm", 0), "mcw"])
            data.append([item["date"].split("T")[0], item["pe_ttm"]["ew"], item["pb"]["ew"], item["ps_ttm"]["ew"], item["dyr"]["ew"], item.get("cp", 0), item.get("r_cp", 0), item.get("cpc", 0), item.get("r_cpc", 0), item.get("mc", 0), item.get("cmc", 0), item.get("fb",0), item.get("sb",0), item.get("ha_shm", 0), "ew"])
            data.append([item["date"].split("T")[0], item["pe_ttm"]["ewpvo"], item["pb"]["ewpvo"], item["ps_ttm"]["ewpvo"], item["dyr"]["ewpvo"], item.get("cp", 0), item.get("r_cp", 0), item.get("cpc", 0), item.get("r_cpc", 0), item.get("mc", 0), item.get("cmc", 0), item.get("fb",0), item.get("sb",0), item.get("ha_shm", 0), "ewpvo"])
            data.append([item["date"].split("T")[0], item["pe_ttm"]["avg"], item["pb"]["avg"], item["ps_ttm"]["avg"], item["dyr"]["avg"], item.get("cp", 0), item.get("r_cp", 0), item.get("cpc", 0), item.get("r_cpc", 0), item.get("mc", 0), item.get("cmc", 0), item.get("fb",0), item.get("sb",0), item.get("ha_shm", 0), "avg"])
            data.append([item["date"].split("T")[0], item["pe_ttm"]["median"], item["pb"]["median"], item["ps_ttm"]["median"], item["dyr"]["median"], item.get("cp", 0), item.get("r_cp", 0), item.get("cpc", 0), item.get("r_cpc", 0), item.get("mc", 0), item.get("cmc", 0), item.get("fb",0), item.get("sb",0), item.get("ha_shm", 0), "median"])
        except Exception as e:
            logging.warning("get_history_index failed:%s, ori_data:%s", e, item)
    data.sort(key=lambda x: x[0])
    return data
    

def batch_insert(conn: sqlite3.Connection, table: str, data: list):
    sql = "insert into " + table + " (date, pe_ttm, pb, ps_ttm, dyr, cp, r_cp, cpc, r_cpc, mc, cmc, fb, sb, ha_shm, metric_type) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
    cursor = conn.cursor()
    cursor.executemany(sql, data)
    conn.commit()
    cursor.close()
    return


def insert_history_data():
    conn = get_db_conn()
    index_basic = get_index_basic(conn)
    for code, _ in lixingren.index_conf.items():
        table = "index_" + code
        create_index_table_if_not_exist(conn, table)
        if not is_table_empty(conn, table):
            continue
        start = datetime.strftime(index_basic.at[code, "launchDate"], "%Y-%m-%d")
        end = datetime.strftime(datetime.today(), "%Y-%m-%d")
        history_data = get_history_index(code, start, end)
        batch_insert(conn, table, history_data)
    conn.close()


def get_latest_date(conn: sqlite3.Connection, table: str):
    sql = "select date from '{}' order by date desc limit 1".format(table)
    cursor = conn.cursor()
    cursor.execute(sql)
    res = cursor.fetchone()
    return datetime.strptime(res[0], "%Y-%m-%d")


def get_day_data(codes:List[str], date: str):
    url = "https://open.lixinger.com/api/a/index/fundamental"
    params = gen_index_params()
    params["date"] = date
    params["stockCodes"] = codes
    r = requests.post(url, json=params)
    res = r.json()
    data = defaultdict(list)
    for item in res['data']:
        try:
            code = item["stockCode"]
            data[code].append([item["date"].split("T")[0], item["pe_ttm"]["mcw"], item["pb"]["mcw"], item["ps_ttm"]["mcw"], item["dyr"]["mcw"], item["cp"], item.get("r_cp", 0), item.get("cpc", 0), item.get("r_cpc", 0), item["mc"], item["cmc"], item.get("fb",0), item.get("sb",0), item.get("ha_shm", 0), "mcw"])
            data[code].append([item["date"].split("T")[0], item["pe_ttm"]["ew"], item["pb"]["ew"], item["ps_ttm"]["ew"], item["dyr"]["ew"], item["cp"], item.get("r_cp", 0), item.get("cpc", 0), item.get("r_cpc", 0), item["mc"], item["cmc"], item.get("fb",0), item.get("sb",0), item.get("ha_shm", 0), "ew"])
            data[code].append([item["date"].split("T")[0], item["pe_ttm"]["ewpvo"], item["pb"]["ewpvo"], item["ps_ttm"]["ewpvo"], item["dyr"]["ewpvo"], item["cp"], item.get("r_cp", 0), item.get("cpc", 0), item.get("r_cpc", 0), item["mc"], item["cmc"], item.get("fb",0), item.get("sb",0), item.get("ha_shm", 0), "ewpvo"])
            data[code].append([item["date"].split("T")[0], item["pe_ttm"]["avg"], item["pb"]["avg"], item["ps_ttm"]["avg"], item["dyr"]["avg"], item["cp"], item.get("r_cp", 0), item.get("cpc", 0), item.get("r_cpc", 0), item["mc"], item["cmc"], item.get("fb",0), item.get("sb",0), item.get("ha_shm", 0), "avg"])
            data[code].append([item["date"].split("T")[0], item["pe_ttm"]["median"], item["pb"]["median"], item["ps_ttm"]["median"], item["dyr"]["median"], item["cp"], item.get("r_cp", 0), item.get("cpc", 0), item.get("r_cpc", 0), item["mc"], item["cmc"], item.get("fb",0), item.get("sb",0), item.get("ha_shm", 0), "median"])
        except Exception as e:
            logging.warning("get_day_data failed:%s", e)
    return data


def insert_by_date(conn: sqlite3.Connection, codes: List[str], date: str):
    day_data = get_day_data(codes, date)
    sql_tpl = "insert into '{}' (date, pe_ttm, pb, ps_ttm, dyr, cp, r_cp, cpc, r_cpc, mc, cmc, fb, sb, ha_shm, metric_type) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
    cursor = conn.cursor()
    for code in codes:
        table = "index_" + code
        sql = sql_tpl.format(table)
        if code in day_data:
            try:
                cursor.executemany(sql, day_data[code])
                conn.commit()
            except Exception as e:
                logging.warning("insert_by_date err: %s", e)
    cursor.close()
        

def update_latest_data(date=""):
    conn = get_db_conn()
    # 找最远的一个日期
    left = datetime.today()
    right = datetime.today()
    for code, _ in lixingren.index_conf.items():
        table = "index_" + code
        res = get_latest_date(conn, table) + timedelta(days=1)
        if left > res:
            left = res
    codes = list(lixingren.index_conf.keys())
    while left <= right:
        date = datetime.strftime(left, "%Y-%m-%d")
        # 跳过周六日 isoweekday())
        if left.isoweekday() not in [6,7]:
            insert_by_date(conn, codes, date)
            # pass
        print(left, left.isoweekday())
        left += timedelta(days=1)
    conn.close()


if __name__ == "__main__":
    insert_history_data()
    update_latest_data()
