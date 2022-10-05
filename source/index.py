#!/usr/bin/env python
# coding=utf-8

import logging
import os
import sqlite3
import sys
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import requests


sys.path.append(os.getcwd())

from conf import lixingren

lxr_token = os.getenv("LXR_TOKEN")
index_basic_table = "index_basic"
index_fundmental_table_tpl = "index_{}_fundmental"
index_percentile_fundmental_table_tpl = "index_{}_percentile_fundmental"

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
    """用于获取原始数据
    """
    def __init__(self) -> None:
        self.conn = None

    def get_conn(self):
        if not self.conn:
            self.conn = sqlite3.connect("data/index.db")
        return self.conn

    def init_index_basic_table(self):
        conn = self.get_conn()
        create_sql = """
            CREATE TABLE IF NOT EXISTS "{}" (
                "id"	INTEGER NOT NULL,
                "name"	TEXT NOT NULL,
                "stockCode"	TEXT NOT NULL UNIQUE,
                "areaCode"	TEXT,
                "market"	TEXT,
                "source"	TEXT,
                "currency"	TEXT,
                "series"	TEXT,
                "launchDate"	TEXT,
                "rebalancingFrequency"	TEXT,
                "caculationMethod"	TEXT,
                PRIMARY KEY("id" AUTOINCREMENT)
            );
        """.format(index_basic_table)
        cursor = conn.cursor()
        cursor.execute(create_sql)
        cursor.close()
        return

    def get_index_basic_data(self):
        url = "https://open.lixinger.com/api/cn/index"
        params = {
            "token": lxr_token,
        }
        r = requests.post(url, json=params)
        res = r.json()
        data = res["data"]
        return data

    def insert_index_basic(self, data: List[dict]):
        insert_sql = """
        insert into "{}" (
            name, 
            stockCode, 
            areaCode, 
            market, 
            source, 
            currency, 
            series, 
            launchDate, 
            rebalancingFrequency, 
            caculationMethod) 
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
        on conflict(stockCode) do update
        set 
            name = excluded.name,
            stockCode = excluded.stockCode,
            areaCode = excluded.areaCode,
            market = excluded.market, 
            source = excluded.source, 
            currency = excluded.currency, 
            series = excluded.series, 
            launchDate = excluded.launchDate, 
            rebalancingFrequency = excluded.rebalancingFrequency, 
            caculationMethod = excluded.caculationMethod; 
        """.format(index_basic_table)
        conn = self.get_conn()
        cursor = conn.cursor()
        index_list = []
        for item in data:
            # 发布日期是标准时间，需要转成北京时间
            launchDate = datetime.strptime(item["launchDate"], "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(hours=8)
            item["launchDate"] = launchDate.strftime("%Y-%m-%d")
            index_list.append([
                item.get("name", ""),
                item.get("stockCode", ""),
                item.get("areaCode", ""),
                item.get("market", ""),
                item.get("source", ""),
                item.get("currency", ""),
                item.get("series", ""),
                item.get("launchDate", ""),
                item.get("rebalancingFrequency", ""),
                item.get("caculationMethod", "")
            ])
        try:
            cursor.executemany(insert_sql, index_list)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.warning("insert index basic err:%s ", e)
        cursor.close()
        return

    def dump_index_basic(self):
        self.init_index_basic_table()
        data = self.get_index_basic_data()
        self.insert_index_basic(data)


    def load_index_basic(self):
        sql = "select * from '{}' ".format(index_basic_table)
        res = pd.read_sql(sql, self.get_conn(), index_col="stockCode")
        return res


    def init_index_fundmental_table(self, code: str):
        conn = self.get_conn()
        table_name = index_fundmental_table_tpl.format(code)
        create_sql = """
            CREATE TABLE IF NOT EXISTS "{}" (
            "id"	INTEGER NOT NULL,
            "stockCode"	TEXT NOT NULL,
            "metrics_type"	TEXT,
            "date"	TEXT,
            "pe_ttm"	REAL,
            "pb"	REAL,
            "ps_ttm"	REAL,
            "dyr"	REAL,
            "tv"	REAL,
            "ta"	REAL,
            "cp"	REAL,
            "cpc"	REAL,
            "r_cp"	REAL,
            "r_cpc"	REAL,
            "mc"	REAL,
            "cmc"	REAL,
            "fb"	REAL,
            "sb"	REAL,
            "ha_shm"	REAL,
            PRIMARY KEY("id" AUTOINCREMENT),
            UNIQUE("stockCode","metrics_type", "date")
        );
        """.format(table_name)
        cursor = conn.cursor()
        cursor.execute(create_sql)
        cursor.close()
        return


    def init_index_percentile_fundmental_table(self, code: str):
        conn = self.get_conn()
        table_name = index_percentile_fundmental_table_tpl.format(code)
        create_sql = """
            CREATE TABLE IF NOT EXISTS "{}" (
            "id"	INTEGER NOT NULL,
            "stockCode" TEXT NOT NULL,
            "metrics_name"	TEXT NOT NULL,
            "granularity"	TEXT NOT NULL,
            "metrics_type"	TEXT NOT NULL,
            "date" TEXT NOT NULL,
            "cv"	REAL,
            "cvpos"	REAL,
            "minv"	REAL,
            "maxv"	REAL,
            "maxpv"	REAL,
            "q5v"	REAL,
            "q8v"	REAL,
            "q2v"	REAL,
            "avgv"	REAL,
            UNIQUE("stockCode", "metrics_name","granularity","metrics_type", "date"),
            PRIMARY KEY("id" AUTOINCREMENT)
        );
        """.format(table_name)
        cursor = conn.cursor()
        cursor.execute(create_sql)
        cursor.close()
        return

    
    def get_index_percentile_fundmental_by_date(self, code: str, metrics_name: str, granularity: str, metrics_type: str, date: str):
        conn = self.get_conn()
        table_name = index_percentile_fundmental_table_tpl.format(code)
        sql = """
            select date, cv, cvpos, minv, maxv, maxpv, q5v, q8v, q2v, avgv from {} 
            where 
            stockCode = "{}" 
            and metrics_name = "{}" 
            and granularity = "{}" 
            and metrics_type = "{}" 
            """.format(table_name, code, metrics_name, granularity, metrics_type)
        if date and len(date) > 0:
            sql += (" and date='{}' ".format(date))
        sql += "order by date desc limit 1"
        cursor = conn.cursor()
        cursor.execute(sql)
        res = cursor.fetchone()
        if len(res) == 0:
            return {}
        return {
            "date": res[0],
            "cv": res[1],
            "cvpos": res[2],
            "minv": res[3],
            "maxv": res[4],
            "maxpv": res[5],
            "q5v": res[6],
            "q8v": res[7],
            "q2v": res[8],
            "avgv": res[9],
        }


    def get_latest_index_fundmental_percentile(self, code: str, metrics_name: str, metrics_type: str, granularity: str):
        conn = self.get_conn()
        sql_tpl = """
            select * from {} 
            where 
            stockCode = "{}" 
            and metrics_name = "{}" 
            and granularity = "{}" 
            and metrics_type = "{}" 
            order by date desc limit 1
            """
        table_name = index_percentile_fundmental_table_tpl.format(code)
        cursor = conn.cursor()
        select_sql = sql_tpl.format(table_name, code, metrics_name, granularity, metrics_type)
        cursor.execute(select_sql)
        res = cursor.fetchone()
        return res


    def load_index_fundmental_percentile(self, code: str, metrics_name: str, metrics_type: str, granularity: str):
        table_name = index_percentile_fundmental_table_tpl.format(code)
        sql = """
            select cvpos, date from '{}' where metrics_name="{}" and metrics_type="{}" and granularity="{}" order by date asc
        """.format(table_name, metrics_name, metrics_type, granularity)
        df = pd.read_sql(sql, self.get_conn())
        return df


    def load_index_fundmental(self, code: str, metrics_type: str):
        table_name = index_fundmental_table_tpl.format(code)
        sql = """
            select pe_ttm, pb, cp, date from '{}' where metrics_type="{}" order by date asc
        """.format(table_name, metrics_type)
        df = pd.read_sql(sql, self.get_conn())
        return df


    def get_index_fundmental_data(self, codes: list, start, end):
        start = datetime.strftime(start, "%Y-%m-%d")
        end = datetime.strftime(end, "%Y-%m-%d")
        params = {
            "token": lxr_token,
            "startDate": start,
            "endDate": end,
            "stockCodes": codes,
            "metricsList": [
                "pe_ttm.mcw",
                "pe_ttm.ew",
                "pe_ttm.ewpvo",
                "pe_ttm.avg",
                "pe_ttm.median",
                "pe_ttm.fs.mcw",
                "pe_ttm.fs.ew",
                "pe_ttm.fs.ewpvo",
                "pe_ttm.fs.avg",
                "pe_ttm.fs.median",
                "pe_ttm.y20.mcw",
                "pe_ttm.y20.ew",
                "pe_ttm.y20.ewpvo",
                "pe_ttm.y20.avg",
                "pe_ttm.y20.median",
                "pe_ttm.y10.mcw",
                "pe_ttm.y10.ew",
                "pe_ttm.y10.ewpvo",
                "pe_ttm.y10.avg",
                "pe_ttm.y10.median",
                "pe_ttm.y5.mcw",
                "pe_ttm.y5.ew",
                "pe_ttm.y5.ewpvo",
                "pe_ttm.y5.avg",
                "pe_ttm.y5.median",
                "pe_ttm.y3.mcw",
                "pe_ttm.y3.ew",
                "pe_ttm.y3.ewpvo",
                "pe_ttm.y3.avg",
                "pe_ttm.y3.median",
                "pb.mcw",
                "pb.ew",
                "pb.ewpvo",
                "pb.avg",
                "pb.median",
                "pb.fs.mcw",
                "pb.fs.ew",
                "pb.fs.ewpvo",
                "pb.fs.avg",
                "pb.fs.median",
                "pb.y20.mcw",
                "pb.y20.ew",
                "pb.y20.ewpvo",
                "pb.y20.avg",
                "pb.y20.median",
                "pb.y10.mcw",
                "pb.y10.ew",
                "pb.y10.ewpvo",
                "pb.y10.avg",
                "pb.y10.median",
                "pb.y5.mcw",
                "pb.y5.ew",
                "pb.y5.ewpvo",
                "pb.y5.avg",
                "pb.y5.median",
                "pb.y3.mcw",
                "pb.y3.ew",
                "pb.y3.ewpvo",
                "pb.y3.avg",
                "pb.y3.median",
                "ps_ttm.mcw",
                "ps_ttm.ew",
                "ps_ttm.ewpvo",
                "ps_ttm.avg",
                "ps_ttm.median",
                "ps_ttm.fs.mcw",
                "ps_ttm.fs.ew",
                "ps_ttm.fs.ewpvo",
                "ps_ttm.fs.avg",
                "ps_ttm.fs.median",
                "ps_ttm.y20.mcw",
                "ps_ttm.y20.ew",
                "ps_ttm.y20.ewpvo",
                "ps_ttm.y20.avg",
                "ps_ttm.y20.median",
                "ps_ttm.y10.mcw",
                "ps_ttm.y10.ew",
                "ps_ttm.y10.ewpvo",
                "ps_ttm.y10.avg",
                "ps_ttm.y10.median",
                "ps_ttm.y5.mcw",
                "ps_ttm.y5.ew",
                "ps_ttm.y5.ewpvo",
                "ps_ttm.y5.avg",
                "ps_ttm.y5.median",
                "ps_ttm.y3.mcw",
                "ps_ttm.y3.ew",
                "ps_ttm.y3.ewpvo",
                "ps_ttm.y3.avg",
                "ps_ttm.y3.median",
                "dyr.mcw",
                "dyr.ew",
                "dyr.ewpvo",
                "dyr.avg",
                "dyr.median",
                "tv",
                "ta",
                "cp",
                "cpc",
                "r_cp",
                "r_cpc",
                "mc",
                "cmc",
                "fb",
                "sb",
                "ha_shm",
            ]
        }
        url = "https://open.lixinger.com/api/cn/index/fundamental"
        r = requests.post(url, json=params)
        res = r.json()
        data = []
        percentile_data = []
        for item in res["data"]:
            try:
                item["date"] = item["date"].replace('+08:00','')
                item["date"] = item["date"].split("T")[0]
                ## 基本面数据
                for mtype in ["mcw", "ew", "ewpvo", "avg", "median"]:
                    data.append([item["stockCode"], mtype, item["date"], item["pe_ttm"][mtype], item["pb"][mtype], item["ps_ttm"][mtype], item["dyr"][mtype], item.get("tv", 0), item.get("ta", 0), item.get("cp", 0), item.get("cpc", 0), item.get("r_cp", 0), item.get("r_cpc", 0), item.get("mc", 0), item.get("cmc", 0), item.get("fb",0), item.get("sb",0), item.get("ha_shm", 0)])

                ## 分位值数据
                for mname in ["pe_ttm", "pb", "ps_ttm"]:
                    for gap in ["fs", "y20", "y10", "y5", "y3"]:
                        for mtype in ["mcw", "ew", "ewpvo", "avg", "median"]: 
                            percentile_data.append([
                                item["stockCode"],
                                mname,
                                gap,
                                mtype,
                                item["date"],
                                item[mname][gap][mtype].get("cv", 0),
                                item[mname][gap][mtype].get("cvpos", 0),
                                item[mname][gap][mtype].get("minv", 0),
                                item[mname][gap][mtype].get("maxv", 0),
                                item[mname][gap][mtype].get("maxpv", 0),
                                item[mname][gap][mtype].get("q5v", 0),
                                item[mname][gap][mtype].get("q8v", 0),
                                item[mname][gap][mtype].get("q2v", 0),
                                item[mname][gap][mtype].get("avgv", 0),
                                ]
                            )
            except Exception as e:
                logging.warning("get_index_%s_fundmental_data failed:%s, ori_data:%s", item["stockCode"], e, item)
        data.sort(key=lambda x: x[2])
        percentile_data.sort(key=lambda x: x[4])
        return data, percentile_data


    def insert_index_fundmental(self, code: str, fundmental_data: list, percentile_data: list):
        fundmental_data = list(filter(lambda x: x[0] == code, fundmental_data))
        percentile_data = list(filter(lambda x: x[0] == code, percentile_data))
        fundmental_table_name = index_fundmental_table_tpl.format(code)
        fundmental_sql = """
        insert into {} 
            (stockCode, metrics_type, date, pe_ttm, pb, ps_ttm, dyr, tv, ta, cp, cpc, r_cp, r_cpc, mc, cmc, fb, sb, ha_shm) 
        values 
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict(stockCode, metrics_type, date) do update
        set
            pe_ttm = excluded.pe_ttm, 
            pb = excluded.pb, 
            ps_ttm = excluded.ps_ttm, 
            dyr = excluded.dyr, 
            tv = excluded.tv, 
            ta = excluded.ta, 
            cp = excluded.cp, 
            cpc = excluded.cpc, 
            r_cp = excluded.r_cp, 
            r_cpc = excluded.r_cpc, 
            mc = excluded.mc, 
            cmc = excluded.cmc, 
            fb = excluded.fb, 
            sb = excluded.sb, 
            ha_shm = excluded.ha_shm
        ;
        """.format(fundmental_table_name)
        percentile_table_name = index_percentile_fundmental_table_tpl.format(code)
        percentile_sql = """
        insert into {} 
            (stockCode, metrics_name, granularity, metrics_type, date, cv, cvpos, minv, maxv, maxpv, q5v, q8v, q2v, avgv) 
        values 
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict(stockCode, metrics_name, granularity, metrics_type, date) do update
        set
            stockCode = excluded.stockCode,
            metrics_name = excluded.metrics_name,
            granularity = excluded.granularity,
            metrics_type = excluded.metrics_type,
            date = excluded.date,
            cv = excluded.cv,
            cvpos = excluded.cvpos,
            minv = excluded.minv,
            maxv = excluded.maxv,
            maxpv = excluded.maxpv,
            q5v = excluded.q5v,
            q8v = excluded.q8v,
            q2v = excluded.q2v,
            avgv = excluded.avgv
        ;
        """.format(percentile_table_name)
        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.executemany(fundmental_sql, fundmental_data)
            cursor.executemany(percentile_sql, percentile_data)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.warning("insert index %s fundmental failed:%s", code, e)
        cursor.close()
        return


    def get_fundmental_update_interval_by_index(self, code: str):
        sql = "select date from '{}' order by date desc limit 1".format(index_fundmental_table_tpl.format(code))
        cursor = self.get_conn().cursor()
        cursor.execute(sql)
        res = cursor.fetchone()
        start = datetime.today()
        end = datetime.today()
        if res:
            startStr = res[0]
            start = datetime.strptime(startStr, "%Y-%m-%d") + timedelta(days=1)
        else:
            df_index_basic = self.load_index_basic()
            start = datetime.strptime(df_index_basic.at[code, "launchDate"], "%Y-%m-%d")
        return [start, end]


    def insert_or_update_index_fundmental(self, code:str):
        self.init_index_fundmental_table(code)
        self.init_index_percentile_fundmental_table(code)
        start, end = self.get_fundmental_update_interval_by_index(code)
        ## 因为接口中部分字段数据更新延迟，故start选取2天之前的日期，用于更新历史数据
        start = start + timedelta(days=-2)
        while start <= end:
            next_start = start + timedelta(days=3650)
            fundmental_data, percentile_data = self.get_index_fundmental_data([code], start, next_start)
            self.insert_index_fundmental(code, fundmental_data, percentile_data)
            start = next_start + timedelta(days=1)
        return


    def init_insert_or_update_index_fundmental(self, codes: list):
        for code in codes:
            self.insert_or_update_index_fundmental(code)
        return



if __name__ == "__main__":
    index = Index()
    # index.dump_index_basic()
    # codes = ["000300", "000905"]
    codes = lixingren.index_list
    index.init_insert_or_update_index_fundmental(codes)

    
        