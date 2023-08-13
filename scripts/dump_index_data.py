#!/usr/bin/env python
# coding=utf-8

import logging
import os
import sys
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import requests
import mysql.connector


sys.path.append(os.getcwd())

from conf import index_conf
from conf import db_conf

lxr_token = os.getenv("LXR_TOKEN")
index_basic_table = "index_basic"
index_fundmental_table_tpl = "index_{}_fundmental"
index_percentile_fundmental_table_tpl = "index_{}_percentile_fundmental"


class Index:
    """用于获取原始数据
    """
    def __init__(self) -> None:
        self.conn = None

    def get_conn(self):
        if not self.conn:
            self.conn = mysql.connector.connect(
                host=db_conf.DB_HOST,
                port=db_conf.DB_PORT,
                user=db_conf.DB_USER,
                password=db_conf.DB_PASSWORD,
                database="finance_data"
            )
        return self.conn


    def close_coon(self):
        if self.conn:
            self.conn.close()
        return


    def init_index_basic_table(self):
        conn = self.get_conn()
        create_sql = """
            CREATE TABLE IF NOT EXISTS `{}` (
                `id` int NOT NULL AUTO_INCREMENT,
                `name` varchar(255) NOT NULL,
                `stockCode` varchar(50) NOT NULL,
                `areaCode` varchar(50) DEFAULT NULL,
                `market` varchar(50) DEFAULT NULL,
                `source` varchar(20) DEFAULT NULL,
                `currency` varchar(50) DEFAULT NULL,
                `series` varchar(20) DEFAULT NULL,
                `launchDate` date DEFAULT NULL,
                `rebalancingFrequency` varchar(20) DEFAULT NULL,
                `caculationMethod` varchar(20) DEFAULT NULL,
                `is_using` tinyint(1) NOT NULL DEFAULT 0,
                PRIMARY KEY (`id`),
                UNIQUE KEY `stockCode_UNIQUE` (`stockCode`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
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
        insert into `{}` (
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
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            stockCode = VALUES(stockCode),
            areaCode = VALUES(areaCode),
            market = VALUES(market), 
            source = VALUES(source), 
            currency = VALUES(currency), 
            series = VALUES(series), 
            launchDate = VALUES(launchDate), 
            rebalancingFrequency = VALUES(rebalancingFrequency), 
            caculationMethod = VALUES(caculationMethod); 
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
        sql = "select * from `{}` ".format(index_basic_table)
        res = pd.read_sql(sql, self.get_conn(), index_col="stockCode")
        return res


    def dump_all_history_index_data(self, codes: list):
        # 初始化表结构
        for code in codes:
            self.init_index_fundmental_table(code)
            self.init_index_percentile_fundmental_table(code)
        # 获取历史数据
        # 由于每个指数的指数基日不同，只能循环
        for code in codes:
            # 判断有没有初始化过
            is_using = self.get_using_flag_by_code(code)
            if is_using == 1:
                continue
            # 读取指数基日
            start, end = self.get_launchdate_by_code(code)
            # 获取全部历史数据，由于接口限制，一次只获取近10年的数据
            while start <= end:
                next_start = start + timedelta(days=3650)
                fundmental_data, percentile_data = self.get_index_fundmental_data([code], start, next_start, None)
                self.insert_index_fundmental(code, fundmental_data, percentile_data)
                start = next_start + timedelta(days=1)
            self.update_using_flag_by_code(code, 1)
        return


    def update_newest_index_data(self, codes: list):
        # 获取上次更新日期
        last_update_date = self.get_latest_index_update_date(codes[0])
        today = datetime.today().date()
        for d in [last_update_date, today]:
            fundmental_data, percentile_data = self.get_index_fundmental_data(codes, None, None, d)
            for code in codes:
                self.insert_index_fundmental(code, fundmental_data, percentile_data)
        return
            

    def get_launchdate_by_code(self, code):
        """获取指数基日"""
        df_index_basic = self.load_index_basic()
        start = df_index_basic.at[code, "launchDate"]
        end = datetime.today()
        return [start, end.date()]


    def get_latest_index_update_date(self, code: str):
        """找最近的更新日期，似乎没必要"""
        conn = self.get_conn()
        sql_tpl = "select date from {} order by date desc limit 1 "
        table_name = index_percentile_fundmental_table_tpl.format(code)
        cursor = conn.cursor()
        select_sql = sql_tpl.format(table_name, code)
        cursor.execute(select_sql)
        res = cursor.fetchone()
        if res:
            return res[0]
        return res

    
    def get_using_flag_by_code(self, code):
        df_index_basic = self.load_index_basic()
        is_using = df_index_basic.at[code, "is_using"]
        return is_using


    def update_using_flag_by_code(self, code, is_using):
        conn = self.get_conn()
        cursor = conn.cursor()
        update_sql = "update index_basic set is_using=%s where stockCode=%s"
        cursor.execute(update_sql, (is_using, code))
        conn.commit()
        cursor.close()
        return


    def init_index_fundmental_table(self, code: str):
        conn = self.get_conn()
        table_name = index_fundmental_table_tpl.format(code)
        create_sql = """
            CREATE TABLE IF NOT EXISTS `{}` (
            `id` int NOT NULL AUTO_INCREMENT,
            `stockCode` varchar(50) NOT NULL,
            `metrics_type`	varchar(10) NOT NULL,
            `date`	date DEFAULT NULL,
            `pe_ttm` DOUBLE,
            `pb`	DOUBLE,
            `ps_ttm`    DOUBLE,
            `dyr`	DOUBLE,
            `tv`	DOUBLE,
            `ta`	DOUBLE,
            `cp`	DOUBLE,
            `cpc`	DOUBLE,
            `r_cp`	DOUBLE,
            `r_cpc`	DOUBLE,
            `mc`	DOUBLE,
            `cmc`	DOUBLE,
            `fb`	DOUBLE,
            `sb`	DOUBLE,
            `ha_shm`	DOUBLE,
            PRIMARY KEY (`id`),
            UNIQUE KEY `stockCode_metricType_date_Unique` (`stockCode`,`metrics_type`, `date`)
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
            CREATE TABLE IF NOT EXISTS `{}` (
            `id` int NOT NULL AUTO_INCREMENT,
            `stockCode` varchar(50) NOT NULL,
            `metrics_name`	varchar(10) NOT NULL,
            `granularity`	varchar(10),
            `metrics_type`	varchar(10) NOT NULL,
            `date` date DEFAULT NULL,
            `cv`	DOUBLE,
            `cvpos`	DOUBLE,
            `minv`	DOUBLE,
            `maxv`	DOUBLE,
            `maxpv`	DOUBLE,
            `q5v`	DOUBLE,
            `q8v`	DOUBLE,
            `q2v`	DOUBLE,
            `avgv`	DOUBLE,
            PRIMARY KEY (`id`),
            UNIQUE KEY `stockCode_metricName_granularity_metricType_date_Unique` (`stockCode`, `metrics_name`,`granularity`,`metrics_type`, `date`)
        );
        """.format(table_name)
        cursor = conn.cursor()
        cursor.execute(create_sql)
        cursor.close()
        return


    def get_index_fundmental_data(self, codes: list, start, end, date):
        params = {
            "token": lxr_token,
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
        if len(codes) > 1:
            date = datetime.strftime(date, "%Y-%m-%d")
            params["date"] = date
        else:
            start = datetime.strftime(start, "%Y-%m-%d")
            end = datetime.strftime(end, "%Y-%m-%d")
            params["startDate"] = start
            params["endDate"] = end
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
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            pe_ttm = VALUES(pe_ttm), 
            pb = VALUES(pb), 
            ps_ttm = VALUES(ps_ttm), 
            dyr = VALUES(dyr), 
            tv = VALUES(tv), 
            ta = VALUES(ta), 
            cp = VALUES(cp), 
            cpc = VALUES(cpc), 
            r_cp = VALUES(r_cp), 
            r_cpc = VALUES(r_cpc), 
            mc = VALUES(mc), 
            cmc = VALUES(cmc), 
            fb = VALUES(fb), 
            sb = VALUES(sb), 
            ha_shm = VALUES(ha_shm)
        ;
        """.format(fundmental_table_name)
        percentile_table_name = index_percentile_fundmental_table_tpl.format(code)
        percentile_sql = """
        insert into {} 
            (stockCode, metrics_name, granularity, metrics_type, date, cv, cvpos, minv, maxv, maxpv, q5v, q8v, q2v, avgv) 
        values 
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            stockCode = VALUES(stockCode),
            metrics_name = VALUES(metrics_name),
            granularity = VALUES(granularity),
            metrics_type = VALUES(metrics_type),
            date = VALUES(date),
            cv = VALUES(cv),
            cvpos = VALUES(cvpos),
            minv = VALUES(minv),
            maxv = VALUES(maxv),
            maxpv = VALUES(maxpv),
            q5v = VALUES(q5v),
            q8v = VALUES(q8v),
            q2v = VALUES(q2v),
            avgv = VALUES(avgv)
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


if __name__ == "__main__":
    index = Index()
    codes = index_conf.index_list

    flag = sys.argv[1]
    if flag == "init":
        index.dump_all_history_index_data(codes)
    elif flag == "update":
        index.update_newest_index_data(codes)
        # print(index.get_latest_index_update_date(codes[0]))
    else:
        print("请输入参数：init/update")
    index.close_coon()
