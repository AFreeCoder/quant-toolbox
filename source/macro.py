#!/usr/bin/env python
# coding=utf-8

import os
import logging
from typing import List
from datetime import datetime, timedelta

import requests
import sqlite3
import pandas as pd

lxr_token = os.getenv("LXR_TOKEN")
national_debt_table = "national_debt"

class Macro:
    """用于获取宏观数据
    """
    def __init__(self) -> None:
        self.conn = None

    
    def get_conn(self):
        if not self.conn:
            self.conn = sqlite3.connect("data/index.db")
        return self.conn


    def init_national_debt_table(self):
        conn = self.get_conn()
        create_sql = """
            CREATE TABLE IF NOT EXISTS "{}" (
                "id"	INTEGER,
                "areaCode"	TEXT,
                "date"	TEXT,
                "mir_m1"	NUMERIC,
                "mir_m3"	NUMERIC,
                "mir_m6"	NUMERIC,
                "mir_y1"	NUMERIC,
                "mir_y2"	NUMERIC,
                "mir_y3"	NUMERIC,
                "mir_y5"	NUMERIC,
                "mir_y7"	NUMERIC,
                "mir_y10"	NUMERIC,
                "mir_y20"	NUMERIC,
                "mir_y30"	NUMERIC,
                PRIMARY KEY("id" AUTOINCREMENT),
                UNIQUE("areaCode","date")
            );
        """.format(national_debt_table)
        cursor = conn.cursor()
        cursor.execute(create_sql)
        cursor.close()
        return


    def get_national_debt_data(self, area_code: str, start, end):
        start = datetime.strftime(start, "%Y-%m-%d")
        end = datetime.strftime(end, "%Y-%m-%d")
        url = "https://open.lixinger.com/api/macro/national-debt"
        params = {
            "token": lxr_token,
            "areaCode": area_code,
            "startDate": start,
            "endDate": end,
            "metricsList": [
                "mir_m1",
                "mir_m3",
                "mir_m6",
                "mir_y1",
                "mir_y2",
                "mir_y3",
                "mir_y5",
                "mir_y7",
                "mir_y10",
                "mir_y20",
                "mir_y30"
            ]
        }
        r = requests.post(url, json=params)
        res = r.json()
        data = res["data"]
        return data


    def insert_national_debt(self, data: List[dict]):
        insert_sql = """
        insert into "{}" (
            areaCode,
            date,
            mir_m1,
            mir_m3,
            mir_m6,
            mir_y1,
            mir_y2,
            mir_y3,
            mir_y5,
            mir_y7,
            mir_y10,
            mir_y20,
            mir_y30) 
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
        on conflict(areaCode,date) do update
        set 
            mir_m1 = excluded.mir_m1,
            mir_m3 = excluded.mir_m3,
            mir_m6 = excluded.mir_m6,
            mir_y1 = excluded.mir_y1,
            mir_y2 = excluded.mir_y2,
            mir_y3 = excluded.mir_y3,
            mir_y5 = excluded.mir_y5,
            mir_y7 = excluded.mir_y7,
            mir_y10 = excluded.mir_y10,
            mir_y20 = excluded.mir_y20,
            mir_y30 = excluded.mir_y30
        ; 
        """.format(national_debt_table)
        conn = self.get_conn()
        cursor = conn.cursor()
        index_list = []
        for item in data:
            areaCode = item["areaCode"]
            item["date"] = item["date"].split("T")[0]
            index_list.append([
                areaCode,
                item["date"],
                item.get("mir_m1", 0),
                item.get("mir_m3", 0),
                item.get("mir_m6", 0),
                item.get("mir_y1", 0),
                item.get("mir_y2", 0),
                item.get("mir_y3", 0),
                item.get("mir_y5", 0),
                item.get("mir_y7", 0),
                item.get("mir_y10", 0),
                item.get("mir_y20", 0),
                item.get("mir_y30", 0),
            ])
        try:
            cursor.executemany(insert_sql, index_list)
            conn.commit()
        except Exception as e:
            conn.rollback()
            logging.warning("insert national debt err:%s ", e)
        cursor.close()
        return


    def get_latest_update_date(self, table: str):
        sql = "select date from '{}' order by date desc limit 1".format(table)
        cursor = self.get_conn().cursor()
        cursor.execute(sql)
        res = cursor.fetchone()
        if res:
            start = res[0]
            start = datetime.strptime(start, "%Y-%m-%d") + timedelta(days=1)
            return start
        return datetime.strptime("1990-01-01", "%Y-%m-%d")


    def load_national_debt_data(self, area_code: str, rows: list):
        sql = "select {} from '{}' where areaCode='{}'".format(",".join(rows), national_debt_table, area_code)
        df = pd.read_sql(sql, self.get_conn())
        return df

    
    def insert_or_update_national_debt(self):
        self.init_national_debt_table()
        start = self.get_latest_update_date(national_debt_table)
        end = datetime.today()
        # 暂时只获取中债数据
        area_code = "cn"
        while start <= end:
            next_start = start + timedelta(days=3650)
            national_debt_data = self.get_national_debt_data(area_code, start, next_start)
            self.insert_national_debt(national_debt_data)
            start = next_start + timedelta(days=1)
        return

if __name__ == "__main__":
    macro = Macro()
    macro.insert_or_update_national_debt()