#!/usr/bin/env python
# coding=utf-8

import os
import logging
from typing import List
from datetime import datetime, timedelta

import requests
import mysql.connector
import pandas as pd

sys.path.append(os.getcwd())

from conf import db_conf

lxr_token = os.getenv("LXR_TOKEN")
national_debt_table = "national_debt"

class Macro:
    """用于获取宏观数据
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
        if self.conn is not None:
            self.conn.close()
        return


    def init_national_debt_table(self):
        conn = self.get_conn()
        create_sql = """
            CREATE TABLE IF NOT EXISTS `{}` (
                `id` INT AUTO_INCREMENT,
                `areaCode` VARCHAR(255),
                `date` VARCHAR(255),
                `tcm_m1` DOUBLE,
                `tcm_m3` DOUBLE,
                `tcm_m6` DOUBLE,
                `tcm_y1` DOUBLE,
                `tcm_y2` DOUBLE,
                `tcm_y3` DOUBLE,
                `tcm_y5` DOUBLE,
                `tcm_y7` DOUBLE,
                `tcm_y10` DOUBLE,
                `tcm_y20` DOUBLE,
                `tcm_y30` DOUBLE,
                PRIMARY KEY (`id`),
                UNIQUE KEY (`areaCode`, `date`)
            )ENGINE=InnoDB DEFAULT CHARSET=utf8;
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
                "tcm_m1",
                "tcm_m3",
                "tcm_m6",
                "tcm_y1",
                "tcm_y2",
                "tcm_y3",
                "tcm_y5",
                "tcm_y7",
                "tcm_y10",
                "tcm_y20",
                "tcm_y30"
            ]
        }
        r = requests.post(url, json=params)
        res = r.json()
        data = res["data"]
        return data


    def insert_national_debt(self, data: List[dict]):
        insert_sql = """
        insert into `{}` (
            areaCode,
            date,
            tcm_m1,
            tcm_m3,
            tcm_m6,
            tcm_y1,
            tcm_y2,
            tcm_y3,
            tcm_y5,
            tcm_y7,
            tcm_y10,
            tcm_y20,
            tcm_y30) 
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        ON DUPLICATE KEY UPDATE
            tcm_m1 = VALUES(tcm_m1),
            tcm_m3 = VALUES(tcm_m3),
            tcm_m6 = VALUES(tcm_m6),
            tcm_y1 = VALUES(tcm_y1),
            tcm_y2 = VALUES(tcm_y2),
            tcm_y3 = VALUES(tcm_y3),
            tcm_y5 = VALUES(tcm_y5),
            tcm_y7 = VALUES(tcm_y7),
            tcm_y10 = VALUES(tcm_y10),
            tcm_y20 = VALUES(tcm_y20),
            tcm_y30 = VALUES(tcm_y30)
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
                item.get("tcm_m1", 0),
                item.get("tcm_m3", 0),
                item.get("tcm_m6", 0),
                item.get("tcm_y1", 0),
                item.get("tcm_y2", 0),
                item.get("tcm_y3", 0),
                item.get("tcm_y5", 0),
                item.get("tcm_y7", 0),
                item.get("tcm_y10", 0),
                item.get("tcm_y20", 0),
                item.get("tcm_y30", 0),
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
        sql = "select `date` from `{}` order by `date` desc limit 1".format(table)
        print(sql)
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
    macro.close_coon()