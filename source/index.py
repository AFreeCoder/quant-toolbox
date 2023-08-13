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

from conf import db_conf

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


    def load_index_basic(self):
        sql = "select * from `{}` ".format(index_basic_table)
        res = pd.read_sql(sql, self.get_conn(), index_col="stockCode")
        return res


    def get_latest_index_fundmental_percentile(self, code: str, metrics_name: str, metrics_type: str, granularity: str):
        conn = self.get_conn()
        sql_tpl = """
            select * from `{}` 
            where 
            stockCode = "{}" 
            and metrics_name = "{}" 
            and granularity = "{}" 
            and metrics_type = "{}" 
            order by `date` desc limit 1
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
            select cvpos, `date` from `{}` where metrics_name="{}" and metrics_type="{}" and granularity="{}" order by `date` asc
        """.format(table_name, metrics_name, metrics_type, granularity)
        df = pd.read_sql(sql, self.get_conn())
        return df


    def load_index_fundmental(self, code: str, metrics_type: str):
        table_name = index_fundmental_table_tpl.format(code)
        sql = """
            select pe_ttm, pb, cp, `date` from `{}` where metrics_type="{}" order by `date` asc
        """.format(table_name, metrics_type)
        df = pd.read_sql(sql, self.get_conn())
        return df


if __name__ == "__main__":
    pass

    
        