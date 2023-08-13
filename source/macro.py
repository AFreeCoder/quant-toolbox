#!/usr/bin/env python
# coding=utf-8

import os
import sys
import logging
from typing import List
from datetime import datetime, timedelta

import requests
import pandas as pd
import mysql.connector

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


    def load_national_debt_data(self, area_code: str, rows: list):
        sql = "select {} from `{}` where areaCode='{}'".format(",".join(rows), national_debt_table, area_code)
        df = pd.read_sql(sql, self.get_conn())
        return df


if __name__ == "__main__":
    macro = Macro()
    macro.insert_or_update_national_debt()