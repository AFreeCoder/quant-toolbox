from curses.ascii import isdigit
import requests
import json
from functools import lru_cache
from datetime import datetime

import pandas as pd

from . import company

base_host = "http://fund.eastmoney.com"

url = "/Company/f10/jjjl_80000229.html"

@lru_cache(maxsize=256, typed=True)
def get_work_year_by_code(date_str: str, company_code: str):
    url = (base_host + "/Company/f10/jjjl_{}.html").format(company_code)
    r = requests.get(url=url)
    df = pd.read_html(r.text)
    df = df[1]
    year, rank = df["本公司"].iloc[1], df["公司排名"].iloc[1]
    rank = rank.split("|")[0]
    return [year, rank]


def get_work_year_info(orderdir: str):
    today = datetime.now().strftime("%Y-%m-%d")
    # 获取基金公司名单
    df_company = company.get_company_list_by_fund_type(today, "all")
    work_year_infos = []
    for company_code in df_company["company_code"]:
        info = get_work_year_by_code(today, company_code)
        if not info[1].isdigit():
            continue
        work_year_infos.append({
            "company_code": company_code,
            "work_year": info[0],
            "rank": int(info[1]),
        })
    df = pd.DataFrame(work_year_infos)
    df = pd.merge(df, df_company, how="left", on="company_code")
    df.sort_values(by="rank", ascending=(orderdir=="desc"), inplace=True, ignore_index=True)
    df.index = df.index + 1
    df.reset_index(inplace=True)
    return df.to_dict("records")
