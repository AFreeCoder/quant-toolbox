from cmath import nan
from dataclasses import replace
import requests
import json
from functools import lru_cache
from datetime import datetime

import numpy as np
import pandas as pd

from . import company

base_host = "http://fund.eastmoney.com"
headers = {'content-type': 'application/json',
           "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
           'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36 Edg/95.0.1020.30',
           "Referer": "https://mpservice.com/0fd9c439912651715e2620af0357f0e5/release/pages/index/index"
          }


@lru_cache(maxsize=2048, typed=True)
def get_yield_by_code(date_str: str, company_code: str, fund_type: str):
    url = base_host + "/Company/home/GetSygmChart"
    header = {
        'content-type': 'application/json',
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.54 Safari/537.36 Edg/95.0.1020.30',
        "Referer": "https://mpservice.com/0fd9c439912651715e2620af0357f0e5/release/pages/index/index"
    }
    param = {
        "gsid": company_code,
        "fundType": company.fund_type_dic[fund_type]
    }
    r = requests.get(url=url, params=param, headers=header)
    resp = json.loads(r.text)
    # 返回值含义
    #   6月   1年    3年    5年
    # [[0.14, 0.14, 0.65, 4],   本公司平均
    # [0.53, 1.22, 0.65, 4],    同类平均
    # [2.35, 4.09, 12.67, 19.35]]   比较基准
    # company_yield = {
    #     "avg_six_month": resp[0][0],
    #     "avg_one_year": resp[0][1],
    #     "avg_three_year": resp[0][2],
    #     "avg_five_year": resp[0][3]
    # }

    return resp


def get_yield_info(fund_type: str, orderby: str, orderdir: str):
    today = datetime.now().strftime("%Y-%m-%d")
    # 获取基金公司名单
    df_company = company.get_company_list_by_fund_type(today, "all")
    yield_infos = []
    for company_code in df_company["company_code"]:
        yield_info = get_yield_by_code(today, company_code, fund_type)
        if len(yield_info) == 0:
            continue
        item = {
            "company_code": company_code,
            "avg_six_month": yield_info[0][0],
            "avg_one_year": yield_info[0][1],
            "avg_three_year": yield_info[0][2],
            "avg_five_year": yield_info[0][3]
        }
        yield_infos.append(item)
    df = pd.DataFrame(yield_infos)
    df = pd.merge(df, df_company[["company_code", "fund_company"]], how="left", on="company_code")
    df.fillna("-", inplace=True)
    if not orderby:
        orderby = "avg_five_year"
    df = df[df[orderby] != "-"]
    df.sort_values(by=orderby, ascending=(orderdir=="asc"), inplace=True, ignore_index=True)
    df.index = df.index + 1
    df.reset_index(inplace=True)
    return df.to_dict("records")