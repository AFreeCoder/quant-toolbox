from datetime import datetime
import json
from functools import lru_cache

import pandas as pd
import numpy as np
import requests

base_host = "https://fundztapi.eastmoney.com"

fund_type_dic = {
    "all": 0,
    "equity": 25,
    "hybrid": 27,
    "bond": 31,
    "index": 11,
    "qdii": 37,
    "monetary": 35,
    "wealth-management": 36
}

@lru_cache(maxsize=8, typed=True)
def get_company_list_by_fund_type(date_str: str, fund_type: str):
    url = base_host + "/FundCompanyApi/CompanyApi2.ashx"
    header = {
        'content-type': 'application/json',
        "Accept-Language": "zh-Hans-CN;q=1, en-CN;q=0.9",
        'User-Agent': 'EMProjJijin/6.4.9 (iPhone; iOS 15.1; Scale/2.00)',
        "clientInfo": "ttjj-iPhone11,8-iOS-iOS15.1",
        "Referer": "https://mpservice.com/0fd9c439912651715e2620af0357f0e5/release/pages/index/index"
    }
    param = {
        "action": "fundcompanylist",
        "ftype": fund_type_dic[fund_type],
        "pi": 1,
        "ps": 500,
        "sd": "desc",
        "sf": "TotalScale"
    }
    r = requests.get(url=url, params=param, headers=header)
    resp = json.loads(r.text)
    df = pd.DataFrame(resp["data"])
    df.rename(columns={
        "companycode": "company_code",
        "estabdate": "establish_date",
        "fname": "fund_company",
        "sname": "short_name",
        "FundCount": "fund_count",
        "TotalScale": "scale",
        "RDate": "update_date"
    }, inplace=True)
    # 对异常值进行处理
    df.replace({'scale': {"--": 0}}, inplace=True)
    df.replace({'update_date': {"--": ""}}, inplace=True)
    df.replace({'fund_count': {"": 0}}, inplace=True)
    # 更改数据类型
    df["scale"] = df["scale"].astype(float)
    df["fund_count"] = df["fund_count"].astype(int)
    return df


def get_company_detail(orderby: str, orderdir: str):
    today = datetime.now().strftime("%Y-%m-%d")
    # 获取基金公司名单
    df = get_company_list_by_fund_type(today, "all")
    if not orderby:
        orderby = "scale"
    df = df.sort_values(by=orderby, ascending=(orderdir=="asc"), ignore_index=True)
    df.index = df.index + 1
    df.reset_index(inplace=True)
    return df.to_dict("records")
