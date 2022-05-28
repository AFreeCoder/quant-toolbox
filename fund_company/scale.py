import json

from datetime import date, datetime
from functools import lru_cache
import pandas as pd
import numpy as np
import requests


base_host = "https://fundztapi.eastmoney.com"
scale_base_url = "http://fund.eastmoney.com/Company/home/gspmlist?"
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

@lru_cache(maxsize=32, typed=True)
def get_origin_scale_info(date_str: str, fund_type: str):
    param = {
        "fundType": fund_type_dic[fund_type]
    }
    r = requests.get(url=scale_base_url, params=param)
    df = pd.read_html(r.text, index_col="序号", encoding="gbk")[0]
    df.columns = ["fund_company", "relate_link", "establish_date", "evaluate", "scale", "fund_count", "fund_manager_count"]
    df.drop(columns=["relate_link"], inplace=True)
    # 对管理规模一列字符串处理
    df["scale"] = df["scale"].str.split(" ").str[0]
    # 部分基金公司规模为 - ，替换成 0
    df.replace({'scale': {"-": 0}}, inplace=True)
    # 去掉千分位分隔符
    df["scale"] = df["scale"].str.replace(',', '').astype(float)
    df.fillna(0, inplace=True)
    return df


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
        "TotalScale": "total_scale",
        "RDate": "update_date"
    }, inplace=True)
    # 对异常值进行处理
    df.replace({'total_scale': {"--": 0}}, inplace=True)
    df.replace({'update_date': {"--": ""}}, inplace=True)
    df.replace({'fund_count': {"": 0}}, inplace=True)
    # 更改数据类型
    df["total_scale"] = df["total_scale"].astype(float)
    df["fund_count"] = df["fund_count"].astype(int)
    return df


def get_scale_info_exclude_monetary(date_str: str):
    """获取非货币理财型基金规模
    """
    df_all = get_origin_scale_info(date_str, "all")
    df_monetary = get_origin_scale_info(date_str, "monetary")
    df_wm = get_origin_scale_info(date_str, "wealth-management")
    df_monetary = df_monetary.rename(columns={"scale": "monetary_scale", "fund_count": "monetary_fund_count", "fund_manager_count": "monetary_fund_manager_count"})
    df_wm = df_wm.rename(columns={"scale": "wm_scale", "fund_count": "wm_fund_count", "fund_manager_count": "wm_fund_manager_count"})
    df = pd.merge(df_all, df_monetary[["fund_company", "monetary_scale", "monetary_fund_count", "monetary_fund_manager_count"]], how="left", on="fund_company")
    df = pd.merge(df, df_wm[["fund_company", "wm_scale", "wm_fund_count", "wm_fund_manager_count"]], how="left", on="fund_company")
    df.fillna(0, inplace=True)
    df["scale"] = df["scale"] - df["monetary_scale"] - df["wm_scale"]
    df["fund_count"] = df["fund_count"] - df["monetary_fund_count"] - df["wm_fund_count"]
    df["fund_manager_count"] = df["fund_manager_count"] - df["monetary_fund_manager_count"] - df["wm_fund_manager_count"]
    return df

def format_scale_info(fund_type: str, orderby: str, orderdir: str):
    """
    """
    today = datetime.now().strftime("%Y-%m-%d")
    # 先获取公司列表
    df_company = get_company_list_by_fund_type(today, "all")
    if fund_type == "non-monetary-wm":
        df = get_scale_info_exclude_monetary(today)
    else:
        df = get_origin_scale_info(today, fund_type)
    df = pd.merge(df, df_company[["fund_company", "company_code"]], how="left", on="fund_company")
    # 计算平均规模和平均管理规模
    df["avg_scale"] = df["scale"] / df["fund_count"]
    df["avg_manage_scale"] = df["scale"] / df["fund_manager_count"]
    # 处理inf值和nan值
    df.replace(np.inf, 0, inplace=True)
    df.fillna(0, inplace=True)
    # 统一保留两位小数
    df = df.round(2)
    if not orderby:
        orderby = "scale"
    df.sort_values(by=orderby, ascending=(orderdir=="asc"), inplace=True, ignore_index=True)
    df.index = df.index + 1
    df.reset_index(inplace=True)
    return df.to_dict("records")
