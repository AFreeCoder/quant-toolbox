from datetime import datetime
from functools import lru_cache
import pandas as pd
import numpy as np
import requests

from . import company


base_host = "http://fund.eastmoney.com"

@lru_cache(maxsize=32, typed=True)
def get_origin_scale_info(date_str: str, fund_type: str):
    url = base_host + "/Company/home/gspmlist"
    param = {
        "fundType": company.fund_type_dic[fund_type]
    }
    r = requests.get(url=url, params=param)
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


def get_scale_info_exclude_monetary(date_str: str):
    """获取非货币理财型基金规模
    """
    df_all = get_origin_scale_info(date_str, "all")
    df_monetary = get_origin_scale_info(date_str, "monetary")
    df_wm = get_origin_scale_info(date_str, "wealth-management")
    df_monetary = df_monetary.rename(columns={
        "scale": "monetary_scale",
        "fund_count": "monetary_fund_count",
        "fund_manager_count": "monetary_fund_manager_count"
    })
    df_wm = df_wm.rename(columns={
        "scale": "wm_scale",
        "fund_count": "wm_fund_count",
        "fund_manager_count": "wm_fund_manager_count"
    })
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
    df_company = company.get_company_list_by_fund_type(today, "all")
    if fund_type == "non-monetary-wm":
        df = get_scale_info_exclude_monetary(today)
    else:
        df = get_origin_scale_info(today, fund_type)
    df = pd.merge(df, df_company[["fund_company", "company_code", "update_date"]], how="left", on="fund_company")
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
