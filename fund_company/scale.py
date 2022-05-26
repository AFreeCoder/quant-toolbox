from datetime import date, datetime
from functools import lru_cache
import pandas as pd
import numpy as np
import requests

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
    df.columns = ["fund_company", "relate_link", "establish_time", "evaluate", "scale", "fund_num", "fund_manager_num"]
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
    df_monetary = df_monetary.rename(columns={"scale": "monetary_scale", "fund_num": "monetary_fund_num", "fund_manager_num": "monetary_fund_manager_num"})
    df_wm = df_wm.rename(columns={"scale": "wm_scale", "fund_num": "wm_fund_num", "fund_manager_num": "wm_fund_manager_num"})
    df = pd.merge(df_all, df_monetary[["fund_company", "monetary_scale", "monetary_fund_num", "monetary_fund_manager_num"]], how="left", on="fund_company")
    df = pd.merge(df, df_wm[["fund_company", "wm_scale", "wm_fund_num", "wm_fund_manager_num"]], how="left", on="fund_company")
    df.fillna(0, inplace=True)
    df["scale"] = df["scale"] - df["monetary_scale"] - df["wm_scale"]
    df["fund_num"] = df["fund_num"] - df["monetary_fund_num"] - df["wm_fund_num"]
    df["fund_manager_num"] = df["fund_manager_num"] - df["monetary_fund_manager_num"] - df["wm_fund_manager_num"]
    return df

def format_scale_info(fund_type: str, orderby: str, orderdir: str):
    """
    """
    today = datetime.now().strftime("%Y-%m-%d")
    if fund_type == "non-monetary-wm":
        df = get_scale_info_exclude_monetary(today)
    else:
        df = get_origin_scale_info(today, fund_type)
    # 计算平均规模和平均管理规模
    df["avg_scale"] = df["scale"] / df["fund_num"]
    df["avg_manage_scale"] = df["scale"] / df["fund_manager_num"]
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
