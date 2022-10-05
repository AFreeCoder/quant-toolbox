from datetime import datetime
import numpy as np
import pandas as pd

from . import company

def get_company_list_by_scale(fund_type: str, orderby: str, orderdir: str):
    today = datetime.now().strftime("%Y-%m-%d")
    # 先获取公司列表
    df_company = company.get_company_list_by_fund_type(today, "all")
    if fund_type == "non-monetary-wm":
        df = company.get_scale_info_exclude_monetary(today)
    else:
        df = company.get_origin_scale_info(today, fund_type)
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
    return df