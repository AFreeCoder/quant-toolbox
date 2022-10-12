from datetime import datetime
from functools import lru_cache

import pandas as pd

from . import company

data_file = "./data/fund_company_awards.csv"

@lru_cache(maxsize=2, typed=True)
def read_awards_info(date_str: str):
    df = pd.read_csv(data_file)
    return df


def get_awards_info(orderdir: str):
    today = datetime.now().strftime("%Y-%m-%d")
    # 获取基金公司名单
    df_company = company.get_company_list_by_fund_type(today, "all")
    df = read_awards_info(today)
    df = pd.merge(df, df_company[["fund_company", "company_code"]], how="inner", on="fund_company")
    df.sort_values(by="award_count", ascending=(orderdir=="asc"), inplace=True, ignore_index=True)
    df.index = df.index + 1
    df.reset_index(inplace=True)
    return df