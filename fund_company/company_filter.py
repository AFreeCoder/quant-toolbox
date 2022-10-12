import sys
import os
from datetime import datetime

import pandas as pd

sys.path.append(os.getcwd())
from fund_company import scale, company_yield, awards, work_year, company


def filter_company_top_N_by_scale(fund_type: str, orderby: str, n: int):
    df_company = scale.format_scale_info(fund_type, "", "")
    df_company.sort_values(by=orderby, ascending=False, inplace=True, ignore_index=True)
    df_top_n = df_company[:n]
    return df_top_n


def filter_company_top_N_by_avg_yield(fund_type: str, orderby: str, n: int):
    df_company = company_yield.get_yield_rank(fund_type, "", "")
    df_company.sort_values(by=orderby, ascending=False, inplace=True, ignore_index=True)
    df_top_n = df_company[:n]
    return df_top_n


def filter_company_awards_lager_than_N(n: int):
    df_company = awards.get_awards_info("desc")
    df_company = df_company[df_company["award_count"]>=n]
    return df_company


def filter_company_top_N_by_work_year(n: int):
    df_company = work_year.get_work_year_info("desc")
    df_company.sort_values(by="rank", ascending=True, inplace=True, ignore_index=True)
    df_top_n = df_company[:n]
    return df_top_n


def apply_condition_expression(conditions: dict):
    # 获取基金公司名单
    today = datetime.now().strftime("%Y-%m-%d")
    df_result = company.get_company_list_by_fund_type(today, "all")
    # 应用条件表达式
    conjunction = conditions["conjunction"]
    children = conditions["children"]
    df_list = []
    for item in children:
        if "conjunction" in item:
            df = apply_condition_expression(item)
        else:
            df = filter_company_by_condition(item)
        df_list.append(df)
    merge_how = "inner"
    if conjunction == "or":
        merge_how = "outer"
    for df in df_list:
        df_result = pd.merge(df_result, df[["company_code"]], on="company_code", how=merge_how)
    return df_result


def filter_company_by_condition(condition):
    left = condition["left"]
    data_category = left["field"]
    # op暂时不支持修改，排名默认小于等于，获奖数默认大于等于
    value_n = int(condition["right"])
    df = pd.DataFrame()
    if data_category == "non-monetary-wm-scale":
        df = filter_company_top_N_by_scale("non-monetary-wm", "scale", value_n)
    elif data_category == "non-monetary-wm-avg-scale":
        df = filter_company_top_N_by_scale("non-monetary-wm", "avg_scale", value_n)
    elif data_category == "equity-scale":
        df = filter_company_top_N_by_scale("equity", "scale", value_n)
    elif data_category == "hybrid-scale":
        df = filter_company_top_N_by_scale("hybrid", "scale", value_n)
    elif data_category == "equity-avg-five-year-yield":
        df = filter_company_top_N_by_avg_yield("equity", "avg_five_year", value_n)
    elif data_category == "hybrid-avg-five-year-yield":
        df = filter_company_top_N_by_avg_yield("hybrid", "avg_five_year", value_n)
    elif data_category == "awards-num":
        df = filter_company_awards_lager_than_N(value_n)
    elif data_category == "work-year":
        df = filter_company_top_N_by_work_year(value_n)
    else:
        df = pd.DataFrame()
    return df


if __name__ == "__main__":
    conditions = {
        "page":1,
        "conditions": {
            "conjunction":"and",
            "children":[
                {
                    "left":{
                        "type":"field",
                        "field":"equity-scale"
                    },
                    "op":"less",
                    "right":"10"
                },
                {
                    "conjunction":"and",
                    "children":[
                        {
                            "left":{
                                "type":"field",
                                "field":"equity-scale"
                            },
                            "op":"less_or_equal",
                            "right":"10"
                        },
                        {
                            "left":{
                                "type":"field",
                                "field":"hybrid-scale"
                            },
                            "op":"less_or_equal",
                            "right":"10"
                        }
                    ]
                }
            ]
        },
        "perPage":10
    }
    df = apply_condition_expression(conditions["conditions"])
    print(df[["fund_company", "scale"]])