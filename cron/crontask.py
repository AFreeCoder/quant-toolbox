import time
import logging
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler

from fund_company import company, company_yield, work_year
from conf import index_conf

def polling_company_yield():
    """轮询请求获取基金公司平均收益率，生成缓存
    """
    today = datetime.now().strftime("%Y-%m-%d")
    # 获取基金公司名单
    df_company = company.get_company_list_by_fund_type(today, "all")
    fund_types = ["equity", "hybrid", "bond", "index", "qdii", "monetary", "wealth-management"]
    for fund_type in fund_types:
        for company_code in df_company["company_code"]:
            try:
                company_yield.get_yield_by_code(today, company_code, fund_type)
            except Exception as e:
                print(company_code, e)
            time.sleep(1)
    return

def polling_manager_work_year():
    """轮询请求获取基金经理平均任职年限，生成缓存
    """
    today = datetime.now().strftime("%Y-%m-%d")
    # 获取基金公司名单
    df_company = company.get_company_list_by_fund_type(today, "all")
    for company_code in df_company["company_code"]:
        try:
            work_year.get_work_year_by_code(today, company_code)
        except Exception as e:
            print(company_code, e)
        time.sleep(1)
    return


def start():
    sched = BackgroundScheduler()
    sched.add_job(polling_company_yield, "cron", day="*", hour=6, minute=10)
    sched.add_job(polling_manager_work_year, "cron", day="*", hour=8, minute=10)
    sched.start()


if __name__ == "__main__":
    start()
