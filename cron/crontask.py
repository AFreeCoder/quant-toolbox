import time
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler

from fund_company import company, company_yield

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

def start():
    sched = BackgroundScheduler()
    sched.add_job(polling_company_yield, "cron", day="*", hour=6, minute=10)


if __name__ == "__main__":
    sched = BackgroundScheduler()
    sched.add_job(polling_company_yield, "interval", seconds=5, id="my_job_id")
    sched.start()
    time.sleep(20)
