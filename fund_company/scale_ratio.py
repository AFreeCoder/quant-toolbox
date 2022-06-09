from datetime import datetime

from . import company

fund_name_map = {
    "equity": "股票型基金",
    "hybrid": "混合型基金",
    "bond": "债券型基金",
    "index": "指数型基金",
    "qdii": "QDII基金",
    "monetary": "货币型基金"
}

def get_scale_ratio(company_code: str):
    """根据基金公司代码获取各类型基金规模
    """
    today = datetime.today().strftime("%Y-%m-%d")
    ratio = []
    for fund_type in ["equity", "hybrid", "bond", "index", "qdii", "monetary"]:
        df = company.get_company_list_by_fund_type(today, fund_type)
        df = df[df["company_code"] == company_code]
        if len(df) == 0:
            ratio.append({
                "name": fund_name_map[fund_type],
                "value": 0
            })
            continue
        ratio.append({
            "name": fund_name_map[fund_type],
            "value": df.iloc[0]["scale"]
        })
    return ratio

