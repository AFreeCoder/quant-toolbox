#!/usr/bin/env python
# coding=utf-8
import sys
import os

import pandas as pd

sys.path.append(os.getcwd())
from source import index, macro

class Debt:

    def __init__(self) -> None:
        pass


    def compute_fed(self, index_code: str):
        indx = index.Index()
        df_index = indx.load_index_fundmental(index_code, "mcw")

        debt = macro.Macro()
        df_debt = debt.load_national_debt_data(area_code="cn", rows=["date", "mir_y10"])
        df = pd.merge(df_index, df_debt, on="date", how="inner")
        df["fed"] = round((1/df["pe_ttm"] - df["mir_y10"]) * 100, 3)
        df.sort_values(by="date", ascending=True, inplace=True)
        data = {
            "x_data": df["date"].tolist(),
            "y_cp": df["cp"].tolist(),
            "y_fed": df["fed"].tolist(),
            "count": len(df["date"])
        }
        return data


if __name__ == "__main__":
    debt = Debt()
    debt.compute_fed("000300")

