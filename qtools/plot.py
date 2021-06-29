#!/usr/bin/env python
# coding=utf-8
"""
@Description: 画图
@Date: 2021-06-26 21:48:12
@LastEditors: wanghaijie01
@LastEditTime: 2021-06-28 13:18:35
"""

from pyecharts import options as opts
from pyecharts.charts import Kline


def kline(title, subtitle, x_data, y_data, y_label):
    """绘制K线图
    args:
        title: 图表标题
        subtitle: 字符串类型
        x_data: 日期list，示例：["2020/01/01", "2020/01/02"]
        y_data: 日线数据list，示例：[
                [2320.26, 2320.26, 2287.3, 2362.94],
                [2300, 2291.3, 2288.26, 2308.38],
            ]
            list中每个元素格式为 [open, close, low, high]
        y_label: 标签
        详细文档见：https://pyecharts.org/#/zh-cn/rectangular_charts?id=klinecandlestick%ef%bc%9ak%e7%ba%bf%e5%9b%be
    return:
        kl:  jupyter 中，对返回的 kl 执行 kl.render_notebook() 即可
    """
    kl = Kline()
    kl.add_xaxis(x_data)
    kl.add_yaxis(y_label, y_data)
    kl.set_global_opts(
    xaxis_opts=opts.AxisOpts(is_scale=True),
    yaxis_opts=opts.AxisOpts(
            is_scale=True,
            splitarea_opts=opts.SplitAreaOpts(
                is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1)
            ),
        ),
    datazoom_opts=[opts.DataZoomOpts()],
    title_opts=opts.TitleOpts(title=title, subtitle=subtitle))
    return kl

    