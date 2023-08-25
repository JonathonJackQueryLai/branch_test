#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : create_date.py
# @Time    : 2023/8/25 14:59
# @motto   :  rain cats and dogs

import datetime
import polars as pl
import time


def create_monday(func, *args, **kwargs):
    start_date = datetime.datetime(2023, 1, 1)
    end_date = datetime.datetime.now()
    date_range = pl.date_range(start=start_date, end=end_date, eager=True)
    date_range = pl.DataFrame({'date': date_range})
    # 提取所有周一的日期
    mondays = date_range.filter(pl.col("date").dt.weekday() == 1).with_columns(pl.col('date'))
    st = time.time()
    # 打印结果
    for i in mondays['date']:
        try:
            start_date = i.strftime("%Y-%m-%d")
            print(f'周一日期:{start_date}')
            func(*args, **kwargs)
        except Exception as ex:
            print(ex)
    et = time.time()
    print(f'running program used time:{et - st}')
    return func
