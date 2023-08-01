#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : weekly_report_task_test.py
# @Time    : 2023/7/25 15:32
# @motto   :  rain cats and dogs
import logging
import time
import sys, os
import polars as pl
import datetime
import smtplib
import email.message

if __name__ == '__main__':
    df = pl.DataFrame(
        {
            "foo": [1, 2, 3],
            "bar": [6, 7, 8],
            "ham": ["a", "b", "c"],
        }
    )
    print(df.filter(pl.col('foo') > 2))
    print(df.groupby('foo').count()['count'])
