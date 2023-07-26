#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : exec_main.py
# @Time    : 2023/7/20 10:40
# @motto   :  rain cats and dogs


import sys
import polars as pl
from datetime import datetime, timedelta

if __name__ == '__main__':
    start_date = sys.argv[1]
    # 将字符串转换为datetime对象
    # date_str = "2022-12-26"
    end_date = datetime.strptime(start_date, "%Y-%m-%d") +timedelta(days=6)
    print(end_date.strftime("%Y-%m-%d"))
    # # 将新日期转换为字符串
    # new_date_str = new_date.strftime("%Y-%m-%d")
    #
    # print("原日期:", start_date)
    # print("一周后的日期:", new_date_str)

    # import pdfkit
    #
    #
    # def convert_log_to_pdf(log_file, pdf_file):
    #     options = {
    #         'page-size': 'A4',
    #         'margin-top': '0mm',
    #         'margin-right': '0mm',
    #         'margin-bottom': '0mm',
    #         'margin-left': '0mm'
    #     }
    #     pdfkit.from_file(log_file, pdf_file, options=options)
    #
    #
    # # 指定日志文件和PDF文件的路径
    # log_file_path = 'path/to/log_file.log'
    # pdf_file_path = 'path/to/output_file.pdf'
    #
    # # 调用函数进行转换
    # convert_log_to_pdf(log_file_path, pdf_file_path)
