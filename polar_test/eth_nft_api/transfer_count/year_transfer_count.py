#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : year_transfer_count.py
# @Time    : 2023/8/15 14:46
# @motto   :  rain cats and dogs
# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : year_transfer_count.py
# @Time    : 2023/8/15 14:46
# @motto   :  rain cats and dogs

import polars as pl
import sys
import os
import time


if __name__ == '__main__':
    eth_nft_uri = "postgresql://dev_user:nft_project_dev220@52.89.34.220:5432/eth_nft"
    eth_nft_api_uri = "postgresql://postgres:nft_project123@52.89.34.220:5432/eth_nft_api"
    print(f'to locate the issue that out of memory  ,so print currency process num :{os.getpid()}')
    st = time.time()
    for year in range(2016, 2023):
        start_date = f'{year}-01-01'
        end_date = f'{year}-12-31'
        sql = f"select min(block_number),max(block_number) from block_info where date_of_block >= '{start_date}' and date_of_block <='{end_date}'"
        block_numbers = pl.read_database(sql, eth_nft_uri)
        # 776788 | 2910454
        sql = f"select count(1) from transfer_record where block_number >= {block_numbers['min'][0]} and block_number<= {block_numbers['max'][0]}"
        number = pl.read_database(sql, eth_nft_uri)
        y_series = pl.Series([year])
        number = number.with_columns([(y_series).alias("year")])
        number.write_database('year_transfer_count', connection_uri=eth_nft_api_uri, if_exists='append')

    et = time.time()
    print(f'running program used time:{et - st}')
