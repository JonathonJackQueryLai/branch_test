#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : memory_test.py
# @Time    : 2023/7/21 11:14
# @motto   :  rain cats and dogs
import sys
import time

from memory_profiler import profile

import polars as pl

uri = "postgresql://dev_user:nft_project_dev220@52.89.34.220:5432/eth_nft"
import gc


def trade_info_generator(uri, pages, chunks):
    for i in range(pages):
        trade_info_query_sql = f"select currency_address, contract_address,price_value,block_number from trade_record  limit {chunks} offset {i * chunks}"
        trade_info_temp = pl.read_database(trade_info_query_sql, uri, partition_num=10)
        yield trade_info_temp


@profile
def my_function():
    # 这里是您的代码
    # 数据获取

    # 从pgsql中获取合约信息
    trade_record_query_sql = "select currency_address, contract_address,price_value,block_number from trade_record where currency_address= '0x0000000000000000000000000000000000000000' or currency_address ='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' limit 1388133 offset 2776266"
    trade_record = pl.read_database(trade_record_query_sql, uri)
    print(sys.getsizeof(trade_record))


@profile
def my_generator():
    chunks = 34703325
    for i in range(24):
        transfer_info_query_sql = f"select 'transaction_hash','contract_address','from_address','to_address','value','block_number' from transfer_record limit offset  limit {1388132} offset {i * chunks}"
        transfer_info_temp = pl.read_database(transfer_info_query_sql, uri, partition_num=10)
        yield transfer_info_temp


fp = open('./log/memory_profiler.log', 'w+')


@profile(stream=fp)
def memory_profiler_test():
    print(1)
    time.sleep(1)
    print(2)
    time.sleep(1)
    print(3)
    time.sleep(1)


if __name__ == '__main__':
    # memory_profiler_test()
    import datetime

    timestamp = 16262901
    dt = datetime.datetime.fromtimestamp(timestamp)
    formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S')

    print(formatted_date)
