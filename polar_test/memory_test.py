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


def memory_profiler_test(uri):
    start_date = '2023-07-17'
    end_date = '2023-07-23'
    block_info_sql = """
         SELECT * FROM block_info
        """
    block_info = pl.read_database(block_info_sql, uri)
    week_block_df = block_info.filter((pl.col('date_of_block').cast(str) >= start_date) & (pl.col('date_of_block').cast(str) <= end_date))
    start_block_weekly = week_block_df.head(1)['block_number'][0]
    end_block_weekly = week_block_df.tail(1)['block_number'][0]
    print(start_block_weekly)
    print(end_block_weekly)

    print(week_block_df.sort('block_number').head(1)['block_number'][0])
    print(week_block_df.sort('block_number').tail(1)['block_number'][0])

if __name__ == '__main__':
    memory_profiler_test(uri)
