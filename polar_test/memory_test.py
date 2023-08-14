# !/usr/bin/env python
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
api_uri = "postgresql://postgres:nft_project123@52.89.34.220:5432/eth_nft_api"
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


def calculate_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()  # 记录函数开始执行的时间
        result = func(*args, **kwargs)  # 执行函数
        end_time = time.time()  # 记录函数执行结束的时间
        execution_time = end_time - start_time  # 计算函数执行耗时
        print(f"函数 {func.__name__} 的执行时间为: {execution_time} 秒")
        return result

    return wrapper


@calculate_execution_time
def memory_profiler_test(uri):
    start_date = '2023-07-17'
    end_date = '2023-07-23'
    17806895
    #     block_info_sql = f"""
    #          select min(block_number) as block_number1 ,max(block_number) as block_number2 from
    # block_info where date_of_block >='2022-12-26' and date_of_block <='2023-01-01'
    #         """
    #     sql = "select * from transfer_record where block_number between 16262813 and 16312970"
    #     transfer_sql = """SELECT * FROM transfer_record  where  block_number > 15000000 limit 500"""
    end_date = "2023-07-30"
    block_info_sql = f"""SELECT * FROM block_info  where  date_of_block <='{end_date}'"""
    block_info = pl.read_database(block_info_sql, uri)
    # start_block_weekly = block_info.sort('block_number').head(1)['block_number'][0]
    end_block_weekly = block_info.sort('block_number').tail(1)['block_number'][0]
    # trade_sql = f"""SELECT * FROM trade_record  where  block_number <={end_block_weekly}"""
    transfer_record_sql = f"""SELECT * FROM transfer_record  where  block_number <={end_block_weekly}"""

    transfer_record_info = pl.read_database(transfer_record_sql, uri)
    token_num_df = transfer_record_info.groupby(['contract_address', 'token_id']).count()
    print(token_num_df)
    # print(f"end_block: {end_block_weekly}, trade_info: {trade_info.shape}, df_last: {trade_info['block_number'].tail(1)}, df_start:{trade_info['block_number'].head(1)}")


def change_field():
    query = f"select * from week_trade_df_head_rank_2023_07_24"
    df = pl.read_database(query, api_uri)
    print(tuple(df['project']))
    # df = df.rename({'contract_name': 'project', 'count': "changed_hands"})
    # df1 = pl.DataFrame({'rank': [i for i in range(1, df.shape[0] + 1)]})
    # df = df.hstack(df1)
    # df.write_database('week_trade_df_head_rank_2023_07_24', api_uri, if_exists='append')


if __name__ == '__main__':
    # memory_profiler_test(uri)
    change_field()
