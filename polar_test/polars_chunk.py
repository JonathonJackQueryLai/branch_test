#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : polars_chunk.py
# @Time    : 2023/7/20 14:03
# @motto   :  rain cats and dogs
import sys
import time

import polars as pl
import re
from memory_profiler import profile
import gc
import logging
import datetime

logging.basicConfig(filename='app.log', level=logging.INFO)
# 创建格式化器
formatter = logging.Formatter('[%(levelname)s][%(asctime)s][%(filename)s:%(lineno)d]%(message)s:')

# 创建日志记录器
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 创建文件处理器，并将格式化器添加到处理器中

# file_handler = logging.FileHandler(f'./log/app_{datetime.datetime.now().strftime("%Y_%m_%d_%H%M%S_%f")}.log')
file_handler = logging.FileHandler(
    f'/home/project/logs/weekreport_log/logs/app_{datetime.datetime.now().strftime("%Y_%m_%d_%H%M%S_%f")}.log')
file_handler.setFormatter(formatter)

# 将处理器添加到日志记录器中
logger.addHandler(file_handler)


@profile
def weekly_report_test():
    # 数据获取
    uri = "postgresql://postgres:nft_project123@52.89.34.220:5432/eth_nft"
    # 从pgsql中获取合约信息
    contract_info_query_sql = "select * from contract_info"
    contract_info = pl.read_database(contract_info_query_sql, uri)

    # 从pgsql中获取汇率信息
    rate_info_query_sql = "select * from rate_info "
    rate_info = pl.read_database(rate_info_query_sql, uri)
    rate_info = rate_info.rename({'date_of_rate': 'date'})
    print("rate_info.columns:", rate_info.columns)

    # 从pgsql中获取transfer信息
    # transfer_info_query_sql = "select 'transaction_hash','contract_address','from_address','to_address','value','block_number' from transfer_record "
    # transfer_info = pl.read_database(transfer_info_query_sql, uri)

    # # 从pgsql中获取合约信息
    # 从pgsql中获取块信息
    block_info_query_sql = "select block_number,date_of_block from block_info"
    block_info = pl.read_database(block_info_query_sql, uri)
    # block_info['date_of_block'].rename('date', in_place=True)
    block_info = block_info.rename({'date_of_block': 'date'})
    print("block_info.columns:", block_info.columns)
    print("从pgsql中block_info表获取块信息")

    # 统计表trade_record的记录行数
    trade_info_count_query_sql = "select COUNT(1) from trade_record"
    trade_info_count = pl.read_database(trade_info_count_query_sql, uri)
    # 选择特定的列再选择特定行的值
    trade_record_row = int(trade_info_count['count'][0])

    # 统计多少G
    transfer_info_memory_query_sql = "SELECT pg_size_pretty(pg_total_relation_size('trade_record'))"
    transfer_info_memory = pl.read_database(transfer_info_memory_query_sql, uri)
    transfer_info_memory = re.findall(r'\d+', transfer_info_memory['pg_size_pretty'][0])[0]
    transfer_info_memory = int(transfer_info_memory)
    transfer_info_memory = transfer_info_memory
    # 1g  对应多少数据
    chunks = trade_record_row // transfer_info_memory
    page = transfer_info_memory + (trade_record_row % chunks > 0)
    # 从pgsql中获取transfer信息 在读sql的时候会消耗内存

    # transfer_info = pl.read_database(transfer_info_query_sql, uri)
    trade_info = pl.DataFrame({})
    # 分块执行
    for i in range(page):
        print(f'第{i}次如下------------------------------------------------------------:')
        transfer_info_query_sql = f"select currency_address, contract_address,price_value,block_number from trade_record where currency_address= '0x0000000000000000000000000000000000000000' or currency_address ='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' limit {chunks} offset {i * chunks}"
        logger.info(f'第{i * chunks}条：{transfer_info_query_sql}')
        transfer_info_temp = pl.read_database(transfer_info_query_sql, uri, partition_num=10)
        transfer_info_temp = transfer_info_temp.join(block_info.with_columns(pl.col('block_number')), on='block_number')
        print(transfer_info_temp.columns, transfer_info_temp.shape)
        transfer_info_temp = transfer_info_temp.join(rate_info, on='date', how='inner')
        trade_info = pl.concat([trade_info, transfer_info_temp])
        logger.info(f'处理完后的trade_info:{sys.getsizeof(trade_info)}比特')
        del transfer_info_temp
        gc.collect()

    print(trade_info.shape, trade_info.columns)
    print("------进行计算 ------")
    # ['currency_address', 'contract_address', 'price_value', 'block_number', 'id', 'timestamp_of_block', 'date', '_metadata_created_time', '_metadata_updated_time', 'id_right', 'eth_usd_rate'
    trade_info = trade_info.with_columns((trade_info['price_value'] * trade_info['eth_usd_rate']).rename('price_usd'))
    # trade_info = trade_info.join(contract_info, on='contract_address')

    # 提取数据
    # 周数据dataframe用于计算周榜
    # 全数据dataframe用于计算总榜

    # 时间为从当前任务执行之间到7天前, 时间跨度为一周
    # start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    # end_date = datetime.datetime.today().strftime('%Y-%m-%d')
    # week_block_df = block_info.filter((pl.col('date') >= start_date) & (pl.col('date') <= end_date))
    # start_block_weekly = week_block_df.head(1)['block_number'][0]
    # end_block_weekly = week_block_df.tail(1)['block_number'][0]
    # week_trade_df = trade_info.filter((pl.col('block_number') >= start_block_weekly) & (
    #         pl.col('block_number') <= end_block_weekly)).sort('block_number')
    # week_trade_df = week_trade_df.with_columns(
    #     (week_trade_df['price_value'] * week_trade_df['eth_usd_rate']).rename('price_usd'))
    #
    # week_trade_df = week_trade_df.join(contract_info, on='contract_address')
    # week_transfer_df = transfer_info.filter((pl.col('block_number').cast(int) >= start_block_weekly) & (
    #         pl.col('block_number').cast(int) <= end_block_weekly)).sort('block_number')




if __name__ == '__main__':
    weekly_report_test()
