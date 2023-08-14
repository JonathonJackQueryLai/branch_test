#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : week_transfer_count_rank.py
# @Time    : 2023/8/11 11:12
# @motto   :  rain cats and dogs


import polars as pl
import sys
import os
import time
import datetime


def transfer_count_rank(start_date, end_date=None):
    end_date = end_date if end_date else datetime.datetime.strptime(start_date, "%Y-%m-%d") + datetime.timedelta(days=6)
    last_week_end_date = end_date + datetime.timedelta(days=-7)
    last_week_start_date = last_week_end_date + datetime.timedelta(days=-6)
    end_date = end_date.strftime("%Y-%m-%d")
    pl.Config.set_fmt_str_lengths(100)
    pl.Config.set_tbl_rows(20)
    # 从pgsql中获取块信息
    block_info_query_sql = "select block_number,timestamp_of_block,date_of_block from block_info"
    block_info = pl.read_database(block_info_query_sql, uri)
    # join block dataframe, rate dataframe and trade dataframe together
    block_info = block_info.rename({'date_of_block': 'date'})

    # contract_info
    contract_info_query_sql = "select * from contract_info"
    contract_info = pl.read_database(contract_info_query_sql, uri)

    # block_info周表
    week_block_df = block_info.filter((pl.col('date').cast(str) >= start_date) & (pl.col('date').cast(str) <= end_date))
    start_block_weekly = week_block_df.sort('block_number').head(1)['block_number'][0]
    end_block_weekly = week_block_df.sort('block_number').tail(1)['block_number'][0]
    last_week_end_date = last_week_end_date.strftime("%Y-%m-%d")
    last_week_start_date = last_week_start_date.strftime("%Y-%m-%d")

    # 上周的block_info表
    last_week_block_df = block_info.filter(
        (pl.col('date').cast(str) >= last_week_start_date) & (pl.col('date').cast(str) <= last_week_end_date))
    last_start_block_weekly = last_week_block_df.sort('block_number').head(1)['block_number'][0]
    last_end_block_weekly = last_week_block_df.sort('block_number').tail(1)['block_number'][0]

    # -----------------
    print(start_block_weekly)
    print(end_block_weekly)
    print(last_start_block_weekly)

    #  读取transfer_info的一周数据的表
    st = time.time()
    transfer_info_query_sql = f"select transaction_hash,contract_address,from_address,to_address,token_id,block_number from transfer_record where  block_number >={start_block_weekly} and  block_number <= {end_block_weekly}"
    transfer_info = pl.read_database(transfer_info_query_sql, uri)
    et = time.time()
    print("transfer_info加载成功")
    # 计算读取最大的表的时间为多少很重要
    print(f'read transfer_record used time :{et - st}')

    # 周交易次数计算
    print('Collection级别本周交易次数排行榜为：')

    start_date = start_date.replace('-', '_')

    week_trade_df_head = transfer_info.groupby('contract_address').count().join(contract_info,
                                                                                on='contract_address').select(
        ['contract_name', 'count']).sort('count', descending=True).head(10)
    et = time.time()
    print("transfer_info加载成功")
    # 计算读取最大的表的时间为多少很重要
    print(f'read transfer_record used time :{et - st}')
    #  读取transfer_info的上一周数据的表

    last_contract_info = contract_info.filter(
        pl.col('contract_name').is_in(list(week_trade_df_head['contract_name'])
    ))
    last_transfer_info_query_sql = f"select transaction_hash,contract_address,from_address,to_address,token_id,block_number from transfer_record where  block_number >= {last_start_block_weekly} and block_number <= {last_end_block_weekly}"
    last_transfer_info = pl.read_database(last_transfer_info_query_sql, uri)
    last_week_trade_df_head = last_transfer_info.groupby('contract_address').count().join(last_contract_info,
                                                                                          on='contract_address').select(
        ['contract_name', 'count']).sort('count', descending=True).head(10)
    last_week_trade_df_head = last_week_trade_df_head.rename({'count': "last_week_change_hands"})

    week_trade_df_head = week_trade_df_head.join(last_week_trade_df_head, on='contract_name').with_columns(
        ((week_trade_df_head['count'] - last_week_trade_df_head['last_week_change_hands']) / week_trade_df_head['count']).rename('change_rate'))

    week_trade_df_head = week_trade_df_head.rename({'contract_name': 'project', 'count': "changed_hands"})
    rank_columns = pl.DataFrame({'rank': [i for i in range(1, week_trade_df_head.shape[0] + 1)]})
    week_trade_df_head = week_trade_df_head.hstack(rank_columns)
    print(week_trade_df_head)
    # 写入数据库
    week_trade_df_head.write_database(f'week_trade_df_head_rank_{start_date}', connection_uri=write_uri,
                                      if_exists='replace')


if __name__ == '__main__':
    uri = "postgresql://dev_user:nft_project_dev220@52.89.34.220:5432/eth_nft"
    write_uri = "postgresql://postgres:nft_project123@52.89.34.220:5432/eth_nft_api"
    print(f'to locate the issue that out of memory  ,so print currency process num :{os.getpid()}')
    if sys.argv[1]:
        start_date = sys.argv[1]
    else:
        print('lack of start_date args')
    os.system('exit')

    transfer_count_rank(start_date)
