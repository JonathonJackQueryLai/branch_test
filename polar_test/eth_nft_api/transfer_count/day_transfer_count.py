#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : day_transfer_count.py
# @Time    : 2023/8/15 14:46
# @motto   :  rain cats and dogs

import polars as pl
import sys
import os
import time
import datetime


def transfer_count_rank(start_date, end_date=None):
    # start_date = '2023-01-01'
    end_date = None
    end_date = end_date if end_date else datetime.datetime.strptime(start_date, "%Y-%m-%d") + datetime.timedelta(days=6)

    pl.Config.set_fmt_str_lengths(100)
    pl.Config.set_tbl_rows(20)
    # 从pgsql中获取块信息
    block_info_query_sql = f"select block_number,timestamp_of_block,date_of_block from block_info where date_of_block >= '{start_date}' and date_of_block <='{end_date}' "
    block_info = pl.read_database(block_info_query_sql, uri)
    # join block dataframe, rate dataframe and trade dataframe together
    block_info = block_info.rename({'date_of_block': 'date'})

    # contract_info
    contract_info_query_sql = "select * from contract_info"
    contract_info = pl.read_database(contract_info_query_sql, uri)

    # block_info日表
    start_block_weekly = block_info.sort('block_number').head(1)['block_number'][0]
    end_block_weekly = block_info.sort('block_number').tail(1)['block_number'][0]

    print(start_block_weekly)
    print(end_block_weekly)

    #  读取transfer_info的一周数据的表
    st = time.time()
    transfer_info_query_sql = f"select transaction_hash,contract_address,from_address,to_address,token_id,block_number from transfer_record where  block_number >= {start_block_weekly} and  block_number <= {end_block_weekly}"
    transfer_info = pl.read_database(transfer_info_query_sql, uri)
    et = time.time()
    print("transfer_info加载成功")
    # 计算读取最大的表的时间为多少很重要
    print(f'read transfer_record used time :{et - st}')

    # 周交易次数计算
    print('Collection级别本周交易次数排行榜为：')

    day_transfer_df_head = transfer_info.groupby('contract_address').count().join(contract_info,
                                                                                  on='contract_address').select(
        ['contract_name', 'count', 'contract_address'])

    start_date = start_date.replace('-', '_')
    day_transfer_df_head = day_transfer_df_head.rename({'contract_name': 'project', 'count': "changed_hands"})
    # 写入数据库
    day_transfer_df_head.write_database(f'day_transfer_count_{start_date}', connection_uri=write_uri,
                                        if_exists='replace')


if __name__ == '__main__':
    uri = "postgresql://dev_user:nft_project_dev220@52.89.34.220:5432/eth_nft"
    write_uri = "postgresql://postgres:nft_project123@52.89.34.220:5432/eth_nft_api"
    print(f'to locate the issue that out of memory  ,so print currency process num :{os.getpid()}')
    start_date = datetime.datetime(2015, 7, 30)
    end_date = datetime.datetime.now()
    date_range = pl.date_range(start=start_date, end=end_date, eager=True)
    date_range = pl.DataFrame({'date': date_range})
    #
    # mondays = date_range.filter(pl.col("date").dt.weekday() == 1).with_columns(pl.col('date'))
    st = time.time()
    # 打印结果
    for i in date_range['date']:
        try:
            start_date = i.strftime("%Y-%m-%d")
            print(f'周一日期:{start_date}')
            transfer_count_rank(start_date)
        except Exception as ex:
            print(f'迭代出现错误：{ex}')
    et = time.time()
    print(f'running program used time:{et - st}')
