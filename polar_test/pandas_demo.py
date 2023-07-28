#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : pandas_demo.py
# @Time    : 2023/7/19 13:49
# @motto   :  rain cats and dogs

import logging
import gc

import datetime

import pandas as pd
from sqlalchemy import create_engine

log = logging.getLogger('task')


def weekly_report_task():
    pd.set_option('display.max_columns', 20)
    pd.set_option('display.width', 100)
    uri = "postgresql://postgres:nft_project123@52.89.34.220:5432/eth_nft"
    engine = create_engine(uri)
    # 从pgsql中获取合约信息
    contract_info_query_sql = "select * from contract_info limit 500"
    contract_info = pd.read_sql(contract_info_query_sql, engine)
    # 从pgsql中获取块信息
    block_info_query_sql = "select * from block_info "
    block_info = pd.read_sql(block_info_query_sql, engine)
    # 从pgsql中获取汇率信息
    rate_info_query_sql = "select * from rate_info limit 500"
    rate_info = pd.read_sql(rate_info_query_sql, engine)
    # 从pgsql中获取transfer信息
    transfer_info_query_sql = "select * from transfer_record limit 500"
    transfer_info = pd.read_sql(transfer_info_query_sql, engine)
    # 从pgsql中获取trade信息
    trade_info_query_sql = "select ,,  from trade_record where currency_address = '0x0000000000000000000000000000000000000000' or currency_address ='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' "
    trade_info = pd.read_sql(trade_info_query_sql, engine)

    print('00000000000000000000000000 从Pgsql获取数据成功.')

    # set the currency filter
    currency_list = [
        '0x0000000000000000000000000000000000000000',
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    ]

    # filter the trade data
    trade_info = trade_info[trade_info['currency_address'].isin(currency_list)]

    # join block dataframe, rate dataframe and trade dataframe together
    trade_info = pd.merge(trade_info, block_info[['block_number', 'date_of_block']], on='block_number')
    trade_info = pd.merge(trade_info, rate_info, left_on='date', right_on='date_of_rate')
    trade_info['price_value'] = trade_info['price_value'].astype(float)
    trade_info['eth_usd_rate'] = trade_info['eth_usd_rate'].astype(float)

    # calculate the trade price in USD
    trade_info['price_usd'] = trade_info['price_value'] * trade_info['eth_usd_rate']

    # 提取数据
    # 周数据dataframe用于计算周榜
    # 全数据dataframe用于计算总榜

    # 时间为从当前任务执行之间到7天前, 时间跨度为一周
    start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    end_date = datetime.datetime.today().strftime('%Y-%m-%d')

    week_block_df = block_info[(block_info['date_of_block'] >= start_date) & (block_info['date_of_block'] <= end_date)]
    start_block_weekly = week_block_df.head(1)['block_number'].values[0]
    end_block_weekly = week_block_df.tail(1)['block_number'].values[0]
    week_trade_df = trade_info[(trade_info['block_number'].astype(int) >= start_block_weekly) & (
            trade_info['block_number'].astype(int) <= end_block_weekly)].sort_values('block_number')
    week_trade_df['price_usd'] = week_trade_df['price_value'] * week_trade_df['eth_usd_rate']

    week_trade_df = pd.merge(week_trade_df, contract_info, on='contract_address')
    # week_transfer_df = transfer_info[(transfer_info['block_number'].astype(int) >= start_block_weekly) & (transfer_info['block_number'].astype)


if __name__ == '__main__':
    from polars import LazyFrame


    # 定义计算操作
    def compute_sum(a, b):
        print("正在计算...")
        return a + b


    # 创建LazyFrame对象
    lazy_frame = LazyFrame({'A': [1], 'B': [2]})

    # 获取结果
    result = lazy_frame.select()
    print("计算结果:", result)
