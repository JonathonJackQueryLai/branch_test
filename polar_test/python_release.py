#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/7/19 21:00
# @Author  : Jonathon
# @File    : python_release.py
# @Software: PyCharm
# @ Motto : 客又至，当如何
import logging
import pandas as pd
import datetime
import smtplib
import email.message
import pymysql
log = logging.getLogger('task')
from sqlalchemy import create_engine

def weekly_report_task():
    uri = "postgresql://postgres:nft_project123@52.89.34.220:5432/eth_nft"
    engine = create_engine(uri)
    # 从pgsql中获取合约信息
    contract_info_query_sql = "select * from contract_info limit 500"
    contract_info = pd.read_sql(contract_info_query_sql, engine)

    # 从pgsql中获取块信息
    block_info_query_sql = "select * from block_info limit 500"
    block_info = pd.read_sql(block_info_query_sql, uri)

    # 从pgsql中获取汇率信息
    rate_info_query_sql = "select * from rate_info"
    rate_info = pd.read_sql(rate_info_query_sql, uri)

    # 从pgsql中获取transfer信息
    transfer_info_query_sql = "select * from transfer_record"
    transfer_info = pd.read_sql(transfer_info_query_sql, uri)

    # 从pgsql中获取trade信息
    trade_info_query_sql = "select * from trade_record"
    trade_info = pd.read_sql(trade_info_query_sql, uri)

    print('00000000000000000000000000 从Pgsql获取数据成功.')

    # set the currency filter
    currency_list = [
        '0x0000000000000000000000000000000000000000',
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    ]

    # filter the trade data
    trade_info = trade_info[trade_info['currency_address'].isin(currency_list)]

    # join block dataframe, rate dataframe and trade dataframe together
    trade_info = pd.merge(trade_info, block_info.rename(columns={'date_of_block': 'date'}), on='block_number')
    trade_info = pd.merge(trade_info, rate_info, left_on='date', right_on='date_of_rate')
    trade_info['price_usd'] = trade_info['price_value'] * trade_info['eth_usd_rate']

    # 提取数据
    # 周数据dataframe用于计算周榜
    # 全数据dataframe用于计算总榜

    # 时间为从当前任务执行之间到7天前, 时间跨度为一周
    start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    end_date = datetime.datetime.today().strftime('%Y-%m-%d')

    week_block_df = block_info[(block_info['date_of_block'] >= start_date) & (block_info['date_of_block'] <= end_date)]
    start_block_weekly = week_block_df.head(1)['block_number'].iloc[0]
    end_block_weekly = week_block_df.tail(1)['block_number'].iloc[0]
    week_trade_df = trade_info[(trade_info['block_number'].astype(int) >= start_block_weekly) &
                               (trade_info['block_number'].astype(int) <= end_block_weekly)].sort_values('block_number')
    week_trade_df['price_usd'] = week_trade_df['price_value'] * week_trade_df['eth_usd_rate']

    # 总市值计算
    avg_price_df = trade_info.groupby('contract_address')[['price_value', 'price_usd']].mean()
    token_num_df = transfer_info.groupby(['contract_address', 'token_id']).size()
    marketcap_df = pd.merge(contract_info, token_num_df, left_on='contract_address', right_on='contract_address')
    marketcap_df = pd.merge(marketcap_df, avg_price_df, left_on='contract_address', right_on='contract_address')
    market_cap_eth = round((marketcap_df['size'] * marketcap_df['price_value']).sum(), 2)
    market_cap_usd = round((marketcap_df['size'] * marketcap_df['price_usd']).sum(), 2)
    market_cap_result = f'截至{end_date},总市值为:{market_cap_eth}ETH/{market_cap_usd}USD'
    print(market_cap_result)

    # 周交易量排行榜计算
    trade_volume_df = week_trade_df.groupby('contract_address')['price_usd'].sum()

    # final result
    # trade_volume_collection_df = pd.merge(trade_volume_df, contract_info, left_on


if __name__ == '__main__':
    # weekly_report_task()
    import polars as pl

    # 构建示例数据
    data = {
        'contract_address': ['0x123', '0x456', '0x123', '0x789', '0x456','0x789'],
        'token_id': [1, 2, 1, 3, 2,3],
        'price': [100, 200, 150, 300, 250,1000]
    }

    # 创建DataFrame
    week_trade_df = pl.DataFrame(data)

    # 分组并计算每个分组中价格字段的最小值、最大值和平均值
    result = week_trade_df.groupby(['contract_address', 'token_id']).agg(
        pl.min('price').alias('min_price'),
        pl.max('price').alias('max_price'),
        pl.mean('price').alias('avg_price')
    ).sort('avg_price',descending=True)
    print(week_trade_df.groupby(['contract_address', 'token_id']).sum())
        # 计算 price 字段的平均值
    average_price = week_trade_df['price'].mean()
    print(average_price)
    # 打印结果
    # print(result.head(1))
    # print(result.tail(1))