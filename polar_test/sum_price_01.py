#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : sum_price_01.py
# @Time    : 2023/7/20 9:43
# @motto   :  rain cats and dogs
import logging
import gc
import datetime
import sys
import pandas as pd
from sqlalchemy import create_engine

log = logging.getLogger('task')
# 建立与数据库的连接,且创建一个pandas的SQL连接对象
# con = psycopg2.connect(host='52.89.34.220',
#                        user='postgres',
#                        password='nft_project123',
#                        database='eth_nft',
#                        port=5432
#                        )
# 创建数据库连接

'''
transaction_hash
block_number
contract_address
token_id
seller
buyer
currency_address
price_value
market
'''


def market_cap_result():
    uri = "postgresql://postgres:nft_project123@52.89.34.220:5432/eth_nft"
    engine = create_engine(uri)

    # 从pgsql中获取汇率信息
    rate_info_query_sql = "select * from rate_info"
    rate_info = pd.read_sql(rate_info_query_sql, engine)
    # 为了调试方便
    rate_info.rename(columns={'date_of_rate': 'date'}, inplace=True)
    sys.getsizeof(rate_info)
    # 从pgsql中获取合约信息
    contract_info_query_sql = "select * from contract_info"
    contract_info = pd.read_sql(contract_info_query_sql, engine)
    sys.getsizeof(contract_info)
    # 从pgsql中获取块信息
    block_info_query_sql = "select * from block_info limit 500"
    block_info = pd.read_sql(block_info_query_sql, engine)
    block_info.rename(columns={'date_of_rate': 'date'}, inplace=True)

    # 从pgsql中获取trade信息
    trade_info_query_sql = "select  contract_address,price_value,block_number from trade_record where currency_address= '0x0000000000000000000000000000000000000000' or currency_address ='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' limit 500"
    trade_info = pd.read_sql(trade_info_query_sql, engine)

    # join block dataframe, rate dataframe and trade dataframe together
    trade_info = pd.merge(trade_info, block_info[['block_number', 'date_of_block']], on='block_number')
    trade_info = pd.merge(trade_info, rate_info, left_on='date', right_on='date_of_rate')
    trade_info['price_value'] = trade_info['price_value'].astype(float)
    trade_info['eth_usd_rate'] = trade_info['eth_usd_rate'].astype(float)

    # calculate the trade price in USD
    trade_info['price_usd'] = trade_info['price_value'] * trade_info['eth_usd_rate']


if __name__ == '__main__':
    market_cap_result()
