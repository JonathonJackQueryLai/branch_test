#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : trade_project_head_10.py
# @Time    : 2023/8/10 16:53
# @motto   :  rain cats and dogs
import polars as pl


def weekly_report_task():
    pl.Config.set_fmt_str_lengths(100)
    pl.Config.set_tbl_rows(20)
    # 周日的日期

    # end_date = end_date - datetime.timedelta(days=30) * 3
    # end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    # 数据获取
    uri = "postgresql://dev_user:nft_project_dev220@52.89.34.220:5432/eth_nft"
    # # 从pgsql中获取合约信息
    # contract_info_query_sql = "select contract_address,contract_type,contract_name,contract_symbol,init_block from contract_info"
    # contract_info = pl.read_database(contract_info_query_sql, uri)

    # 从pgsql中获取块信息
    block_info_query_sql = "select block_number,timestamp_of_block,date_of_block from block_info"
    block_info = pl.read_database(block_info_query_sql, uri)

    # 从pgsql中获取汇率信息
    rate_info_query_sql = "select date_of_rate,eth_usd_rate from rate_info"
    rate_info = pl.read_database(rate_info_query_sql, uri)
    block_info = block_info.rename({'date_of_block': 'date'})
    rate_info = rate_info.rename({'date_of_rate': 'date'})
    print('rename success')
    # 从pgsql中获取trade信息
#     trade_info_query_sql = f"""
#     select * from trade_record   where contract_address in (
# '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb',
# '0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d',
# '0x99a9b7c1116f9ceeb1652de04d5969cce509b069',
# '0x60e4d786628fea6478f785a6d7e704777c86a7c6',
# '0x8821bee2ba0df28761afff119d66390d594cd280',
# '0x50f5474724e0ee42d9a4e711ccfb275809fd6d4a',
# '0xed5af388653567af2f388e6224dc7c4b3241c544',
# '0x769272677fab02575e84945f03eca517acc544cc',
# '0x06012c8cf97bead5deae237070f9587f8e7a266d',
# '0x5af0d9827e0c53e4799bb226655a1de152a425a5')  """
    # 0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d
    trade_info_query_sql = "select * from trade_record  where contract_address = '0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d'"

    trade_info = pl.read_database(trade_info_query_sql, uri)
    currency_list = [
        '0x0000000000000000000000000000000000000000',
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    ]
    # filter the trade data
    trade_info = trade_info.filter(
        pl.col('currency_address').is_in(currency_list)
    )
    print("表trade_info加载成功")
    trade_info = trade_info.join(block_info.with_columns(pl.col('block_number')), on='block_number')
    trade_info = trade_info.join(rate_info.with_columns(pl.col('eth_usd_rate')), on='date')
    trade_info = trade_info.with_columns(
        (trade_info['price_value'] * trade_info['eth_usd_rate']).rename('eth_usd_price'))
    trade_info.write_csv('/home/project/logs/weekreport_log/logs/trade_0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d.csv')


if __name__ == '__main__':
    weekly_report_task()
