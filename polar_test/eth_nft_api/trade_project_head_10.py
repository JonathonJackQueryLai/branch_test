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
    name_li = ['CryptoPunks', 'Bored Ape Yacht Club', 'SupDucks', 'Decentraland', 'CryptoKitties']
    name_li = ['CryptoKitties']

    # 从pgsql中获取块信息
    block_info_query_sql = "select block_number,timestamp_of_block,date_of_block from block_info"
    block_info = pl.read_database(block_info_query_sql, uri)

    # 从pgsql中获取汇率信息
    rate_info_query_sql = "select date_of_rate,eth_usd_rate from rate_info"
    rate_info = pl.read_database(rate_info_query_sql, uri)
    block_info = block_info.rename({'date_of_block': 'date'})
    rate_info = rate_info.rename({'date_of_rate': 'date'})
    print('rename success')
    for name in name_li:
        # # 从pgsql中获取合约信息
        contract_info_query_sql = f"select contract_address,contract_type,contract_name,contract_symbol,init_block from contract_info where contract_name = '{name}'"

        contract_info = pl.read_database(contract_info_query_sql, uri)
        contract_address = tuple(contract_info['contract_address'])
        print(contract_address)
        trade_info_query_sql = f"select * from trade_record  where contract_address in {contract_address}"

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
        trade_info.write_csv(
            f'/home/project/logs/weekreport_log/logs/trade_{name}.csv')


if __name__ == '__main__':
    weekly_report_task()
