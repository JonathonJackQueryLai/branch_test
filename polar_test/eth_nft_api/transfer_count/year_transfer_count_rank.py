#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : year_transfer_count_rank.py.py
# @Time    : 2023/8/23 11:35
# @motto   :  rain cats and dogs
import logging
import time
import sys
import os
import polars as pl
import datetime
from dateutil.relativedelta import relativedelta


def year_rank_head():
    contract_info = pl.read_database('select contract_address,contract_name from contract_info',connection_uri=eth_nft_uri)
    for year in range(2015, 2024):
        print(f"--------------{year}------------------")
        sql = f"""
        WITH block_range AS (
            SELECT min(block_number) AS min_block, max(block_number) AS max_block
            FROM block_info
            WHERE date_of_block >=  '{year}-01-01' and date_of_block <=  '{year}-12-31'
        )
        SELECT contract_address,count(contract_address)
        FROM transfer_record
        WHERE block_number >= (SELECT min_block FROM block_range)
        AND block_number <= (SELECT max_block FROM block_range)
        group by contract_address order by count desc limit 10 
        """
        df = pl.read_database(query=sql, connection_uri=eth_nft_uri)
        df = df.with_columns(df['count'].rename('changed_hands'))
        rank_columns = pl.DataFrame({'rank': [i for i in range(1, df.shape[0] + 1)]})
        df = df.hstack(rank_columns)
        df = df.with_columns((pl.col('changed_hands')).cast(int)).select(
            ['rank', 'contract_address', 'changed_hands'])


        df = df.join(contract_info, on='contract_address', how='left')
        df.write_database(table_name=f"year_transfer_head_rank_{year}", connection_uri=eth_nft_api_uri,
                          if_exists='append')


if __name__ == '__main__':
    print(f'to locate the issue that out of memory  ,so print currency process num :{os.getpid()}')
    eth_nft_uri = "postgresql://dev_user:nft_project_dev220@52.89.34.220:5432/eth_nft"
    eth_nft_api_uri = "postgresql://postgres:nft_project123@52.89.34.220:5432/eth_nft_api"
    start_time = time.time()
    year_rank_head()
    end_time = time.time()
    print(f"耗时：{end_time - start_time}")
