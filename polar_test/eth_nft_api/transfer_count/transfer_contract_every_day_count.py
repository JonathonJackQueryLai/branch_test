#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : transfer_contract_every_day_count.py
# @Time    : 2023/8/21 16:13
# @motto   :  rain cats and dogs
import datetime

import polars as pl
import sys
import os
import time


def history():
    start_date = datetime.datetime(2015, 7, 30)
    # end_date = datetime.datetime(2023, 8, 14)
    end_date = datetime.datetime.now()
    date_range = pl.date_range(start_date, end_date, eager=True)
    day_count_dict = {'day': [], 'changed_hands': []}
    date_range = pl.DataFrame({'date': date_range})
    for i in date_range['date']:
        try:
            day = i.strftime("%Y-%m-%d")
            day_count_dict['day'].append(day)

            query = f"""
            WITH block_range AS (
                SELECT min(block_number) AS min_block, max(block_number) AS max_block
                FROM block_info
                WHERE date_of_block = '{day}'
            )
            SELECT count(1)
            FROM transfer_record
            WHERE block_number >= (SELECT min_block FROM block_range)
            AND block_number <= (SELECT max_block FROM block_range)

            """
            res = pl.read_database(query=query, connection_uri=uri)
            day_count_dict['changed_hands'].append(res['count'][0])

        except Exception as ex:
            print(f'迭代出现错误：{ex}')
    day_count_df = pl.DataFrame(day_count_dict)
    print(day_count_df)
    day_count_df.write_database('transfer_contract_every_day_count', connection_uri=write_uri, if_exists='append')


def date_task():
    pass


if __name__ == '__main__':
    uri = "postgresql://dev_user:nft_project_dev220@52.89.34.220:5432/eth_nft"
    write_uri = "postgresql://postgres:nft_project123@52.89.34.220:5432/eth_nft_api"
    print(f'to locate the issue that out of memory  ,so print currency process num :{os.getpid()}')
    st = time.time()
    history()
    et = time.time()
    print(f'running program used time:{et - st}')
