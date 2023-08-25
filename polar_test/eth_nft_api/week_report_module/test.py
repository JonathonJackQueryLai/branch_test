#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : test.py
# @Time    : 2023/8/22 10:19
# @motto   :  rain cats and dogs

import polars as pl

# uri = "postgresql://dev_user:nft_project_dev220@52.89.34.220:5432/eth_nft"
ETH_NFT_API_uri = "postgresql://postgres:nft_project123@52.89.34.220:5432/eth_nft_api"
# holder_df_sql = '''
#         SELECT contract_address, token_id,
#         MAX(from_address) AS last_from_address,
#         MAX(to_address) AS last_to_address
#         FROM transfer_record
#         GROUP BY contract_address, token_id
#         LIMIT 10 '''
sql = "SELECT * FROM transfer_contract_every_day_count"
df = pl.read_database(query=sql, connection_uri=ETH_NFT_API_uri)

df = df.with_columns((pl.col('changed_hands').cast(int)))
print(df)
df.write_database('year_transfer_count', if_exists='replace', connection_uri=ETH_NFT_API_uri)
