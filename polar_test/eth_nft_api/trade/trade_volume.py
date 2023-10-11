#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : trade_volume.py
# @Time    : 2023/8/31 11:38
# @motto   :  rain cats and dogs

import polars as pl
from polar_test.eth_nft_api.util.db_uri import DbUri

A = pl.read_database(query='select * from day_block_number_min_max', connection_uri=DbUri.ETH_NFT_API_URI)
A = pl.read_database(query='select * from day_block_number_min_max group by  contract_address', connection_uri=DbUri.ETH_NFT_URI)
print(A)
