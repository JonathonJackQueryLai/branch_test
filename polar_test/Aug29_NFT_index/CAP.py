#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : CAP.py
# @Time    : 2023/8/29 16:08
# @motto   :  rain cats and dogs

data1 = pd.read_csv(r"C:\Users\jingt\Desktop\working\Aug22_NFTcap\trade_SupDucks.csv")

data, link = data_preprocessing(data1,'all','all',0.01)
ts, nft_eth, nft_usd, ethR2, usdR2 =  repeat_sale_regression(data_SD, link_SD)
ret = NFT_mrk_ret(ts, nft_eth, nft_usd)


plt.figure(figsize=(10, 4))
plt.plot(ts, nft_usd)
plt.title('SupDucks Market Index (USD)')