#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : weekly_report_task_test.py
# @Time    : 2023/7/25 15:32
# @motto   :  rain cats and dogs
import logging
import time
import sys
import os
import polars as pl
import datetime
from dateutil.relativedelta import relativedelta
import json


# @profile
def weekly_report_task(start_date):
    pl.Config.set_fmt_str_lengths(100)
    pl.Config.set_tbl_rows(20)
    # 周日的日期
    end_date = datetime.datetime.strptime(start_date, "%Y-%m-%d") + datetime.timedelta(days=6)
    # end_date = end_date - datetime.timedelta(days=30) * 3
    # end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")

    week_report_dict['date']['year'] = datetime.datetime.strptime(start_date, "%Y-%m-%d").year
    week_report_dict['date']['month'] = datetime.datetime.strptime(start_date, "%Y-%m-%d").month
    week_report_dict['date']['day'] = datetime.datetime.strptime(start_date, "%Y-%m-%d").day
    week_report_dict['period']['start'] = '.'.join(start_date.split('-')[1:])
    week_report_dict['period']['end'] = end_date.strftime("%m.%d")
    end_date = end_date.strftime("%Y-%m-%d")
    # 数据获取
    uri = "postgresql://dev_user:nft_project_dev220@52.89.34.220:5432/eth_nft"
    # 从pgsql中获取合约信息
    contract_info_query_sql = "select contract_address,contract_type,contract_name,contract_symbol,init_block from contract_info"
    contract_info = pl.read_database(contract_info_query_sql, uri)

    # 从pgsql中获取块信息
    block_info_query_sql = "select block_number,timestamp_of_block,date_of_block as date from block_info"
    block_info = pl.read_database(block_info_query_sql, uri)
    # join block dataframe, rate dataframe and trade dataframe together
    # block_info = block_info.rename({'date_of_block': 'date'})
    # block_info周表
    week_block_df = block_info.filter((pl.col('date').cast(str) >= start_date) & (pl.col('date').cast(str) <= end_date))
    start_block_weekly = week_block_df.sort('block_number').head(1)['block_number'][0]
    end_block_weekly = week_block_df.sort('block_number').tail(1)['block_number'][0]
    print(start_block_weekly)
    print(end_block_weekly)

    # 从pgsql中获取汇率信息
    rate_info_query_sql = "select date_of_rate as date,eth_usd_rate from rate_info"
    rate_info = pl.read_database(rate_info_query_sql, uri)
    # 从pgsql中获取trade信息
    trade_info_query_sql = f"select transaction_hash,block_number,contract_address,token_id,seller,buyer,currency_address,price_value from trade_record where block_number <= {end_block_weekly}"
    trade_info = pl.read_database(trade_info_query_sql, uri)
    print("表trade_info加载成功")

    print('00000000000000000000000000 从Pgsql获取数据成功.')
    # set the currency filter 美元的单位
    currency_list = [
        '0x0000000000000000000000000000000000000000',
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    ]
    # filter the trade data
    trade_info = trade_info.filter(
        pl.col('currency_address').is_in(currency_list)
    )

    trade_info = trade_info.join(block_info.with_columns(pl.col('block_number')), on='block_number')

    trade_info = trade_info.filter(pl.col('block_number') <= end_block_weekly)
    trade_info = trade_info.join(rate_info, on='date')
    trade_info = trade_info.with_columns(
        pl.col('price_value').cast(float),
        pl.col('eth_usd_rate').cast(float)
    )

    # calulate the trade price in USD
    trade_info = trade_info.with_columns((trade_info['price_value'] * trade_info['eth_usd_rate']).rename('price_usd'))

    # 提取数据
    # 周数据dataframe用于计算周榜
    # 全数据dataframe用于计算总榜

    # 时间为从当前任务执行之间到7天前, 时间跨度为一周
    # start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    # end_date = datetime.datetime.today().strftime('%Y-%m-%d')
    # 改为使用接受start_date的入参来启动脚本

    # 算的不对
    # start_block_weekly = week_block_df.head(1)['block_number'][0]
    # end_block_weekly = week_block_df.tail(1)['block_number'][0]
    # 修改后
    # start_block_weekly = week_block_df.sort('block_number').head(1)['block_number'][0]
    # end_block_weekly = week_block_df.sort('block_number').tail(1)['block_number'][0]
    # print(start_block_weekly)
    # print(end_block_weekly)
    week_trade_df = trade_info.filter((pl.col('block_number').cast(int) >= start_block_weekly) & (
            pl.col('block_number').cast(int) <= end_block_weekly)).sort('block_number')
    week_trade_df = week_trade_df.with_columns(
        (week_trade_df['price_value'] * week_trade_df['eth_usd_rate']).rename('price_usd'))
    week_trade_df = week_trade_df.join(contract_info, on='contract_address')
    trade_info = trade_info.join(contract_info, on='contract_address')

    avg_price_df = trade_info.select(['contract_address', 'price_value', 'price_usd']).groupby(
        'contract_address').mean()
    # ---------------------------------------------------从pgsql中获取transfer信息------------------------------------
    # where block_number >= {start_block_weekly} and block_number <= {end_block_weekly}
    # WHERE block_number <= {end_block_weekly}

    week_transfer_df = pl.read_database(
        query=f"select * from transfer_record where block_number >= {start_block_weekly} and  block_number <= {end_block_weekly} order by block_number",connection_uri=uri)
    # week_transfer_df = transfer_info.filter((pl.col('block_number').cast(int) >= start_block_weekly) & (
    #         pl.col('block_number').cast(int) <= end_block_weekly)).sort('block_number')

    # 全量计算
    # token_num_df = transfer_info.groupby(['contract_address', 'token_id']).count()
    token_num_df = pl.read_database(f"select contract_address, token_id, count(*) from transfer_record group by contract_address, token_id where block_number>= 17959394 and block_number <= 18007049", connection_uri=uri)
    marketcap_df = contract_info.join(token_num_df, on='contract_address').join(avg_price_df, on='contract_address')
    print(marketcap_df)
    market_cap_eth = round((marketcap_df['count'] * marketcap_df['price_value']).sum(), 2)
    market_cap_usd = round((marketcap_df['count'] * marketcap_df['price_usd']).sum(), 2)

    logger.info(
        '**************************************************第一部分**************************************************')

    # ------------------------------------ 截至周末的总市值 ETH  USD
    market_cap_result = f"截至{end_date},总市值为:{market_cap_eth}ETH/{market_cap_usd}USD, 由{trade_info.groupby(['contract_address']).count().shape[0]}个Collection贡献, 由{token_num_df.shape[0]}个nft贡献"
    week_report_dict['market_overview']["trading_data"]["total_market_cap"]["eth"] = market_cap_eth
    week_report_dict['market_overview']["trading_data"]["total_market_cap"]["usd"] = market_cap_usd
    logger.info(market_cap_result)

    logger.info(
        '**************************************************第二部分**************************************************')
    # 周交易量排行榜计算
    trade_volume_df = week_trade_df.select(['contract_address', 'price_usd']).groupby('contract_address').sum()
    # trade_volume_df_eth = week_trade_df['price_value'].sum()
    # 周交易量nft贡献值
    trade_volume_df_nft = week_trade_df.groupby(['contract_address', 'token_id']).count().shape[0]
    trade_info_df_nft = trade_info.groupby(['contract_address', 'token_id']).count().shape[0]

    # final result
    # trade_volume_collection_df是排行榜
    trade_volume_collection_df = trade_volume_df.join(contract_info, on='contract_address').select(
        ['contract_name', 'price_usd']).sort('price_usd', descending=True)
    trade_volume_market_df = week_trade_df.select(['market', 'price_usd']).groupby('market').sum().sort('price_usd',
                                                                                                        descending=True)

    # -----week trade的交易量
    print(f'{start_date}至{end_date},交易量合计为:',
          f"{week_trade_df['price_value'].sum()}ETH",
          f"{round(trade_volume_df['price_usd'].sum(), 2)}USD",
          f"由{trade_info.groupby(['contract_address']).count().shape[0]}个Collection贡献",
          f'由{trade_volume_df_nft}个nft贡献')
    logger.info(
        f"{start_date}至{end_date},交易量合计为:{week_trade_df['price_value'].sum()}ETH/{round(trade_volume_df['price_usd'].sum(), 2)}USD,由{week_trade_df.groupby(['contract_address']).count().shape[0]}个Collection贡献,由{trade_volume_df_nft}个nft贡献")
    print(f'截至{end_date},交易量合计为:',
          f"{trade_info['price_value'].sum()}ETH",
          f"{round(trade_info['price_usd'].sum(), 2)}USD",
          f"由{trade_info.groupby(['contract_address']).count().shape[0]}个Collection贡献",
          f'由{trade_info_df_nft}个nft贡献')
    logger.info(
        f"截至{end_date},交易量合计为:{trade_info['price_value'].sum()}ETH/{round(trade_info['price_usd'].sum(), 2)}USD,由{trade_info.groupby(['contract_address']).count().shape[0]}个Collection贡献,由{trade_info_df_nft}个nft贡献")

    week_report_dict['market_overview']["trading_data"]["total_trading_volume"]["eth"] = trade_info['price_value'].sum()
    week_report_dict['market_overview']["trading_data"]["total_trading_volume"]["usd"] = round(
        trade_info['price_usd'].sum(), 2)
    week_report_dict['market_overview']["trading_data"]["new_volume_of_this_week"]["eth"] = week_trade_df[
        'price_value'].sum()
    week_report_dict['market_overview']["trading_data"]["new_volume_of_this_week"]["usd"] = round(
        trade_volume_df['price_usd'].sum(), 2)
    week_report_dict['market_overview']["trading_data"]["new_volume_of_this_week"]["total_collection"] = \
    week_trade_df.groupby(['contract_address']).count().shape[0]
    week_report_dict['market_overview']["trading_data"]["new_volume_of_this_week"]["total_nfts"] = trade_volume_df_nft

    logger.info(
        '**************************************************第三部分**************************************************')
    logger.info(
        f'------------------------------------本周transfer交易次数总量计算------------------------------------\n')
    week_transfer_count = week_transfer_df.shape[0]
    print(f'(transfer)本周交易次数总量为 {week_transfer_count} 次')

    week_report_dict['market_overview']["trading_data"]['new_transfer_of_this_week']["times"] = week_transfer_count
    logger.info(f'(transfer)本周交易次数总量为 {week_transfer_count} 次')
    logger.info(
        f'------------------------------------all time截止到周末transfer交易次数总量计算-------------------------------\n')
    # all-time 交易次数总量计算

    all_time_transfer_count = pl.read_database("select count(1) from transfer_record", connection_uri=uri)['count'][0]
    print(f'截止至{end_date}交易次数总量为 {all_time_transfer_count} 次')

    logger.info(f'截止至{end_date}交易次数总量为 {all_time_transfer_count} 次')
    week_report_dict['market_overview']["trading_data"]['total_transfer_times']["times"] = all_time_transfer_count
    logger.info(
        f'------------------------------------本周trade交易次数总量计算---------------------------------------\n')
    week_trade_count = week_trade_df.shape[0]
    print(f'(trade)本周交易次数总量为 {week_trade_count} 次')
    week_report_dict['market_overview']["trading_data"]['new_trade_of_this_week']["times"] = week_trade_count

    logger.info(f'(trade)本周交易次数总量为 {week_trade_count} 次')
    logger.info(
        f'------------------------------------all time trade交易次数总量计算--------------------------------------\n')
    # all-time 交易次数总量计算
    all_time_trade_count = trade_info.shape[0]
    print(f'截止至{end_date}trade交易次数总量为 {all_time_trade_count} 次')
    week_report_dict['market_overview']["trading_data"]['total_trade_times']["times"] = all_time_trade_count
    logger.info(f'截止至{end_date}trade交易次数总量为 {all_time_trade_count} 次')
    logger.info(
        '**************************************************第四部分**************************************************')
    print('Token级别本周排行榜如下:')
    logger.info('\nToken级别本周排行榜如下:')

    week_trade_df_token = week_trade_df.with_columns(
        week_trade_df['price_value'].rename('price_value_avg'),
        week_trade_df['price_value'].rename('price_value_max'),
        week_trade_df['price_value'].rename('price_value_min'),
    ).groupby(['contract_address', 'token_id']).agg([
        pl.col('contract_name').first(),
        pl.mean('price_value_avg'),
        pl.max('price_value_max'),
        pl.min('price_value_min'),
        pl.count()
    ]).sort('price_value_avg', descending=True)[
        ['contract_name', 'price_value_avg', 'price_value_max', 'price_value_min']]
    print(f'{week_trade_df_token.head(10)}')
    # Token级别本周市场的平均值
    print('week市场的平均值price_value', week_trade_df['price_value'].mean())
    week_trade_df_token1 = \
        week_trade_df.groupby(['contract_address', 'token_id']).max().sort('price_value', descending=True)[
            ['contract_name', 'price_value']]
    group_by_min = week_trade_df_token1.filter(pl.col('price_value') > 0).tail(1)
    print(f'group_by_min:{group_by_min}')
    logger.info(f'Token级别本周市场最高价：{week_trade_df_token.head(1)}')
    logger.info(f"Token级别本周市场最低价：{group_by_min}")
    logger.info(f"Token级别本周市场平均值:{week_trade_df['price_value'].mean()}")

    week_report_dict['market_overview']["strike_price_of_this_week"]['max']['price_value'] = \
    week_trade_df_token.head(1)['price_value_max'][0]
    week_report_dict['market_overview']["strike_price_of_this_week"]['max']['contract_name'] = \
    week_trade_df_token.head(1)['contract_name'][0]
    week_report_dict['market_overview']["strike_price_of_this_week"]['avg']["price_value"] = week_trade_df[
        'price_value'].mean()
    week_report_dict['market_overview']["strike_price_of_this_week"]['min']['price_value'] = \
    group_by_min['price_value'][0]
    week_report_dict['market_overview']["strike_price_of_this_week"]['min']['contract_name'] = \
    group_by_min['contract_name'][0]

    print('Token级别all time排行榜如下:')
    logger.info('Token级别all time排行榜如下:')
    trade_info_token = trade_info.with_columns(
        trade_info['price_value'].rename('price_value_avg'),
        trade_info['price_value'].rename('price_value_max'),
        trade_info['price_value'].rename('price_value_min'),
    ).groupby(['contract_address', 'token_id']).agg([
        pl.col('contract_name').first(),
        pl.mean('price_value_avg'),
        pl.max('price_value_max'),
        pl.min('price_value_min'),
        pl.count()
    ]).sort('price_value_avg', descending=True)[
        ['contract_name', 'price_value_avg', 'price_value_max', 'price_value_min']]
    trade_info_token1 = \
        trade_info.groupby(['contract_address', 'token_id']).max().sort('price_value', descending=True)[
            ['contract_name', 'price_value']]
    group_by_max = trade_info_token1.head(1)
    group_by_min = trade_info_token1.filter(pl.col('price_value') > 0).tail(1)
    print(f'all_time_group_by_max:{group_by_max}')
    print(f'all_time_group_by_min:{group_by_min}')
    print(f'group_by_max:{trade_info_token.head(1)}')
    print(f'group_by_min:{trade_info_token.tail(1)}')
    print('Token级别all_time市场平均值', trade_info['price_value'].mean())

    logger.info(f'all_time token级别市场最高价:{group_by_max}')
    logger.info(f'all_time token级别市场最低价:{group_by_min}')
    logger.info(f"Token级别all_time市场平均值:{trade_info['price_value'].mean()}")

    week_report_dict['market_overview']["strike_price"]['max']['price_value'] = group_by_max['price_value'][0]
    week_report_dict['market_overview']["strike_price"]['max']['contract_name'] = group_by_max['contract_name'][0]
    week_report_dict['market_overview']["strike_price"]['avg']["price_value"] = trade_info['price_value'].mean()
    week_report_dict['market_overview']["strike_price"]['min']['price_value'] = group_by_min['price_value'][0]
    week_report_dict['market_overview']["strike_price"]['min']['contract_name'] = group_by_min['contract_name'][0]

    logger.info(
        '**************************************************第五部分**************************************************')

    # 周初次交易统计计算
    before_trade_set = set(
        trade_info.filter(pl.col('block_number').cast(int) < start_block_weekly)['contract_address'].unique())
    week_tradet_set = set(week_trade_df['contract_address'].unique())
    print('本周新增交易Collection数为：%s' % len(week_tradet_set - before_trade_set))
    logger.info('\n本周新增交易Collection数为：%s' % len(week_tradet_set - before_trade_set))
    week_report_dict['market_overview']["strike_price_of_this_week"]['initial_trading_projects_of_this_week'] = \
        len(week_tradet_set - before_trade_set)
    # 周初次Mint统计计算
    print('本周初次Mint的Collection数为：', contract_info.filter(pl.col('init_block') >= start_block_weekly).shape[0])
    logger.info('\n本周初次Mint的Collection数为：%s' %
                contract_info.filter(pl.col('init_block') >= start_block_weekly).shape[0])
    week_report_dict['market_overview']["strike_price_of_this_week"]['initial_minting_projects_of_this_week'] = \
    contract_info.filter(pl.col('init_block') >= start_block_weekly).shape[0]
    # all time NFT持仓者数统计
    # holder_df = transfer_info.groupby(['contract_address', 'token_id']).agg(
    #     pl.col('from_address').last(),
    #     pl.col('to_address').last()
    # )
    holder_df_sql = f'''
    SELECT contract_address, token_id,
    MAX(from_address) AS last_from_address,
    MAX(to_address) AS last_to_address,
    count(to_address)
    FROM transfer_record
    GROUP BY contract_address, token_id
    '''
    holder_df = pl.read_database(query=holder_df_sql, connection_uri=uri)
    holder_num = holder_df['last_to_address'].unique().shape[0]
    holder_num = pl.read_database(f''' 
    SELECT COUNT(last_to_address)
    FROM transfer_record
    GROUP BY contract_address, token_id,last_to_address
    ''')

    token_num = holder_df.shape[0]
    print(f'token_num:{token_num}')
    print('总持仓交易者数为：', holder_num)
    print('平均持仓数为：', token_num / holder_num)
    logger.info('\n总持仓交易者数为：%s' % holder_num)
    logger.info('平均持仓数为：%s' % (token_num / holder_num))
    week_report_dict['market_overview']["holder_data"]['number_of_wallets_holding_nfts'] = holder_num
    week_report_dict['market_overview']["holder_data"]['avg_amount_of_nfts_held_per_wallet'] = token_num / holder_num

    # 活跃交易者计算 计算口径按三个月日期相减

    last_three_month_date = datetime.datetime.strptime(end_date, "%Y-%m-%d") + relativedelta(months=-3)
    last_three_month_date = last_three_month_date.strftime("%Y-%m-%d")
    active_block = \
        block_info.filter(pl.col('date').cast(str) < last_three_month_date).tail(1)['block_number'][0]
    active_df = trade_info.groupby(['contract_address', 'token_id']).agg(
        pl.col('seller').last(),
        pl.col('buyer').last(),
        pl.col('block_number').last()
    ).filter(
        pl.col('block_number').cast(int) > active_block
    )

    active_traders = list(set(active_df['seller']) | set(active_df['buyer']))

    active_holder_df = holder_df.filter(
        pl.col('last_to_address').is_in(active_traders)
    )

    print('活跃交易者的数量为：', len(active_traders))
    print('活跃交易者的平均持仓数为：', active_holder_df.shape[0] / len(active_traders))
    logger.info('活跃交易者的数量为：%s' % len(active_traders))
    logger.info('活跃交易者的平均持仓数为：%s' % (active_holder_df.shape[0] / len(active_traders)))
    week_report_dict['market_overview']["holder_data"]['number_of_active_wallets'] = len(active_traders)
    week_report_dict['market_overview']["holder_data"]['avg_amount_of_nfts_held_by_active_wallet'] = \
    active_holder_df.shape[0] / len(active_traders)
    logger.info(
        '**************************************************第六部分**************************************************')
    print(f'collection级别本周交易量排行榜为：')
    logger.info(f'collection级别本周交易量排行榜为：')
    trade_volume_collection_df_head = trade_volume_collection_df.head(10)
    print(trade_volume_collection_df_head)
    logger.info(f'{trade_volume_collection_df_head}')

    week_report_dict['leaderboards']['trading_volume_top_list'][
        'weekly_ranking'] = trade_volume_collection_df_head.to_dicts()

    # all time 交易量排行榜计算
    trade_volume_df = trade_info.select(['contract_address', 'price_usd']).groupby('contract_address').sum()

    # final result
    trade_volume_collection_df = trade_volume_df.join(contract_info, on='contract_address').select(
        ['contract_name', 'price_usd']).sort('price_usd', descending=True)
    trade_volume_market_df = trade_info.select(['market', 'price_usd']).groupby('market').sum().sort('price_usd',
                                                                                                     descending=True)
    print(f'collection级别all-time交易量排行榜为：')
    trade_volume_collection_df_head = trade_volume_collection_df.head(10)
    print(trade_volume_collection_df_head)
    logger.info(f'collection级别all-time交易量排行榜为：')
    logger.info(f'{trade_volume_collection_df_head}')
    week_report_dict['leaderboards']['trading_volume_top_list'][
        'overall_ranking'] = trade_volume_collection_df_head.to_dicts()

    logger.info(
        '**************************************************第七部分**************************************************')

    # print('Token级别交易本周次数排行榜为：')
    # print(week_trade_df.groupby(['contract_address','token_id']).count().join(contract_df,on='contract_address').select(['contract_name','token_id','count']).sort('count',descending=True).head(10))

    # 周成交价格计算
    print('Collection级别本周排行榜如下:')
    logger.info('\nCollection级别本周排行榜如下:')
    week_trade_df_collection = week_trade_df.with_columns(
        week_trade_df['price_value'].rename('price_value_avg'),
        week_trade_df['price_value'].rename('price_value_max'),
        week_trade_df['price_value'].rename('price_value_min'),
    ).groupby('contract_address').agg([
        pl.col('contract_name').first(),
        pl.mean('price_value_avg'),
        pl.max('price_value_max'),
        pl.min('price_value_min'),
        pl.count()
    ]).sort('price_value_avg', descending=True).head(10)[
        ['contract_name', 'price_value_avg', 'price_value_max', 'price_value_min', 'count']]

    logger.info(week_trade_df_collection)
    week_report_dict['leaderboards']['strike_price_top_list'][
        'weekly_ranking'] = week_trade_df_collection.to_dicts()
    # all time 成交价格计算
    print('Collection级别all time排行榜如下:')
    logger.info('\nCollection级别all time排行榜如下:')
    trade_info_collection_time = trade_info.with_columns(
        trade_info['price_value'].rename('price_value_avg'),
        trade_info['price_value'].rename('price_value_max'),
        trade_info['price_value'].rename('price_value_min'),
    ).groupby('contract_address').agg([
        pl.col('contract_name').first(),
        pl.mean('price_value_avg'),
        pl.max('price_value_max'),
        pl.min('price_value_min'),
        pl.count()
    ]).sort('price_value_avg', descending=True).head(10)[
        ['contract_name', 'price_value_avg', 'price_value_max', 'price_value_min', 'count']]
    print(trade_info_collection_time)
    logger.info(f'{trade_info_collection_time}')
    week_report_dict['leaderboards']['strike_price_top_list'][
        'overall_ranking'] = trade_info_collection_time.to_dicts()
    logger.info(
        '**************************************************第八部分**************************************************')
    # all time 交易次数计算
    print('Collection级别all-time交易次数排行榜为：')
    trade_info_head = trade_info.groupby('contract_address').count().join(contract_info, on='contract_address').select(
        ['contract_name', 'count']).sort('count', descending=True).head(10)
    print(trade_info_head)
    logger.info('Collection级别all-time交易次数排行榜为：')
    logger.info(f'{trade_info_head}')

    week_report_dict['leaderboards']['collection_turnover_rate_top_list'][
        'overall_ranking'] = trade_info_collection_time.to_dicts()
    # 周交易次数计算
    print('Collection级别本周交易次数排行榜为：')
    week_trade_df_head = week_trade_df.groupby('contract_address').count().join(contract_info,
                                                                                on='contract_address').select(
        ['contract_name', 'count']).sort('count', descending=True).head(10)
    print(week_trade_df_head)
    week_report_dict['leaderboards']['collection_turnover_rate_top_list'][
        'weekly_ranking'] = week_trade_df_head.to_dicts()

    logger.info('\nCollection级别本周交易次数排行榜为：')
    logger.info(f'{week_trade_df_head}')

    print(week_report_dict)
    with open(
            f'/home/project/eth_nft_data_module/weekreport/eth_nft_api/week_report_module/template_json/{start_date}.json',
            mode='w') as fp:
        json.dump(week_report_dict, fp)


if __name__ == '__main__':
    print(f'to locate the issue that out of memory  ,so print currency process num :{os.getpid()}')
    # fp = open('./template_json/base.json', mode='r')
    with open('/home/project/eth_nft_data_module/weekreport/eth_nft_api/week_report_module/template_json/base.json',
              mode='r') as fp:
        week_report_dict = json.load(fp)
    if sys.argv[1]:
        start_date = sys.argv[1]
    else:
        print('lack of start_date args')
        os.system('exit')

    logging.basicConfig(level=logging.INFO)
    # 创建格式化器
    formatter = logging.Formatter('%(message)s')

    # 创建日志记录器
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # 创建文件处理器，并将格式化器添加到处理器中

    # file_handler = logging.FileHandler(f'../{datetime.datetime.now().strftime("%Y_%m_%d_%H%M%S_%f")}.log')
    file_handler = logging.FileHandler(
        f'/home/project/logs/weekreport_log/logs/week_report_start_{start_date}.log')
    file_handler.setFormatter(formatter)

    # 将处理器添加到日志记录器中
    logger.addHandler(file_handler)
    start_time = time.time()
    weekly_report_task(start_date)
    end_time = time.time()
    print(f"耗时：{end_time - start_time}")
