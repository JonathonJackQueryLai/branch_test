#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : Jonathon
# @File    : all_in_limit.py
# @Time    : 2023/7/25 9:19
# @motto   :  rain cats and dogs
import logging
import time

import polars as pl
import datetime
import smtplib
import email.message

# from eth_nft_data_module.task.schedulers import run_scheduler

logging.basicConfig(filename='app.log', level=logging.INFO)
# 创建格式化器
formatter = logging.Formatter('%(message)s')


# 创建日志记录器
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 创建文件处理器，并将格式化器添加到处理器中

# file_handler = logging.FileHandler(f'./log/app_{datetime.datetime.now().strftime("%Y_%m_%d_%H%M%S_%f")}.log')
file_handler = logging.FileHandler(
    f'/home/project/logs/weekreport_log/logs/app_{datetime.datetime.now().strftime("%Y_%m_%d_%H%M%S_%f")}.log')
file_handler.setFormatter(formatter)

# 将处理器添加到日志记录器中
logger.addHandler(file_handler)


def weekly_report_task():
    # 数据获取
    uri = "postgresql://dev_user:nft_project_dev220@52.89.34.220:5432/eth_nft"
    # 从pgsql中获取合约信息
    contract_info_query_sql = "select * from contract_info"
    contract_info = pl.read_database(contract_info_query_sql, uri)
    print("表contract_info加载成功")
    # 从pgsql中获取块信息
    block_info_query_sql = """
    SELECT b.block_number,b.date_of_block,b.timestamp_of_block
    FROM block_info b
INNER JOIN trade_record tr on b.block_number = tr.block_number
order by date_of_block desc
limit 500"""
    block_info = pl.read_database(block_info_query_sql, uri)
    print("表block_info加载成功")

    # 从pgsql中获取汇率信息
    rate_info_query_sql = "select date_of_rate, eth_usd_rate,date_of_rate from rate_info"
    rate_info = pl.read_database(rate_info_query_sql, uri)
    print("表rate_info加载成功")
    # 从pgsql中获取transfer信息
    transfer_info_query_sql = """
        select *
        from trade_record  where block_number >= 17207219
        and block_number <= 17199460
        limit 500
            """
    transfer_info = pl.read_database(transfer_info_query_sql, uri)
    print("表transfer_info加载成功")
    # 从pgsql中获取trade信息
    trade_info_query_sql = """
    select t.transaction_hash, t.block_number, t.contract_address, t.token_id, t.seller, t.buyer, t.currency_address, t.price_value, t.market from block_info b
right outer join trade_record t on b.block_number = t.block_number
order by date_of_block desc
limit 500
    """
    trade_info = pl.read_database(trade_info_query_sql, uri)
    print("表trade_info加载成功")
    print('00000000000000000000000000 从Pgsql获取数据成功.')
    # set the currency filter
    currency_list = [
        '0x0000000000000000000000000000000000000000',
        '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2',
    ]

    # filter the trade data
    trade_info = trade_info.filter(
        pl.col('currency_address').is_in(currency_list)
    )

    # join block dataframe, rate dataframe and trade dataframe together
    block_info = block_info.rename({'date_of_block': 'date'})
    print("block_info.columns:", block_info.columns)
    rate_info = rate_info.rename({'date_of_rate': 'date'})
    print("rate_info.columns:", rate_info.columns)

    trade_info = trade_info.join(block_info.with_columns(pl.col('block_number')), on='block_number')

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
    start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
    end_date = datetime.datetime.today().strftime('%Y-%m-%d')

    start_date = '2023-04-30'
    end_date = '2023-05-07'
    week_block_df = block_info.filter((pl.col('date').cast(str) >= start_date) & (pl.col('date').cast(str) <= end_date))
    start_block_weekly = week_block_df.head(1)['block_number'][0]
    end_block_weekly = week_block_df.tail(1)['block_number'][0]
    print(start_block_weekly)
    print(end_block_weekly)
    week_trade_df = trade_info.filter((pl.col('block_number').cast(int) >= start_block_weekly) & (
            pl.col('block_number').cast(int) <= end_block_weekly)).sort('block_number')
    week_trade_df = week_trade_df.with_columns(
        (week_trade_df['price_value'] * week_trade_df['eth_usd_rate']).rename('price_usd'))

    week_trade_df = week_trade_df.join(contract_info, on='contract_address')
    week_transfer_df = transfer_info.filter((pl.col('block_number').cast(int) >= start_block_weekly) & (
            pl.col('block_number').cast(int) <= end_block_weekly)).sort('block_number')

    trade_info = trade_info.join(contract_info, on='contract_address')

    # 总市值计算
    avg_price_df = trade_info.select(['contract_address', 'price_value', 'price_usd']).groupby(
        'contract_address').mean()
    token_num_df = transfer_info.groupby(['contract_address', 'token_id']).count()
    marketcap_df = contract_info.join(token_num_df, on='contract_address').join(avg_price_df, on='contract_address')

    market_cap_eth = round((marketcap_df['count'] * marketcap_df['price_value']).sum(), 2)
    market_cap_usd = round((marketcap_df['count'] * marketcap_df['price_usd']).sum(), 2)

    market_cap_result = f'截至{end_date},总市值为:{market_cap_eth}ETH/{market_cap_usd}USD'
    print(market_cap_result)
    logger.info(f' {market_cap_result}')
    # 周交易量排行榜计算
    trade_volume_df = week_trade_df.select(['contract_address', 'price_usd']).groupby('contract_address').sum()

    # final result
    trade_volume_collection_df = trade_volume_df.join(contract_info, on='contract_address').select(
        ['contract_name', 'price_usd']).sort('price_usd', descending=True)
    trade_volume_market_df = week_trade_df.select(['market', 'price_usd']).groupby('market').sum().sort('price_usd',
                                                                                                        descending=True)

    print(f'{start_date}至{end_date},交易量合计为:', round(trade_volume_df['price_usd'].sum(), 2),
          f'USD,由{trade_volume_df.shape[0]}个Collection贡献')
    print(f'collection级别本周交易量排行榜为：')
    trade_volume_collection_df_head = trade_volume_collection_df.head(10)
    print(trade_volume_collection_df_head)

    logger.info(f'{start_date}至{end_date},交易量合计为:', round(trade_volume_df['price_usd'].sum(), 2),
                f'USD,由{trade_volume_df.shape[0]}个Collection贡献')
    logger.info(f'collection级别本周交易量排行榜为：')
    logger.info(f'{trade_volume_collection_df_head}')
    # print(f'market级别本周交易量排行榜为：')
    # print(trade_volume_market_df.head(10))

    # all time 交易量排行榜计算
    trade_volume_df = trade_info.select(['contract_address', 'price_usd']).groupby('contract_address').sum()

    # final result
    trade_volume_collection_df = trade_volume_df.join(contract_info, on='contract_address').select(
        ['contract_name', 'price_usd']).sort('price_usd', descending=True)
    trade_volume_market_df = trade_info.select(['market', 'price_usd']).groupby('market').sum().sort('price_usd',
                                                                                                     descending=True)

    print(f'截至{end_date},交易量合计为:', round(trade_volume_df['price_usd'].sum(), 2),
          f'USD,由{trade_volume_df.shape[0]}个Collection贡献')
    print(f'collection级别all-time交易量排行榜为：')
    trade_volume_collection_df_head = trade_volume_collection_df.head(10)
    print(trade_volume_collection_df_head)

    logger.info(f'截至{end_date},交易量合计为:', round(trade_volume_df['price_usd'].sum(), 2),
                f'USD,由{trade_volume_df.shape[0]}个Collection贡献')
    logger.info(f'collection级别all-time交易量排行榜为：')
    logger.info(f'{trade_volume_collection_df_head}')

    # print(f'market级别all-time交易量排行榜为：')
    # print(trade_volume_market_df.head(10))

    # 周交易次数计算
    print('Collection级别本周交易次数排行榜为：')
    week_trade_df_head = week_trade_df.groupby('contract_address').count().join(contract_info,
                                                                                on='contract_address').select(
        ['contract_name', 'count']).sort('count', descending=True).head(10)
    print(week_trade_df_head)
    logger.info('\nCollection级别本周交易次数排行榜为：')
    logger.info(f'{week_trade_df_head}')
    # print('Token级别交易本周次数排行榜为：')
    # print(week_trade_df.groupby(['contract_address','token_id']).count().join(contract_df,on='contract_address').select(['contract_name','token_id','count']).sort('count',descending=True).head(10))

    # all time 交易次数计算
    print('Collection级别all-time交易次数排行榜为：')
    trade_info_head = trade_info.groupby('contract_address').count().join(contract_info, on='contract_address').select(
        ['contract_name', 'count']).sort('count', descending=True).head(10)
    print(trade_info_head)
    logger.info('\nCollection级别all-time交易次数排行榜为：')
    logger.info(f'{trade_info_head}')
    # print('Token级别all-time交易次数排行榜为：')
    # print(trade_df.groupby(['contract_address','token_id']).count().join(contract_df,on='contract_address').select(['contract_name','token_id','count']).sort('count',descending=True).head(10))

    # 周交易次数总量计算
    week_trade_count = week_transfer_df.shape[0]
    print(f'本周交易次数总量为 {week_trade_count} 次')
    logger.info(f'本周交易次数总量为 {week_trade_count} 次')

    # all-time 交易次数总量计算
    all_time_trade_count = transfer_info.shape[0]
    print(f'截止至{end_date}交易次数总量为 {all_time_trade_count} 次')
    logger.info(f'截止至{end_date}交易次数总量为 {all_time_trade_count} 次')

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
        ['contract_name', 'price_value_avg', 'price_value_max', 'price_value_min']]

    print(week_trade_df_collection)
    logger.info(week_trade_df_collection)

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
    ]).sort('price_value_avg', descending=True).head(10)[
        ['contract_name', 'price_value_avg', 'price_value_max', 'price_value_min']]
    print(f'{week_trade_df_token}')
    logger.info(f'{week_trade_df_token}')

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
        ['contract_name', 'price_value_avg', 'price_value_max', 'price_value_min']]
    print(trade_info_collection_time)
    logger.info(f'{trade_info_collection_time}')

    print('Token级别all time排行榜如下:')
    # logger.info('\nToken级别all time排行榜如下:')
    trade_info.with_columns(
        trade_info['price_value'].rename('price_value_avg'),
        trade_info['price_value'].rename('price_value_max'),
        trade_info['price_value'].rename('price_value_min'),
    ).groupby(['contract_address', 'token_id']).agg([
        pl.col('contract_name').first(),
        pl.mean('price_value_avg'),
        pl.max('price_value_max'),
        pl.min('price_value_min'),
        pl.count()
    ]).sort('price_value_avg', descending=True).head(10)[
        ['contract_name', 'price_value_avg', 'price_value_max', 'price_value_min']]

    # 周初次交易统计计算
    before_trade_set = set(
        trade_info.filter(pl.col('block_number').cast(int) < start_block_weekly)['contract_address'].unique())
    week_tradet_set = set(week_trade_df['contract_address'].unique())
    print('本周新增交易Collection数为：', len(week_tradet_set - before_trade_set))
    logger.info('\n本周新增交易Collection数为：', len(week_tradet_set - before_trade_set))

    # 周初次Mint统计计算

    print('本周初次Mint的Collection数为：', contract_info.filter(pl.col('init_block') >= start_block_weekly).shape[0])
    logger.info('\n本周初次Mint的Collection数为：',
                contract_info.filter(pl.col('init_block') >= start_block_weekly).shape[0])

    # all time NFT持仓者数统计
    holder_df = transfer_info.groupby(['contract_address', 'token_id']).agg(
        pl.col('from_address').last(),
        pl.col('to_address').last()
    )
    holder_num = holder_df['to_address'].unique().shape[0]
    token_num = holder_df.shape[0]
    print('总持仓交易者数为：', holder_num)
    print('平均持仓数为：', token_num / holder_num)
    logger.info('\n总持仓交易者数为：', holder_num)
    logger.info('平均持仓数为：', token_num / holder_num)
    # 活跃交易者计算
    active_block = block_info.filter(pl.col('date') <= '2023-04-30').tail(1)['block_number'][0]
    active_df = trade_info.groupby(['contract_address', 'token_id']).agg(
        pl.col('seller').last(),
        pl.col('buyer').last(),
        pl.col('block_number').last()
    ).filter(
        pl.col('block_number').cast(int) > active_block
    )

    active_traders = list(set(active_df['seller']) | set(active_df['buyer']))
    active_holder_df = holder_df.filter(
        pl.col('to_address').is_in(active_traders)
    )

    print('活跃交易者的数量为：', len(active_traders))
    print('活跃交易者的平均持仓数为：', active_holder_df.shape[0] / len(active_traders))
    logger.info('活跃交易者的数量为：', len(active_traders))
    logger.info('活跃交易者的平均持仓数为：', active_holder_df.shape[0] / len(active_traders))
    # 发送邮件
    # send_email()


def send_email():
    # 寄送139邮箱邮件的程序
    # 准备通讯模块设定
    msg = email.message.EmailMessage()
    msg["From"] = "15711996135@139.com"
    # msg["To"] = "vivizhang0916@gmail.com"
    msg["To"] = "838061964@qq.com"
    msg["Subject"] = "测试python发邮件"
    # 寄送纯文字的内容
    msg.set_content("<这是一封来自139邮箱由python程序自动发送的邮件>")
    # 寄送多样式内容(html)
    # msg.add_alternative("<H3>优惠券</H3>满五百送二百哦", subtype="html")
    # 连线到SMTP SERVER,验证寄件人身份并发送邮件

    # 到网上搜索Email的服务器设定
    server = smtplib.SMTP_SSL("smtp.139.com", 465)
    # 这里的登录账号就是邮箱账号，登录密码是"客户端授权码"
    server.login("15711996135@139.com", "61b95d016e3859b69500")
    server.send_message(msg)
    server.close()


# run_scheduler(weekly_report_task, 'cron', hour=10)

if __name__ == '__main__':

    start_time = time.time()
    weekly_report_task()
    end_time = time.time()
    print(f"耗时：{end_time - start_time}")
