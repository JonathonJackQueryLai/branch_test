import logging
import time
import sys
import os
import polars as pl
import datetime
from dateutil.relativedelta import relativedelta
import json


def weekly_report_task(start_date):
    pl.Config.set_fmt_str_lengths(100)
    pl.Config.set_tbl_rows(20)
    # 数据获取
    end_date = None
    end_date = end_date if end_date else datetime.datetime.strptime(start_date, "%Y-%m-%d") + datetime.timedelta(days=6)
    last_week_end_date = end_date + datetime.timedelta(days=-7)
    last_week_start_date = last_week_end_date + datetime.timedelta(days=-6)
    end_date = end_date.strftime("%Y-%m-%d")
    # 从pgsql中获取合约信息
    contract_info_query_sql = "select contract_address,contract_type,contract_name,contract_symbol,init_block from contract_info"
    contract_info = pl.read_database(contract_info_query_sql, uri)

    # 从pgsql中获取块信息
    block_info_query_sql = "select block_number,timestamp_of_block,date_of_block from block_info"
    block_info = pl.read_database(block_info_query_sql, uri)
    # join block dataframe, rate dataframe and trade dataframe together
    block_info = block_info.rename({'date_of_block': 'date'})
    # block_info周表
    week_block_df = block_info.filter((pl.col('date').cast(str) >= start_date) & (pl.col('date').cast(str) <= end_date))
    start_block_weekly = week_block_df.sort('block_number').head(1)['block_number'][0]
    end_block_weekly = week_block_df.sort('block_number').tail(1)['block_number'][0]
    print(start_block_weekly)
    print(end_block_weekly)

    # 从pgsql中获取汇率信息
    rate_info_query_sql = "select date_of_rate,eth_usd_rate from rate_info"
    rate_info = pl.read_database(rate_info_query_sql, uri)
    rate_info = rate_info.rename({'date_of_rate': 'date'})
    # 从pgsql中获取trade信息
    trade_info_query_sql = f"select transaction_hash,block_number,contract_address,token_id,seller,buyer,currency_address,price_value,market from trade_record where block_number <= {end_block_weekly}"
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

    week_trade_df = trade_info.filter((pl.col('block_number').cast(int) >= start_block_weekly) & (
            pl.col('block_number').cast(int) <= end_block_weekly)).sort('block_number')
    week_trade_df = week_trade_df.with_columns(
        (week_trade_df['price_value'] * week_trade_df['eth_usd_rate']).rename('price_usd'))
    week_trade_df = week_trade_df.join(contract_info, on='contract_address')
    trade_info = trade_info.join(contract_info, on='contract_address')

    holder_df_sql = '''
        SELECT contract_address, token_id,
        MAX(from_address) AS last_from_address,
        MAX(to_address) AS last_to_address
        FROM transfer_record
        GROUP BY contract_address, token_id 
        '''
    holder_df = pl.read_database(query=holder_df_sql, connection_uri=uri)
    holder_num = holder_df['last_to_address'].unique().shape[0]
    token_num = holder_df.shape[0]
    print(f'token_num:{token_num}')
    print('总持仓交易者数为：', holder_num)
    print('平均持仓数为：', token_num / holder_num)
    logger.info('\n总持仓交易者数为：%s' % holder_num)
    logger.info('平均持仓数为：%s' % (token_num / holder_num))
    week_report_dict['market_overview']["holder_data"]['number_of_wallets_holding_nfts'] = holder_num
    week_report_dict['market_overview']["holder_data"]['avg_amount_of_nfts_held_per_wallet'] = token_num / holder_num

    # 周日的日期
    end_date = datetime.datetime.strptime(start_date, "%Y-%m-%d") + datetime.timedelta(days=6)
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
    # final result
    # trade_volume_collection_df是排行榜
    trade_volume_df = week_trade_df.select(['contract_address', 'price_usd']).groupby('contract_address').sum()
    trade_volume_collection_df = trade_volume_df.join(contract_info, on='contract_address').select(
        ['contract_name', 'price_usd']).sort('price_usd', descending=True)
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


if __name__ == '__main__':
    uri = "postgresql://dev_user:nft_project_dev220@52.89.34.220:5432/eth_nft"
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
