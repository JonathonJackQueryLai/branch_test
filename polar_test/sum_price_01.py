# @Time    : 2023/7/25 15:32
# @motto   :  rain cats and dogs
import logging
import time
import sys
import os
import polars as pl
import datetime
import smtplib
import email.message
from memory_profiler import profile

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


@profile
def weekly_report_task():
    pl.Config.set_fmt_str_lengths(100)
    pl.Config.set_tbl_rows(20)

    # 数据获取
    uri = "postgresql://dev_user:nft_project_dev220@52.89.34.220:5432/eth_nft"
    # 从pgsql中获取合约信息
    contract_info_query_sql = "select contract_address,contract_type,contract_name,contract_symbol,init_block from contract_info"
    contract_info = pl.read_database(contract_info_query_sql, uri)
    logger.info("contract_info加载成功")
    # 从pgsql中获取块信息
    block_info_query_sql = "select block_number,timestamp_of_block,date_of_block from block_info"
    block_info = pl.read_database(block_info_query_sql, uri)
    logger.info("block_info加载成功")
    # 从pgsql中获取汇率信息
    rate_info_query_sql = "select date_of_rate,eth_usd_rate from rate_info"
    rate_info = pl.read_database(rate_info_query_sql, uri)

    # 从pgsql中获取trade信息
    trade_info_query_sql = "select transaction_hash,block_number,contract_address,token_id,seller,buyer,currency_address,price_value,market from trade_record"
    trade_info = pl.read_database(trade_info_query_sql, uri)
    print("表trade_info加载成功")
    logger.info("trade_info加载成功")
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
    rate_info = rate_info.rename({'date_of_rate': 'date'})

    trade_info = trade_info.join(block_info.with_columns(pl.col('block_number')), on='block_number')
    print(trade_info.columns)
    print(block_info.columns)
    print(rate_info.columns)
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
    end_date = datetime.datetime.strptime(start_date, "%Y-%m-%d") + datetime.timedelta(days=6)
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
    trade_info = trade_info.join(contract_info, on='contract_address')

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
    ])
    print(week_trade_df_token.shape, week_trade_df_token.columns)
    print(week_trade_df_token.head(10))
    print('平均值', week_trade_df['price_value'].mean())
    print('group by  max ',week_trade_df.groupby(['contract_address', 'token_id']).max().sort('price_value'))
    print('group by  min ',week_trade_df.groupby(['contract_address', 'token_id']).min().sort('price_value'))
    print('最高价', week_trade_df_token.head(1))
    print('最低价', week_trade_df_token.tail(1))
    # .sort('price_value_avg', descending=True).head(10)[
    #     ['contract_name', 'price_value_avg', 'price_value_max', 'price_value_min']]
    logger.info(f'{week_trade_df_token}')


if __name__ == '__main__':
    if sys.argv[1]:
        start_date = sys.argv[1]
    else:
        print('there is no arg')
        os.system('exit')
    start_time = time.time()
    weekly_report_task()
    end_time = time.time()
    print(f"耗时：{end_time - start_time}")
