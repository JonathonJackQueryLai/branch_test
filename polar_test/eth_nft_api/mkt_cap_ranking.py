import logging
import time
import sys
import os
import polars as pl
import datetime
from dateutil.relativedelta import relativedelta


# from memory_profiler import profile

# from eth_nft_data_module.task.schedulers import run_scheduler


# @profile
def market_cp(start_date):
    pl.Config.set_fmt_str_lengths(100)
    pl.Config.set_tbl_rows(20)
    # 周日的日期
    end_date = datetime.datetime.strptime(start_date, "%Y-%m-%d") + datetime.timedelta(days=6)
    # end_date = end_date - datetime.timedelta(days=30) * 3
    # end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    # 数据获取
    uri = "postgresql://dev_user:nft_project_dev220@52.89.34.220:5432/eth_nft"
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
    st = time.time()
    transfer_info_query_sql = f"select transaction_hash,contract_address,from_address,to_address,token_id,block_number from transfer_record where block_number >= {start_block_weekly} and block_number <= {end_block_weekly}"
    transfer_info = pl.read_database(transfer_info_query_sql, uri)
    et = time.time()
    print("transfer_info加载成功")
    # 计算读取最大的表的时间为多少很重要
    print(f'read transfer_record used time :{et - st}')

    week_transfer_df = transfer_info.filter((pl.col('block_number').cast(int) >= start_block_weekly) & (
            pl.col('block_number').cast(int) <= end_block_weekly)).sort('block_number')

    # 全量计算
    token_num_df = transfer_info.groupby(['contract_address', 'token_id']).count()

    marketcap_df = contract_info.join(token_num_df, on='contract_address').join(avg_price_df, on='contract_address')
    print(marketcap_df.columns)
    market_cap_eth = marketcap_df.with_columns((marketcap_df['count'] * marketcap_df['price_value']).rename('market_cap_eth'))
    print(market_cap_eth.shape)
    market_cap_eth.sort(by=marketcap_df['price_value'])


if __name__ == '__main__':
    print(f'to locate the issue that out of memory  ,so print currency process num :{os.getpid()}')
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

    # file_handler = logging.FileHandler(f'./log/app_{datetime.datetime.now().strftime("%Y_%m_%d_%H%M%S_%f")}.log')
    file_handler = logging.FileHandler(
        f'/home/project/logs/weekreport_log/logs/week_report_rank_{start_date}.log')
    file_handler.setFormatter(formatter)

    # 将处理器添加到日志记录器中
    logger.addHandler(file_handler)
    start_time = time.time()
    market_cp(start_date)
    end_time = time.time()
    print(f"耗时：{end_time - start_time}")
