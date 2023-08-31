start_date = '2023-06-21'
end_date ='2023-07-21'

from collections import OrderedDict
import polars as pl
import concurrent.futures
from concurrent.futures import as_completed
ETH_NFT_API_URI = "postgresql://postgres:nft_project123@52.89.34.220:5432/eth_nft_api"
uri = "postgresql://dev_user:nft_project_dev220@52.89.34.220:5432/eth_nft"
min_weekly = pl.read_database(
            f"select min_block_num from day_block_number_min_max where date = '{start_date}' ",
            connection=ETH_NFT_API_URI)['min_block_num'][0]
max_weekly = pl.read_database(f"select max_block_num from day_block_number_min_max where date = '{end_date}' ",
                                      connection=ETH_NFT_API_URI)['max_block_num'][0]
sql_li = [
f"select date_of_rate as date ,eth_usd_rate  from rate_info where date_of_rate >= '{start_date}' and date_of_rate <= '{end_date}' ",
f"select contract_address,contract_name from contract_info ",
f"select block_number,date_of_block as date from block_info where block_number >=  {min_weekly} and  block_number <=  {max_weekly}",
f"select block_number,contract_address,price_value as volume_eth from trade_record where  block_number >= {min_weekly} and block_number <= {max_weekly}  "

]
from collections import OrderedDict


def exec_sql(sql, uri):
    df = pl.read_database(query=sql, connection=uri)
    return df
try:
    # 创建线程池
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # 提交任务到线程池
        futures = [executor.submit(exec_sql, sql, uri) for sql in sql_li]

        results = OrderedDict()

        # 获取任务结果并按顺序存储到有序字典中
        for sql, future in zip(['rate_info',
                                'contract_info',
                                'block_info',
                                'trade_info'
                                ], futures):
            result = future.result()
            results[sql] = result
except Exception as ex:
    print(ex)


rate_info = results['rate_info']
contract_info = results['contract_info']
block_info = results['block_info']
trade_info = results['trade_info']
trade_info = trade_info.join(block_info, on="block_number", how="left")
trade_info = trade_info.join(rate_info, on="date", how="left")
trade_info = trade_info.with_columns(
    (trade_info["volume_eth"] * trade_info["eth_usd_rate"]).rename('volume_usd'))
trade_info
trade_info = trade_info.join(contract_info, on="contract_address", how="inner")
