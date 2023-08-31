import polars as pl

# li = ['2023-02-11', '2023-02-12', '2023-02-13', '2023-02-14', '2023-02-15', '2023-02-16', '2023-02-17', '2023-02-18',
#       '2023-02-19', '2023-02-20', '2023-02-21', '2023-02-22', '2023-02-23', '2023-02-24', '2023-02-25', '2023-02-26',
#       '2023-02-27', '2023-02-28', '2023-03-01', '2023-03-02', '2023-03-03', '2023-03-04', '2023-03-05', '2023-03-06',
#       '2023-03-07', '2023-03-08', '2023-03-09', '2023-03-10', '2023-03-11', '2023-03-12', '2023-03-13', '2023-03-14',
#       '2023-03-15', '2023-03-16', '2023-03-17', '2023-03-18', '2023-08-01', '2023-08-02', '2023-08-03', '2023-08-04',
#       '2023-08-05', '2023-08-06', '2023-08-16', '2023-08-17']

li = ['2023-08-18', '2023-08-19', '2023-08-20', '2023-08-21', '2023-08-22', '2023-08-23', '2023-08-24']


sql = f"""
  SELECT contract_address, token_id,
MAX(from_address) AS last_from_address,
MAX(to_address) AS last_to_address,
count(to_address)
FROM transfer_record
GROUP BY contract_address, token_id
  """
holder_df = pl.read_database(query=sql,
                      connection="postgresql://dev_user:nft_project_dev220@52.89.34.220:5432/eth_nft")

holder_num = holder_df['last_to_address'].unique().shape[0]
holder_df.write_csv('/home/project/logs/weekreport_log/logs/holder_num.csv')

print(holder_num)
