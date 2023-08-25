import polars as pl

# li = ['2023-02-11', '2023-02-12', '2023-02-13', '2023-02-14', '2023-02-15', '2023-02-16', '2023-02-17', '2023-02-18',
#       '2023-02-19', '2023-02-20', '2023-02-21', '2023-02-22', '2023-02-23', '2023-02-24', '2023-02-25', '2023-02-26',
#       '2023-02-27', '2023-02-28', '2023-03-01', '2023-03-02', '2023-03-03', '2023-03-04', '2023-03-05', '2023-03-06',
#       '2023-03-07', '2023-03-08', '2023-03-09', '2023-03-10', '2023-03-11', '2023-03-12', '2023-03-13', '2023-03-14',
#       '2023-03-15', '2023-03-16', '2023-03-17', '2023-03-18', '2023-08-01', '2023-08-02', '2023-08-03', '2023-08-04',
#       '2023-08-05', '2023-08-06', '2023-08-16', '2023-08-17']

li = ['2023-08-18', '2023-08-19', '2023-08-20', '2023-08-21', '2023-08-22', '2023-08-23', '2023-08-24']

for i in li:
    sql = f"""
      WITH block_range AS (
      SELECT min(block_number) AS min_block, max(block_number) AS max_block
          FROM block_info
          WHERE date_of_block = '{i}'
      )
      SELECT count(1)
      FROM transfer_record
      WHERE block_number >= (SELECT min_block FROM block_range)
      AND block_number <= (SELECT max_block FROM block_range)
      """
    df = pl.read_database(query=sql,
                          connection_uri="postgresql://dev_user:nft_project_dev220@52.89.34.220:5432/eth_nft")

    print(i, df['count'][0])
