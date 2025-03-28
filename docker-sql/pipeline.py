import pandas as pd
df = pd.read_parquet("./yellow_tripdata_2021-01.parquet")
print(pd.io.sql.get_schema(df, name='yellow_taxi_data'))