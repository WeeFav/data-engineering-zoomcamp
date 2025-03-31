import pandas as pd
from sqlalchemy import create_engine
import argparse
import os 

def main(args):
    user = args.user
    password = args.password
    host = args.host
    port = args.port
    db = args.db
    table = args.table
    url = args.url
    
    path = './output.parquet'
    os.system(f"wget {url} -O {path}")
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # df = pd.read_parquet(path)

    # df.head(n=0).to_sql(name=table, con=engine, if_exists='replace')

    # batch_size = 100000
    # for i in range(0, len(df), batch_size):
    #     chunk = df.iloc[i: i+batch_size]
    #     chunk.to_sql(name=table, con=engine, if_exists='append')
    #     print(f"upload chunk {i} to database")
    
    df_zones = pd.read_csv("./taxi_zone_lookup.csv")
    df_zones.to_sql(name='zones', con=engine, if_exists='replace')
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--user', help='user for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table', help='table name for postgres')
    parser.add_argument('--url', help='url of the website to download data')
    args = parser.parse_args()
    main(args)
    