import pandas as pd
from sqlalchemy import create_engine 
from time import time
import argparse


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # engine.connect()
    # print(pd.io.sql.get_schema(frame=yellow_taxi, name= 'yellow_taxi_data', con=engine))

    yellow_taxi_iter = pd.read_csv(f'{url}',iterator= True, chunksize= 100000)

    yellow_taxi = next(yellow_taxi_iter)


    yellow_taxi.tpep_pickup_datetime = pd.to_datetime(yellow_taxi.tpep_pickup_datetime)
    yellow_taxi.tpep_dropoff_datetime = pd.to_datetime(yellow_taxi.tpep_dropoff_datetime)

    yellow_taxi.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    yellow_taxi.to_sql(name=table_name, con=engine, if_exists='append')


    while True:
        t_start = time()
        yellow_taxi = next(yellow_taxi_iter)
        yellow_taxi.tpep_pickup_datetime = pd.to_datetime(yellow_taxi.tpep_pickup_datetime)
        yellow_taxi.tpep_dropoff_datetime = pd.to_datetime(yellow_taxi.tpep_dropoff_datetime)
        yellow_taxi.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print('time for the chunck', t_end - t_start)

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="ingest csv data to postgres")
    parser.add_argument("--user", help="username for the postgres")
    parser.add_argument("--password", help="password for the postgres")
    parser.add_argument("--host", help="host for the postgres")
    parser.add_argument("--port", help="port for the postgres")
    parser.add_argument("--db", help="databae name of the postgres")
    parser.add_argument("--table_name", help="name of the table that we write the csv in the database")
    parser.add_argument("--url", help="url of the csv file")

    args = parser.parse_args()
    main(args)