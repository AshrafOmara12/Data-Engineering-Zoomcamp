import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
# reading the green taxi trips from January 2019

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name_1 = params.table_name_1
    table_name_2 = params.table_name_2
    # url = params.url
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    # table_name = params.table_name
    # url = params.url
    df_green_taxi = pd.read_csv('https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz', nrows=100)

    # create engine to connect to the database
    # engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # engine.connect()

    # reading dataset with zones
    df_zones = pd.read_csv('https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv')
    # df_zones.head()
    # df_zones.info()

    # create the schema of the df_zones
    df_zones.head(0).to_sql(name=table_name_2, con=engine, if_exists='replace')

    # apppend the data row to the table
    df_zones.to_sql(name=table_name_2, con=engine, if_exists='append')
    print('Ingestion green zones data is done')

    # print(pd.io.sql.get_schema(frame=df_green_taxi, name= 'green_taxi_data', con=engine))

    # take 100k each time from the df
    df_green_taxi_iter =  pd.read_csv('https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz',iterator=True, chunksize=100000)

    #  take the next 100k
    df_green_taxi = next(df_green_taxi_iter)

    #convert the datatime columns to datetime type 
    df_green_taxi.lpep_pickup_datetime = pd.to_datetime(df_green_taxi.lpep_pickup_datetime)
    df_green_taxi.lpep_dropoff_datetime = pd.to_datetime(df_green_taxi.lpep_dropoff_datetime)

    # create the schema of the green_taxi_data
    df_green_taxi.head(0).to_sql(name=table_name_1, con=engine, if_exists='replace')
    df_green_taxi.to_sql(name=table_name_1, con=engine, if_exists='append')

    # ingest the chunks 
    while True:
        try:
            time_start = time()
            df_green_taxi = next(df_green_taxi_iter)
            df_green_taxi.lpep_pickup_datetime = pd.to_datetime(df_green_taxi.lpep_pickup_datetime)
            df_green_taxi.lpep_dropoff_datetime = pd.to_datetime(df_green_taxi.lpep_dropoff_datetime)
            df_green_taxi.to_sql(name=table_name_1, con=engine, if_exists='append')
            time_end = time()
            print('time for each chunk', time_end-time_start)
        except StopIteration:
            print('The ingestion is done')
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name_1', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--table_name_2', required=True, help='name of the table where we will write the results to')

    # parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)