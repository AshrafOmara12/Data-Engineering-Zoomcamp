import pandas as pd
import os
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import tqdm
from google.cloud import bigquery

@task(retries=3, log_prints= True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    gcs_path = f"data\{color}\{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("upload-to-gcs")
    gcs_block.get_directory(from_path=gcs_path)
    return Path(f"{gcs_path}")



@task(log_prints= True)
def read_files(path: Path) -> pd.DataFrame:
    """read files as dataframe"""
    df = pd.read_parquet(path)
    print(df.shape)
    return df

@task(log_prints= True)
def from_local_to_big_query(df)-> None:
    
#     gcp_credentials_block = GcpCredentials.load("upload-to-gcp-cred")
#     chunk_size = 500000
#     df.to_gbq(
#     destination_table="trips_data_all.yellow_taxi_data_2019_Feb",
#     project_id="gtc-flow-375407",
#     credentials=gcp_credentials_block.get_credentials_from_service_account(),
#     chunksize=chunk_size,
#     if_exists="append",
#     progress_bar = True,
# ) 
    print(df.shape)
    client = bigquery.Client()
    job = client.load_table_from_dataframe(
             df, 'gtc-flow-375407.trips_data_all.yellow_taxi_data_2019_Feb' )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table('gtc-flow-375407.trips_data_all.yellow_taxi_data_2019_Feb')  # Make an API request.
    print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), 'gtc-flow-375407.trips_data_all.yellow_taxi_data_2019_Feb'
            )
        )


@flow(log_prints= True)
def upload_data_to_bq(color: str, year: int, month: int):
    """take the files from gcs to bq"""
    file_path = extract_from_gcs(color, year, month)
    df = read_files(file_path)
    from_local_to_big_query(df)

@flow(log_prints= True)
def main_flow_to_bg(color:str ="yellow", year: int = 2019, months: list[int] = [2,3]) -> None:
    """get data from gcs to bq"""
    for month in months:
        upload_data_to_bq(color, year, month)

if __name__ =="__main__":
    main_flow_to_bg()