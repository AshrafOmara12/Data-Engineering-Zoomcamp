import pandas as pd
from pathlib import Path
import os
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries= 3)
def get_data_from_source(url: str) -> pd.DataFrame:
    """get the data files from data source"""
    df = pd.read_parquet(url)
    return df

@task()
def convert_df_to_local_files(df: pd.DataFrame, color: str, data_file: str) -> Path:
    """convert the df to csv file"""
    
    data_dir = f'data/{color}'
    if not os.path.exists(f"{os.getcwd()}/{data_dir}"):
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        path = Path(f'{data_dir}/{data_file}.parquet')
        df.to_parquet(path, compression='gzip')
        return path
    else:
        path = Path(f'{data_dir}/{data_file}.parquet')
        df.to_parquet(path, compression='gzip')
        return path

@task(retries=5)
def local_files_to_gcs(path: Path) -> None:
    gcp_cloud_storage_bucket_block = GcsBucket.load("upload-to-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)

@flow(log_prints=True)
def local_to_gcs_flow(color: str, year: int, month: int) -> None:
    """transfer the data sheets from gcs to BigQuery"""
    data_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-0{month}.parquet"
    df = get_data_from_source(data_url)
    print(df.shape)
    file_name = f"{color}_tripdata_{year}-0{month}"
    file_path = convert_df_to_local_files(df, color= 'yellow',data_file= file_name)
    print(file_path)
    local_files_to_gcs(file_path)

@flow()
def local_to_gcs_flow_parent(color: str = 'yellow', year: int = 2019 ,months: list[int] = [2, 3]) -> None:
    for month in months:
        print("month: ", month)
        local_to_gcs_flow(color, year, month)
if __name__ == "__main__":
    local_to_gcs_flow_parent()