from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> pd.DataFrame:
    """Download trip data from GCS"""
    file_name = f"{color}_tripdata_{year}-{month:02}.parquet"
    gcs_path = f"data/yellow/{file_name}"
    gcs_block = GcsBucket.load("nyc-taxi-datalake")
    file = gcs_block.download_object_to_path(from_path = gcs_path, to_path = f"./data/{file_name}")
    return file


@task()
def transform(file) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(file)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write transformed dataframe to Big Query"""
    gcp_credentials_block = GcpCredentials.load("nyc-taxi-creds")
    df.to_gbq(
        destination_table= 'yellowtrips.rides',
        project_id= 'nyc-taxi-402114',
        credentials = gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append")

@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data to BQ data warehouse"""
    color="yellow"
    year=2021
    month=1

    file= extract_from_gcs(color, year, month)
    df = transform(file)
    write_bq(df)

if __name__=="__main__":
    etl_gcs_to_bq()
