from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(retries=3)
def fetch(dataset_url: str, service: str) -> pd.DataFrame:
    """Read data from web into pandas DataFrame. Enforce data types."""
    if service == "yellow":
        df = pd.read_csv(dataset_url).astype({
            "VendorID": "Int64",
            "tpep_pickup_datetime": "object",
            "tpep_dropoff_datetime": "object",
            "store_and_fwd_flag": "object",
            "RatecodeID": "Int64",
            "PULocationID": "Int64",
            "DOLocationID": "Int64",
            "payment_type": "Int64",
            "passenger_count": "Int64",
            "trip_distance": "Float64",
            "fare_amount": "Float64",
            "extra": "Float64",
            "mta_tax": "Float64",
            "tip_amount": "Float64",
            "tolls_amount": "Float64",
            "improvement_surcharge": "Float64",
            "total_amount": "Float64",
            "congestion_surcharge": "Float64" 
         })
        return df
    elif service == "green":
        df = pd.read_csv(dataset_url).astype({
            "VendorID": "Int64",
            "lpep_pickup_datetime": "object",
            "lpep_dropoff_datetime": "object",
            "store_and_fwd_flag": "object",
            "RatecodeID": "Int64",
            "PULocationID": "Int64",
            "DOLocationID": "Int64",
            "passenger_count": "Int64",
            "trip_distance": "Float64",
            "fare_amount": "Float64",
            "extra": "Float64",
            "mta_tax": "Float64",
            "tip_amount": "Float64",
            "tolls_amount": "Float64",
            "ehail_fee": "Float64",
            "improvement_surcharge": "Float64",
            "total_amount": "Float64",
            "payment_type": "Int64",
            "trip_type": "Int64",
            "congestion_surcharge": "Float64" 
         })
        return df
    elif service == "fhv":
        df = pd.read_csv(dataset_url).astype({
            "dispatching_base_num": "object",
            "pickup_datetime": "object",
            "dropOff_datetime": "object",
            "PUlocationID": "Int64",
            "DOlocationID": "Int64",
            "SR_Flag": "Int64",
            "Affiliated_base_number": "object"
        })
        return df

@task(log_prints=True)
def clean(df: pd.DataFrame, service: str) -> pd.DataFrame:
    """Fix dtype issues"""
    if service == "yellow":
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
        return df
    elif service == "green":
        df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
        df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
        return df  
    elif service == "fhv":
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
        return df


@task()
def write_local(df: pd.DataFrame, service: str, dataset_file:str) -> Path:
    """Write dataframe as parquet file """
    path = Path(f"data/{service}/{dataset_file}.parquet").as_posix()
    df.to_parquet(path, compression = 'gzip')
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload parquet file to GCS"""
    gcs_block = GcsBucket.load("nyc-taxi-datalake")
    gcs_block.upload_from_path(
        from_path =f"{path}",
        to_path = path
    )
    return

@flow()
def etl_web_to_gcs(year: int, month: int, service: str) -> None:
    """The main ETL function"""
    dataset_file = f"{service}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/{dataset_file}.csv.gz"

    df = fetch(dataset_url, service)
    df_clean = clean(df, service)
    path = write_local(df_clean, service, dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow(
    months: list[int] = [1,2], year: int = 2021, service: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(year, month, service)

if __name__ == '__main__':
    service = "yellow"
    months = [1,2,3]
    year = 2019
    etl_parent_flow(months,year,service)
  
