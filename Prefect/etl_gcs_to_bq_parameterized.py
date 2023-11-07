from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("nyc-taxi-datalake")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Transform parquet to df"""
    df = pd.read_parquet(path)
    return df

@task()
def write_bq(df: pd.DataFrame,dest_tab: str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("nyc-taxi-creds")

    df.to_gbq(
        destination_table= dest_tab,
        project_id= 'nyc-taxi-402114',
        credentials = gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )
    return len(df)

@flow()
def etl_gcs_to_bq(color:str,year:int,month:int,dest_tab:str):
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df,dest_tab)
    

@flow(log_prints=True)
def etl_bq_flow(
    months: list[int] = [1,2], year: int = 2021, color: str = "yellow", dest_tab: str = "yellowtrips.rides"
):
    
    for month in months:
       etl_gcs_to_bq(color,year,month,dest_tab)
        
if __name__ == '__main__':
    color = "yellow"
    months = [2,3]
    year = 2019
    dest_tab = "yellowtrips.rides"
    etl_bq_flow(months,year,color,dest_tab)
  
