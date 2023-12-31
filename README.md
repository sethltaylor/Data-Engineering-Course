# Data-Engineering-Course
Repo for code and resources as I work through [DataTalksClub's Data Engineering course](https://github.com/DataTalksClub/data-engineering-zoomcamp). 

Week 1: Dockerizing ingestion of data into Postgres database, intro to google cloud platform, setting up GCP infrastructure wtih terraform 

Week 2: Workflow orchestration with Prefect 
- Utilize Prefect flows and tasks to orchestrate script to ingest data into Postgres database.
- Write Python script that uses Prefect that ETLs data from the web to a Google Cloud Storage bucket.
- Incorporate Prefect blocks within scripts to store credential and configuration data to interface with external systems.
- Write Python script that extracts data from GCS cloud storage bucket, transforms data, and uploads it to Big Query.
- Create deployments with Prefect by pushing scripts to a docker container and this github to allow reguarly scheduled runs of data pipeline instead of always running locally. 
