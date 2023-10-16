FROM python:3.9

RUN apt-get install curl 
RUN pip install pandas sqlalchemy psycopg2 argparse pyarrow

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python", "ingest_data.py"]
