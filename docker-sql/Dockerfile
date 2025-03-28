FROM python:3.9
RUN pip install pandas pyarrow sqlalchemy
WORKDIR /app
COPY pipeline.py pipeline.py
COPY yellow_tripdata_2021-01.parquet yellow_tripdata_2021-01.parquet
ENTRYPOINT [ "bash" ]