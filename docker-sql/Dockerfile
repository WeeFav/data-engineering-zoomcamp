FROM python:3.9
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt
WORKDIR /app
COPY ingest.py ingest.py
COPY taxi_zone_lookup.csv taxi_zone_lookup.csv