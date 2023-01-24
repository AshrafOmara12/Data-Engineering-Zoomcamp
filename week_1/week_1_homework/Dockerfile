FROM python:3.9.1
RUN pip install pandas sqlalchemy psycopg2-binary
WORKDIR /app
COPY green_taxi_ingestion_code.py green_taxi_ingestion_code.py
ENTRYPOINT [ "python","green_taxi_ingestion_code.py" ]