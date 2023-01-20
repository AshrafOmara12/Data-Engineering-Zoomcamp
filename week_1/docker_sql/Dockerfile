FROM  python:3.9

RUN pip install pandas sqlalchemy psycopg2
WORKDIR  /app
COPY ingestion_data.py ingestion_data.py


ENTRYPOINT [ "python", "ingestion_data.py" ]