FROM python:3.8

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data_homework.py  ingest_data_homework.py

ENTRYPOINT [ "python", "ingest_data_homework.py" ]