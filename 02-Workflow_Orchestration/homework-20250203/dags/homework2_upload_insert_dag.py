import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pandas as pd

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'first_dataset_tl')

# Constants
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
YEARS = [2020]
MONTHS = range(1, 13)  # January to December
TAXI_TYPES = ["yellow", "green"]
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# Define DAG. This TaskGroup runs around 19 mins.
# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="homework2_upload_insert_grouptask",
    # schedule_interval="@daily",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for taxi in TAXI_TYPES:
        for year in YEARS:
            for month in MONTHS:
                with TaskGroup(group_id=f"{taxi}_{year}_{month:02d}") as tg:

                    download_unzip_task = BashOperator(
                        task_id=f"download_unzip_{taxi}_{year}_{month:02d}",
                        bash_command=f"""
                        curl -sSL {BASE_URL}/{taxi}/{taxi}_tripdata_{year}-{month:02d}.csv.gz > {path_to_local_home}/{taxi}_tripdata_{year}-{month:02d}.csv.gz  && \
                        gunzip -c {path_to_local_home}/{taxi}_tripdata_{year}-{month:02d}.csv.gz > {path_to_local_home}/{taxi}_tripdata_{year}-{month:02d}.csv
                        """
                        # or simplify code
                        # bash_command=f"""
                        # FILE={path_to_local_home}/{taxi}_tripdata_{year}-{month:02d}.csv.gz && \
                        # curl -sSL {BASE_URL}/{taxi}/{taxi}_tripdata_{year}-{month:02d}.csv.gz > $FILE && \
                        # gunzip -c $FILE > ${FILE%.gz}
                        # """
                    )

                    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
                    local_to_gcs_task = PythonOperator(
                        task_id=f"local_to_gcs_{taxi}_{year}_{month:02d}",
                        python_callable=upload_to_gcs,
                        op_kwargs={
                            "bucket": BUCKET,
                            "object_name": f"raw/{taxi}_tripdata_{year}-{month:02d}.csv",
                            "local_file": f"{path_to_local_home}/{taxi}_tripdata_{year}-{month:02d}.csv",
                        },
                    )

                    # Task to load CSV from GCS to BigQuery
                    load_csv_to_bq = GCSToBigQueryOperator(
                        task_id=f"load_csv_to_bq_{taxi}_{year}_{month:02d}",
                        bucket=BUCKET,
                        source_objects=[
                            f"raw/{taxi}_tripdata_{year}-{month:02d}.csv"],
                        destination_project_dataset_table=f'{PROJECT_ID}.{BIGQUERY_DATASET}.{taxi}_{year}-{month:02d}_table',
                        skip_leading_rows=1,
                        source_format='CSV',
                        write_disposition='WRITE_TRUNCATE',
                        field_delimiter=',',
                    )

                    download_unzip_task >> local_to_gcs_task >> load_csv_to_bq
