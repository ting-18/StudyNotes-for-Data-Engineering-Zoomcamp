import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pandas as pd

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_file = "yellow_tripdata_2021-03.csv.gz"
dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{dataset_file}"
csv_file = dataset_file.replace('.csv.gz', '.csv')
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# cleaned_dataset_file = dataset_file.replace('.csv', '_cleaned.csv')
# parquet_file = dataset_file.replace('.csv.gz', '.parquet') if dataset_file.endswith(
#     '.csv.gz') else dataset_file.replace('.csv', '.parquet')
# parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'first_dataset_tl')


# def format_to_parquet(dataset_file, parquet_file):
#     # Read the gzipped CSV file
#     table = pv.read_csv(
#         dataset_file, read_options=pv.ReadOptions(encoding='utf-8'))
#     # table = pv.read_csv(dataset_file)  ##This code works too!
#     pq.write_table(table, parquet_file)

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed


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

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="homework1_data_ingestion_gcs_dag",
    # schedule_interval="@daily",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_unzip_task = BashOperator(
        task_id="download_unzip_task",
        bash_command=f"""
        curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}  && \
        gunzip -c {path_to_local_home}/{dataset_file} > {path_to_local_home}/{csv_file}
        """
    )

    # format_to_parquet_task = PythonOperator(
    #     task_id="format_to_parquet_task",
    #     python_callable=format_to_parquet,
    #     op_kwargs={
    #         "dataset_file": f"{path_to_local_home}/{dataset_file}",
    #         "parquet_file": f"{path_to_local_home}/{parquet_file}",
    #     },
    # )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{csv_file}",
            "local_file": f"{path_to_local_home}/{csv_file}",
        },
    )

    # Task to load CSV from GCS to BigQuery
    load_csv_to_bq = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        bucket=BUCKET,
        source_objects=[f"raw/{csv_file}"],
        destination_project_dataset_table=f'{PROJECT_ID}.{BIGQUERY_DATASET}.yellow_2021-03_table',
        skip_leading_rows=1,
        source_format='CSV',
        write_disposition='WRITE_TRUNCATE',
        field_delimiter=',',
    )

    download_unzip_task >> local_to_gcs_task >> load_csv_to_bq
