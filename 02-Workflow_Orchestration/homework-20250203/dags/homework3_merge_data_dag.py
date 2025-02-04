import os

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago

# Define your GCP project and dataset
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'first_dataset_tl')

TAXI_TYPES = ["yellow", "green"]
MONTHS = range(1, 13)  # January to December


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

# Define DAG. This task finished in 10 seconds.
with DAG(
    dag_id="homework3_merge_bigquery_tables",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for taxi in TAXI_TYPES:
        # Generate the merge query dynamically
        MERGE_QUERY = f"""
        CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.merged_{taxi}_table` AS
        { " UNION ALL ".join([f"SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{taxi}_2020-{month:02d}_table`" for month in MONTHS]) };
        """

        merge_task = BigQueryInsertJobOperator(
            task_id=f"merge_{taxi}_tables",
            configuration={
                "query": {
                    "query": MERGE_QUERY,
                    "useLegacySql": False,
                }
            },
            # gcp_conn_id="google_cloud_default",
            # dag=dag,
        )

        merge_task  # Single task DAG


# # Step 1: Create the merged table if it doesn't exist
# CREATE_TABLE_QUERY = f"""
# CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{DATASET_ID}.{MERGED_TABLE}` AS
# SELECT * FROM `{GCP_PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLES[0]}` WHERE FALSE;
# """

# # Step 2: Merge all tables into the new table
# MERGE_QUERY = f"""
# INSERT INTO `{GCP_PROJECT_ID}.{DATASET_ID}.{MERGED_TABLE}`
# { " UNION ALL ".join([f"SELECT * FROM `{GCP_PROJECT_ID}.{DATASET_ID}.{table}`" for table in SOURCE_TABLES]) };
# """

# # Task 1: Create merged table if not exists
# create_table_task = BigQueryInsertJobOperator(
#     task_id="create_merged_table",
#     configuration={
#         "query": {
#             "query": CREATE_TABLE_QUERY,
#             "useLegacySql": False,
#         }
#     },
#     gcp_conn_id="google_cloud_default",
#     dag=dag,
# )

# # Task 2: Insert merged data
# merge_task = BigQueryInsertJobOperator(
#     task_id="merge_tables",
#     configuration={
#         "query": {
#             "query": MERGE_QUERY,
#             "useLegacySql": False,
#         }
#     },
#     gcp_conn_id="google_cloud_default",
#     dag=dag,
# )
