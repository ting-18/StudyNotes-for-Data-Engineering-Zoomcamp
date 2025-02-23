##Q1
### meaning of sources.yaml
-This sources.yaml defines a source in dbt, specifically for BigQuery, allowing dbt to reference raw data in the BigQuery data warehouse. \
Defines a source (raw_nyc_tripdata) → This allows dbt to refer to existing raw data.
```
version: 2 # Specifies the YAML schema version (used in dbt sources & models)

sources:
  - name: raw_nyc_tripdata  # Logical name for the source, referenced in dbt 
                            # as source('raw_nyc_tripdata', 'ext_green_taxi')
    database: "{{ env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025') }}"
    schema:   "{{ env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata') }}"
    tables:
      - name: ext_green_taxi  # Table name inside the schema
      - name: ext_yellow_taxi
```
- Uses Environment Variables (env_var) for Flexibility:
    - DBT_BIGQUERY_PROJECT → Specifies the BigQuery project (defaults to dtc_zoomcamp_2025 if not set).
    - DBT_BIGQUERY_SOURCE_DATASET → Defines the schema/dataset where raw data is stored (defaults to raw_nyc_tripdata).
