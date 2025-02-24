## Q1
### meaning of sources.yaml
-This sources.yaml defines a source in dbt, specifically for BigQuery, __allowing dbt to reference raw data in the BigQuery__ data warehouse. \
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



## Q
- Select the option that does NOT apply for materializing fct_taxi_monthly_zone_revenue: (A: dbt run --select models/staging/+)

`dbt run` :Runs all models in the project 
`dbt run --select +models/core/dim_taxi_trips.sql+ --target prod` : Runs dim_taxi_trips.sql and its dependencies (parents & children). If fct_taxi_monthly_zone_revenue depends on dim_taxi_trips.sql, it will be included.
`dbt run --select +models/core/fct_taxi_monthly_zone_revenue.sql` :Runs fct_taxi_monthly_zone_revenue along with its dependencies. 
`dbt run --select +models/core/` :Runs all models inside models/core/, which includes fct_taxi_monthly_zone_revenue.
`dbt run --select models/staging/+` :runs only models inside models/staging/ and their children. If fct_taxi_monthly_zone_revenue is in models/core/, it won't be included unless it depends directly on staging models.

- What are dependencies?  parents & children?

## Q
Say you have to modify the following dbt_model (fct_recent_taxi_trips.sql) to enable Analytics Engineers to dynamically control the date range.

In development, you want to process only the last 7 days of trips
In production, you need to process the last 30 days for analytics
 Meaning: how to dynamically control the date range in fct_recent_taxi_trips.sql based on the environment (development vs. production)?

- Method 1: use `target.name`
```
-- .sql : automatically adjust the date range based on the DBT target (dev or prod)
WITH recent_trips AS (
    SELECT *
    FROM {{ ref('stg_taxi_trips') }}
    WHERE pickup_datetime >= DATE_SUB(CURRENT_DATE(), 
        INTERVAL 
        CASE 
            WHEN target.name = 'prod' THEN 30
            ELSE 7 
        END DAY
    )
)

SELECT * FROM recent_trips;
```
running `dbt run --target prod` will automatically process 30 days, and running `dbt run` in development will use 7 days.

- method 2:
  ```
  -- .sql
  WITH recent_trips AS (
    SELECT *
    FROM {{ ref('stg_taxi_trips') }}
    WHERE pickup_datetime >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('trip_days', 7) }} DAY)
    --- comment: in default:Filter trips to only include records within the last trip_days
  )

  SELECT * FROM recent_trips;
  ```
In Development (Last 7 Days): `dbt run --select fct_recent_taxi_trips`
In Production (Last 30 Days): `dbt run --select fct_recent_taxi_trips --vars '{ "trip_days": 30 }'`

- or pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY



