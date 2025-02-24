## Q1: understanding dbt model resolution
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
    - meaning: If DBT_BIGQUERY_PROJECT is set → dbt will use its value. \
      If DBT_BIGQUERY_PROJECT is not set → dbt will use 'dtc_zoomcamp_2025' as the default value. \
      The same applies to DBT_BIGQUERY_SOURCE_DATASET, which defaults to 'raw_nyc_tripdata2'.
- How to set Environment Variables?
    - Method 1: Linux/MacOS (Bash/Zsh) `export DBT_BIGQUERY_PROJECT=my_project`
    - Method 2: Windows(PowerShell) `$env:DBT_BIGQUERY_PROJECT="my_project"`
    - Method 3: .env File (if using dbt Cloud or dotenv), in this .env file: DBT_BIGQUERY_PROJECT=my_project
- Why use env_var?
  Makes it configurable across different environments (e.g., dev, staging, prod). \
  Prevents hardcoding project/schema names. \
  Allows you to set values via the command line or .env files.

## Q2: dbt Variables and Dynamic Models
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

- explanation of options
  - meaning of "command line arguments takes precedence over ENV_VARs, which takes precedence over DEFAULT value?"
    - Precedence order（优先顺序）: Command-line arguments (--vars): Highest priority > Environment variables (env_var()):Used if no CLI argument > Default values (hardcoded in dbt): Only used if nothing else is set. __This is also called "standard precedence order" in dbt__
      - Command-line arguments (--vars) have the highest priority: If a variable is passed via the dbt CLI (--vars), it will override everything else.
      - Environment variables (env_var()) come next: If a variable isn’t provided via the command line, dbt will check if it exists as an environment variable.
      - Default values (hardcoded in dbt) have the lowest priority: If neither a CLI argument nor an environment variable is found, dbt will use the default value defined in the project.

  - pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY 
    - Command-line arguments (--vars) take precedence → var("days_back", …); If not set, it checks the environment variable (DAYS_BACK) → env_var("DAYS_BACK", …); If neither is provided, it defaults to 30 days
    - How Precedence Works:
      - If dbt run --vars '{ "days_back": 7 }' is used → days_back = 7
      - If no --vars is provided but DAYS_BACK=15 is set in the environment → days_back = 15
      - If neither is set → defaults to 30
  - how to code when ENV_VAR takes precedence over command line arguments, which takes precedence over DEFAULT value?
     ``` pickup_datetime >= CURRENT_DATE - INTERVAL '{{ env_var("DAYS_BACK", var("days_back", "30")) }}' DAY ```
    
## Q3: dbt Data Lineage and Execution
- Considering the data lineage below and that taxi_zone_lookup is the only materialization build (from a .csv seed file). Select the option that does NOT apply for materializing fct_taxi_monthly_zone_revenue: (A: dbt run --select models/staging/+) \
![img](notesimages/01_01.png)

`dbt run` :Runs all models in the project 
`dbt run --select +models/core/dim_taxi_trips.sql+ --target prod` : Runs dim_taxi_trips.sql and its dependencies (parents & children). If fct_taxi_monthly_zone_revenue depends on dim_taxi_trips.sql, it will be included.
`dbt run --select +models/core/fct_taxi_monthly_zone_revenue.sql` :Runs fct_taxi_monthly_zone_revenue along with its dependencies. 
`dbt run --select +models/core/` :Runs all models inside models/core/, which includes fct_taxi_monthly_zone_revenue.
`dbt run --select models/staging/+` :runs only models inside models/staging/ and their children. If fct_taxi_monthly_zone_revenue is in models/core/, it won't be included unless it depends directly on staging models.

- What are dependencies?  parents & children?




## Q

