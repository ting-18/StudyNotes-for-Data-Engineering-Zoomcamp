{{ config(materialized='table') }}

with trip_data as(
    select * from {{ ref('fact_trips') }} 
),
-- Filtering data
filtered_trips as(
    select 
        service_type, 
        fare_amount,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
    from trip_data
    where (fare_amount > 0) 
        and (trip_distance > 0) 
        and (payment_type_description in ('Cash', 'Credit card'))
    order by service_type, year, month, fare_amount
)
-- Computing continous percentile via window function OVER():
SELECT
    service_type, 
    year, 
    month,
    -- PERCENTILE_CONT(0.97) WITHIN GROUP (ORDER BY fare_amount) 
    --     OVER (PARTITION BY service_type, year, month) AS fare_p97,
    -- Syntax error: In BigQuery, the WITHIN GROUP clause is not supported 
     --for PERCENTILE_CONT in the same way it is in some other SQL databases
    -- you'll need to structure it without WITHIN GROUP in the window context.
    PERCENTILE_CONT(fare_amount, 0.97)   --calculates the 97th percentile of the fare_amount.
        OVER (PARTITION BY service_type, year, month)
        AS fare_p97,
    PERCENTILE_CONT(fare_amount, 0.95) 
        OVER (PARTITION BY service_type, year, month) 
        AS fare_p95,
    PERCENTILE_CONT(fare_amount, 0.90) 
        OVER (PARTITION BY service_type, year, month) 
        AS fare_p90
FROM filtered_trips

-- SELECT DISTINCT service_type, year, month, fare_p97 
