{{ config(materialized='table') }}

with trip_data as(
    select *,
       TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) as trip_duration
    from {{ref("dim_fhv_trips")}}
)
-- Compute the continous p90 of trip_duration partitioning 
-- by year, month, pickup_locationid, and dropoff_locationid
select year, month, pickup_locationid, dropoff_locationid, pickup_zone, dropoff_zone,
    PERCENTILE_CONT(trip_duration, 0.9) OVER(PARTITION BY year, month, pickup_locationid, dropoff_locationid) AS p90
from trip_data