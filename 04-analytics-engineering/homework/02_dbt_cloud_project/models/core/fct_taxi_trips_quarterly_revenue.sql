{{ config(materialized='table') }}

with trip_data as(
    select * from {{ ref('fact_trips') }}
),
--Compute the Quarterly Revenues for each year for based on total_amount
quarterly_revenue as(
    select 
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        CONCAT(EXTRACT(YEAR FROM pickup_datetime), '-Q', EXTRACT(QUARTER FROM pickup_datetime)) AS year_quarter,
        -- EXTRACT(MONTH FROM pickup_datetime) AS month
        service_type,
        sum(total_amount) AS revenue
    from trip_data
    where EXTRACT(YEAR FROM pickup_datetime) >= 2019
    group by service_type, year, quarter, year_quarter
),
--Compute the Quarterly YoY (Year-over-Year) revenue growth
revenue_with_lag as(
    select 
        service_type, 
        year, 
        quarter,         
        year_quarter,
        revenue,
        LAG(revenue, 4) OVER(ORDER BY service_type, year_quarter) AS prev_year_revenue
    from quarterly_revenue    
)

select service_type, year, quarter, year_quarter, revenue, prev_year_revenue,
    ROUND(
        100 * (revenue - prev_year_revenue) / NULLIF(prev_year_revenue, 0), 
        2
    ) AS yoy_growth_percentage
from revenue_with_lag
