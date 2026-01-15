{{ config(
    materialized='table',
    alias='taxi_agg_' ~ env_var('DATA_PERIOD')
) }}

SELECT
    Run_Date_Short, 
    sum(total_amount) AS total_amount,
    CAST(count(trip_id) AS DOUBLE PRECISION) AS row_count
FROM {{ ref('yellow_trip_data') }}
GROUP BY
    Run_Date_Short