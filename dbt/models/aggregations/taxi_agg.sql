{{ config(
    materialized='table',
    alias='taxi_agg_' ~ env_var('DATA_PERIOD')
) }}

SELECT 
    sum(fare_amount) AS total_fare_amount,
    sum(tip_amount) AS total_tip_amount,
    count(trip_id) AS total_trips,
    avg(trip_distance) AS avg_trip_distance
FROM {{ ref('yellow_trip_data') }}