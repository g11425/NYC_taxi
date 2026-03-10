{{ config(
    materialized='table',
    alias='taxi_agg_' ~ env_var('DATA_PERIOD')
) }}

SELECT
    Run_Date_Short, 
    payment_type,
    rate_type,
    PULocation,
    DOLocation,
    Vendor,
    IsWeekend,
    IsNight,
    distance_bucket,
    sum(fare_amount) AS fare_amount,
    sum(extra) AS extra,
    sum(mta_tax) AS mta_tax,
    sum(tip_amount) AS tip_amount,
    sum(tolls_amount) AS tolls_amount,
    sum(improvement_surcharge) AS improvement_surcharge,
    sum(total_amount) AS total_amount,
    sum(congestion_surcharge) AS congestion_surcharge,
    sum(airport_fee) AS airport_fee,
    sum(cbd_congestion_fee) AS cbd_charge,
    count(trip_id) AS row_count
FROM {{ ref('yellow_trip_data') }}
GROUP BY
    Run_Date_Short, payment_type, rate_type, PULocation, DOLocation, Vendor, IsWeekend, IsNight, distance_bucket