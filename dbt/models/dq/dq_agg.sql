{{ config(
    materialized='view',
    alias='dq_agg_' ~ env_var('DATA_PERIOD')
) }}
SELECT 
    Run_Date_Short,
    SUM(total_amount) AS total_amount,
    SUM(row_count) AS row_count
FROM {{ ref('taxi_agg') }}
GROUP BY Run_Date_Short