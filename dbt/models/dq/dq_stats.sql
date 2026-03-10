{{ config(
    materialized='table',
    alias='dq_' ~ env_var('DATA_PERIOD')
) }}

SELECT
    Run_Date_Short,
    metric_name,
    metric_value
FROM {{ref('dq_agg')}}
UNPIVOT (
    metric_value FOR metric_name IN (total_amount, row_count)
)