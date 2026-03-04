{{config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['Run_Date_Short'],
    alias='yellow_taxi_analytics'
)}}

SELECT 
    * 
FROM {{ ref('taxi_agg') }}

