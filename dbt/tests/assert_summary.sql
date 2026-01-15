SELECT * 
FROM {{ ref('dq_stats') }} as a
LEFT JOIN {{ source('yellow_taxi_source', 'yellow_taxi_trips_stats') }} as b
    USING (Run_Date_Short, metric_name)
WHERE ROUND(a.metric_value, 4) != ROUND(b.metric_value, 4)
