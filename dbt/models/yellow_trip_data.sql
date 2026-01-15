SELECT * 
FROM {{ source( 'yellow_taxi_source', 'yellow_taxi_trips' ) }}