CREATE TABLE IF NOT EXISTS yellow_taxi_trips_{{get_data_period_ym(ds, -3)}}_stats (
    Run_Date_Short         VARCHAR(7) NOT NULL,
    metric_name            VARCHAR(50) NOT NULL,
    metric_value            DOUBLE PRECISION
)