# NYC Taxi ELT Pipeline

End-to-end ELT pipeline for NYC Yellow Taxi data using Airflow, Spark (EMR), Redshift, and dbt.

## What this project does

- Fetches monthly NYC taxi parquet data from TLC-hosted cloudfront.
- Stores raw data in S3 under a date-partitioned path.
- Runs Spark ETL on EMR to clean, enrich, and derive features.
- Writes processed parquet + data quality metrics back to S3.
- Loads curated data into Redshift tables.
- Runs dbt models and tests on top of Redshift-loaded tables.

The main orchestration DAG is `airflow/etl_flow.py` with `dag_id="NYC_taxi_flow"`.

## Pipeline flow

Current DAG task order:

1. `fetch_data_to_s3`
2. `setup_conf_in_s3`
3. `upload_bootstrap_to_s3`
4. `create_cluster`
5. `add_step`
6. `wait_for_step`
7. `terminate_cluster`
8. `redshift_create_table`
9. `redshift_create_extras_table`
10. `copy_to_redshfit`
11. `copy_extras_to_redshfit`
12. `run_dbt_transforms`
13. `run_dbt_tests`

Schedule: `0 1 1 * *` (monthly, day 1 at 01:00).

## Repository layout

- `airflow/`
  - `etl_flow.py`: Main production DAG.
  - `FileOps.py`: Local file helper utilities.
- `spark_jobs/`
  - `etl_spark_emr.py`: Spark ETL script used by EMR (`--run-date YYYY-MM-DD`).
- `pyspark/`
  - Local/legacy Spark scripts for non-EMR runs.
- `dbt/`
  - `dbt_project.yml`, `profiles.yml`, models, tests, and helper scripts.
  - `run_dbt.sh`, `test_dbt.sh`: dbt execution wrappers.
- `sql/`
  - `create_target_table.sql`, `create_extras_table.sql`: Redshift DDL used by Airflow.
- `data/`
  - Lookup CSVs used by Spark enrichment and reference docs.
- `conf/env`
  - Runtime configuration consumed by `config.py` (do not commit secrets).
- `test/`
  - Unit/integration tests for settings, airflow helpers, and pipeline utilities.

## Prerequisites

- Python 3.10+
- Apache Airflow (with Amazon provider)
- Apache Spark / PySpark
- AWS CLI configured
- Access to AWS services used in the DAG:
  - S3
  - EMR
  - Redshift Serverless
  - Secrets Manager
- dbt Core + dbt-redshift adapter

Python packages used by code include (non-exhaustive):

- `boto3`
- `requests`
- `pydantic`
- `pydantic-settings`
- `python-dateutil`
- `pyspark`
- `smart_open`
- `pytest`

## Configuration

Primary settings are defined in:

- `config.py`
- `conf/env`

Important config fields:

- `EXEC_ENV` (`emr` or `local`)
- `DATA_STORE` (`s3` or `local`)
- `S3_BUCKET_SIMPLE`
- `S3_RAW_KEY`
- `S3_PRCSD_KEY`
- `RUN_DATE`
- `FILE_NAME_TEMPLATE`

Notes:

- `config.py` loads env values from `~/NYC_taxi/conf/env`.
- dbt credentials are pulled from AWS Secrets Manager by `dbt/export_vars_credentials.py`.
- Airflow uses AWS connection IDs like `aws_default` and `redshfit_default` (spelling in code is `redshfit_default`).

## Running the pipeline

### 1. Airflow DAG (recommended)

Deploy the repo where Airflow can load `airflow/etl_flow.py`, then trigger:

- DAG: `NYC_taxi_flow`

The DAG will:

- pull source data to S3
- submit Spark ETL step to EMR
- load outputs to Redshift
- run dbt models/tests

### 2. Run Spark ETL script directly (manual)

Example (from project root):

```bash
python -m spark_jobs.etl_spark_emr --run-date 2025-01-01
```

Or via Spark submit (similar to `bash/spark-submit.sh`):

```bash
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  spark_jobs/etl_spark_emr.py --run-date 2025-01-01
```

### 3. Run dbt models/tests manually

From `dbt/`:

```bash
./run_dbt.sh 2025_01
./test_dbt.sh 2025_01
```

These scripts:

- set `DATA_PERIOD`
- export Redshift credentials from Secrets Manager
- run `dbt deps`, `dbt run`, and `dbt test`

## Testing

Run tests from project root:

```bash
pytest -q
```

Current tests cover:

- settings/path rendering behavior (`test/test_settings.py`)
- Airflow helper functions and task templating (`test/airflow/test_etl_flow.py`)
- file operation helpers (`test/airflow/test_FileOps.py`)

Some tests hit real AWS services (S3/network), so credentials and permissions are required.

## Data and model outputs

- Processed Spark parquet: `s3://<bucket>/NYC_taxi/processed/<YYYY>/<MM>/yellow_tripdata_<YYYY-MM>.parquet`
- Spark DQ CSV stats: `.../processed/<YYYY>/<MM>/extras/`
- Redshift source tables:
  - `yellow_taxi_trips_<YYYY_MM>`
  - `yellow_taxi_trips_<YYYY_MM>_stats`
- dbt models:
  - `taxi_agg_<YYYY_MM>`
  - `dq_agg_<YYYY_MM>`
  - `dq_<YYYY_MM>`
  - `yellow_taxi_analytics` (incremental)

## Known caveats

- This repo does not include a dependency lock file (`requirements.txt`/`pyproject.toml`).
- Some script/task names use legacy typos (for example `redshfit`) and should match existing Airflow connection IDs unless refactored.
- Several test modules are integration-style and assume live AWS access.
