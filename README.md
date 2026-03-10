# NYC Taxi Batch Data Platform

## Overview

This project implements a **configurable batch data platform** designed to ingest, process, validate, and model the NYC Yellow Taxi dataset. This system is designed to feature - 

The system is designed to mimic **production-grade batch data platform patterns**, focusing on:

* configuration-driven deployment
* reproducible batch runs
* schema enforcing
* data enrichment and validation
* cost-optimized compute
* analytics modeling

The platform processes monthly NYC yellow taxi datasets and produces **analytics-ready data**.

Primary technologies used:

* Apache Airflow — orchestration
* Apache Spark on AWS EMR — distributed processing
* Amazon S3 — data lake storage
* Amazon Redshift Serverless — analytical warehouse
* dbt — analytics modeling and testing
* AWS Secrets Manager — credential management
* Grafana — analytics dashboard and pipeline monitoring 


Pipeline DAG:

```
NYC_taxi_flow
```

---

# Architecture

```
                +----------------------------------+
                |  NYC TLC Yellow taxi Dataset     |
                |  CloudFront Parquet              |
                +----------+-----------------------+
                           |
                           v
                    +-------------+
                    |  S3 Raw     |
                    | Data Lake   |
                    +------+------+
                           |
                           v
                  +----------------+
                  | Spark on EMR   |
                  | Distributed ETL|
                  +--------+-------+
                           |
                           v
                    +-------------+
                    | S3 Processed|
                    | Data + DQ   |
                    +------+------+
                           |
                           v
                    +-------------+
                    | Redshift    |
                    | Warehouse   |
                    +------+------+
                           |
                           v
                       +--------+
                       |  dbt   |
                       | Models |
                       +--------+
                           |
                           v
                  Analytics / BI (grafana)
```

---

# Platform Design Principles

Key platform principles include:

### Configuration Driven Execution

Runtime behaviors are controlled via environment configuration files.

This allows:

* easy environment configurations
* reproducible runs
* minimal code modification

---


### Cost Efficient Compute

The platform uses **ephemeral EMR clusters** instead of long-running infrastructure or serverless Spark.

Benefits:

* no idle cluster costs
* cheaper than serverless for large batch workloads.

---

# Configuration System

Configuration is the core of the platform.

The system relies on an **environment file (`conf/env`)** that defines runtime parameters.

This approach allows the same codebase to run in:

* local development
* staging environments
* production environments

without code modifications.

Configuration values are validated using **Pydantic settings** in `config.py`.

---

# Environment Configuration

The `.env` file controls most runtime behavior including:

* storage paths
* compute environment
* dataset URL templates
* lookup file locations
* Spark script locations

Example configuration:

```
EXEC_ENV=emr - sets spark execution environment to emr / local. we can use local here to run tests on logic before testing it on EMR which is expensive.
DATA_STORE=s3 - can be set to local or s3 which defines the raw and processed data storage locations.
RUN_DATE=2025-01-01 - used for single test run


S3_BUCKET_SIMPLE=my-data-bucket
S3_RAW_KEY=NYC_taxi/raw
S3_PRCSD_KEY=NYC_taxi/processed

FILE_URL_TEMPLATE=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet

LOOKUP_FILES_PATH=data/lookups/

SPARK_SCRIPT_S3_PATH=s3://platform/scripts/etl_spark_emr.py
```

Configuration options include:

### Runtime Environment

```
EXEC_ENV
```

Options:

```
local
emr
```

Defines where Spark runs.

---

### Storage Backend

```
DATA_STORE
```

Options:

```
local
s3
```

Allows local development or full cloud execution.

---

### Dataset URL Template

```
FILE_URL_TEMPLATE
```

Defines how source data URLs are generated dynamically.

---

### Lookup Data Paths

```
LOOKUP_FILES_PATH
```

Specifies location of reference datasets used during enrichment.

---

### Script Locations

```
SPARK_SCRIPT_S3_PATH
```

Defines the Spark job location used by EMR clusters.

---

# Airflow Orchestration

Apache Airflow is used for pipeline orchestration.

Primary DAG:

```
NYC_taxi_flow
```

Airflow manages:

* task dependencies
* retry logic
* scheduling
* cluster lifecycle
* pipeline monitoring.

Airflow tasks include:

```
fetch_data_to_s3 - data ingestion to datalake
setup_conf_in_s3 - setting up enviroment and saving scripts to s3 for EMR step
upload_bootstrap_to_s3 - generate EMR bootstrap to install dependencies
create_cluster - triggers start of EMR cluster
add_step - adds the spark step to EMR 
wait_for_step - sensor to await step completion
terminate_cluster - terminate the cluster regardless of fail or pass of the step run.
redshift_create_table - create the reshift tables if they dont exist to avoid runtime error
copy_to_redshift - load to redshift
run_dbt_transforms - trigger dbt model transforms
run_dbt_tests - run dbt tests
```

---


# Local Development Workflow

To minimize cloud costs during development, the platform supports **local execution**.

Typical workflow:

1. run Spark jobs locally
2. validate transformations
3. run tests locally
4. CI/CD pipeline executes tests on EMR

Example configuration:

```
EXEC_ENV=local
DATA_STORE=local
```

Once local validation succeeds, CI/CD executes distributed tests using EMR.

---

# Backfilling and Idempotent Runs

The pipeline supports **historical backfills** and **safe reruns**.

Backfill behavior:

* historical partitions can be reprocessed
* output paths are deterministic
* reruns overwrite the same partition safely.

---

# Data Processing

Spark performs the main transformation stage.

Key tasks include:

* schema enforcement
* data validation
* feature enrichment
* aggregation preparation
* generation of data quality metrics.

Processed outputs are written to:

```
S3 processed zone
```

Example:

```
s3://bucket/NYC_taxi/processed/YYYY/MM/
```

---

# Schema Enforcement

Incoming datasets are validated against an expected schema.

This prevents:

* schema drift
* incorrect type inference
* downstream pipeline failures.

Spark enforces schema during read operations.

---

# Data Validation

Validation occurs during the Spark transformation stage.

Examples include:

* null checks
* range validation
* categorical value checks
* timestamp sanity checks.

Validation metrics are recorded and exported.

---

# Dead Letter Handling

Invalid records are not dropped silently.

Instead, they are redirected to a **dead letter dataset**.

Dead letter storage:

```
s3://bucket/NYC_taxi/dead_letter/
```

This enables:

* investigation of corrupt records
* data debugging
* pipeline transparency.

---

# Data Quality Metrics

Spark generates data quality metric while processing which will be used by dbt for testing the integrity of data loaded to redshift. current metrics used are row count and sum of trip_amount

---

# Warehouse Layer

Processed datasets are loaded into **Amazon Redshift** using the high-performance `COPY` command.

Tables created per run:

```
yellow_taxi_trips_<YYYY_MM>
yellow_taxi_trips_<YYYY_MM>_stats
```

Partition-based tables simplify incremental loading and historical backfills.

---

# Analytics Modeling with dbt

dbt builds analytics models on top of warehouse tables.

Examples:

```
taxi_agg_<YYYY_MM>
dq_agg_<YYYY_MM>
yellow_taxi_analytics
```

dbt performs:

* transformation modeling
* data quality tests
* reconciliation tests.

Aggregation reconciliation ensures that metrics computed from processed datasets match expected totals.

---

# Secrets Management

Credentials are stored securely using **AWS Secrets Manager**.

Examples:

* Redshift credentials
* database access tokens

Airflow and dbt retrieve secrets dynamically during runtime.

This avoids storing credentials in source code.

---

# Monitoring

Pipeline monitoring is implemented using **Grafana dashboards**.

Metrics tracked include:

* pipeline runtime
* row counts processed
* validation failure rates
* EMR cluster usage
* pipeline success/failure rates.

Monitoring enables early detection of pipeline issues.

---

# Testing

Tests exist at multiple layers.

Unit tests:

```
pytest
```

Test coverage includes:

* configuration parsing
* Airflow DAG logic
* file helper utilities.

CI/CD pipelines validate the pipeline on EMR before deployment.

---

# Repository Structure

```
airflow/
  etl_flow.py

spark_jobs/
  etl_spark_emr.py

dbt/
  models/
  tests/

sql/
  warehouse DDL

conf/
  env configuration

test/
  pipeline tests
```

---

# Running the Platform

Deploy DAG:

```
airflow/etl_flow.py
```

Trigger pipeline:

```
NYC_taxi_flow
```

Schedule:

```
0 1 1 * *
```

Runs monthly.

---

# Summary

This project demonstrates a configurable batch data platform with:

* distributed Spark processing
* configuration-driven deployment
* cost-efficient ephemeral compute
* strong data validation and schema enforcement
* dead-letter handling for invalid records
* dbt-powered analytics modeling
* secrets management and monitoring

The architecture reflects patterns commonly used in **modern production data platforms**.
