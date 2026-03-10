# NYC Taxi Batch Data Platform

## Overview

This project implements a **configurable batch data platform** that ingests, processes, and models the NYC Yellow Taxi dataset using a modern cloud data stack.

The platform is designed with the following principles:

* **Configuration-driven execution**
* **Reproducible batch runs**
* **Environment portability**
* **Separation of compute, storage, and orchestration**

The system processes monthly taxi datasets and produces analytics-ready warehouse tables and data quality metrics.

Primary technologies:

* Apache Airflow — orchestration
* Apache Spark on EMR — distributed processing
* Amazon S3 — data lake storage
* Amazon Redshift Serverless — analytical warehouse
* dbt — analytics modeling and validation

Pipeline DAG:

```
NYC_taxi_flow
```

The platform is intended to behave like a **deployable batch data platform where environments and runtime behavior are controlled entirely through configuration.**

---

# Architecture

```
                +----------------------+
                |  NYC TLC Dataset     |
                |  CloudFront Parquet  |
                +----------+-----------+
                           |
                           v
                 +-------------------+
                 |  Airflow DAG      |
                 |  (NYC_taxi_flow)  |
                 +---------+---------+
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
                  Analytics / BI
```

---

# Platform Design Goals

This project intentionally focuses on **platform characteristics rather than a single pipeline**.

Key goals:

### Configuration Driven

Runtime behavior is controlled through configuration files and environment variables.

This enables:

* environment portability
* easy runtime tuning
* reproducible runs
* minimal code changes across deployments

---

### Deployable Batch Platform

The system can be deployed into different environments simply by adjusting configuration.

Example deployment targets:

* local Spark execution
* EMR distributed processing
* local file storage
* S3 data lake

---

### Reproducible Data Processing

Each run is defined by a **data period parameter**.

This enables:

* deterministic processing
* historical backfills
* easy debugging of past runs

---

# Pipeline Workflow

The Airflow DAG orchestrates the following stages.

## 1. Data Ingestion

Task:

```
fetch_data_to_s3
```

Downloads monthly taxi parquet datasets from the NYC TLC CloudFront endpoint.

Raw files are stored in S3:

```
s3://<bucket>/NYC_taxi/raw/YYYY/MM/yellow_tripdata_YYYY-MM.parquet
```

The raw zone is **immutable**.

Benefits:

* reproducibility
* replayability
* auditability

---

## 2. Runtime Setup

Tasks:

```
setup_conf_in_s3
upload_bootstrap_to_s3
```

These tasks prepare runtime configuration and bootstrap scripts used by the Spark cluster.

---

## 3. EMR Cluster Execution

Tasks:

```
create_cluster
add_step
wait_for_step
terminate_cluster
```

The pipeline provisions an **ephemeral EMR cluster** for each run.

Advantages:

* isolated execution environments
* predictable compute usage
* no long-running cluster maintenance

---

## 4. Distributed Spark Transformation

Spark job:

```
spark_jobs/etl_spark_emr.py
```

Key transformations:

* schema normalization
* timestamp normalization
* enrichment of trip attributes
* generation of data quality metrics

Processed output:

```
s3://<bucket>/NYC_taxi/processed/YYYY/MM/yellow_tripdata_YYYY-MM.parquet
```

Data quality metrics:

```
s3://<bucket>/NYC_taxi/processed/YYYY/MM/extras/
```

---

## 5. Warehouse Loading

Tasks:

```
redshift_create_table
redshift_create_extras_table
copy_to_redshfit
copy_extras_to_redshfit
```

Processed data is loaded into Redshift using the high-performance `COPY` command.

Tables created:

```
yellow_taxi_trips_<YYYY_MM>
yellow_taxi_trips_<YYYY_MM>_stats
```

---

## 6. Analytics Modeling

Tasks:

```
run_dbt_transforms
run_dbt_tests
```

dbt builds analytics models on top of warehouse tables.

Example models:

```
taxi_agg_<YYYY_MM>
dq_agg_<YYYY_MM>
yellow_taxi_analytics
```

dbt tests enforce data quality constraints.

---

# Configuration System

Configuration is the core of the platform.

The system is designed so that **behavior can be modified without changing code**.

Primary configuration sources:

```
config.py
conf/env
```

The configuration layer uses **environment variables and Pydantic settings** for validation.

---

# Key Configuration Options

## Environment Control

Defines where the pipeline executes.

```
EXEC_ENV
```

Options:

```
local
emr
```

Example:

```
EXEC_ENV=emr
```

---

## Data Storage Backend

Controls where data is stored.

```
DATA_STORE
```

Options:

```
local
s3
```

This allows the platform to run locally for development or on AWS in production.

Example:

```
DATA_STORE=s3
```

---

## Run Date / Data Period

Defines which dataset period the pipeline processes.

```
RUN_DATE
```

Example:

```
RUN_DATE=2025-01-01
```

This enables:

* historical backfills
* deterministic reruns
* debugging past runs

---

## S3 Storage Configuration

Primary storage bucket:

```
S3_BUCKET_SIMPLE
```

Raw data prefix:

```
S3_RAW_KEY
```

Processed data prefix:

```
S3_PRCSD_KEY
```

Example:

```
S3_BUCKET_SIMPLE=my-data-bucket
S3_RAW_KEY=NYC_taxi/raw
S3_PRCSD_KEY=NYC_taxi/processed
```

---

## File Naming Template

Defines dataset naming pattern.

```
FILE_NAME_TEMPLATE
```

Example:

```
yellow_tripdata_{year}-{month}.parquet
```

---

# Configuration Design Principles

### Environment portability

The same codebase can run in:

* local development
* staging
* production

Only configuration changes.

---

### Parameterized pipelines

Runs are parameterized by **RUN_DATE**.

This allows:

* replaying historical data
* backfilling missing months
* deterministic processing.

---

### Infrastructure flexibility

Switching compute or storage layers requires only config changes.

Example:

Local development:

```
EXEC_ENV=local
DATA_STORE=local
```

Production:

```
EXEC_ENV=emr
DATA_STORE=s3
```

---

# Data Quality Strategy

Data quality checks occur in two stages.

## Spark-level validation

Spark generates metrics including:

* record counts
* null column counts
* distribution statistics
* schema validation

Metrics are written to the extras dataset.

---

## dbt validation

dbt tests enforce:

* non-null constraints
* accepted values
* referential integrity
* metric sanity checks

---

# Scaling Considerations

### Distributed compute

Spark on EMR provides horizontal scaling.

Large datasets are processed across worker nodes.

---

### Partitioned storage

S3 datasets are partitioned by:

```
year/month
```

Benefits:

* efficient Spark scans
* lower query cost
* incremental ingestion

---

### Parallel warehouse ingestion

Redshift `COPY` loads data directly from S3 using parallel slices.

This significantly improves ingestion performance.

---

### Incremental analytics models

dbt incremental models avoid full-table rebuilds.

---

# Reliability Design

### Idempotent data ingestion

Raw data is immutable.

Failed runs can safely be replayed.

---

### Ephemeral compute clusters

EMR clusters are created and destroyed per run.

This prevents cluster drift and reduces cost.

---

### Task-level failure isolation

Airflow DAG tasks isolate each stage:

* ingestion
* transformation
* loading
* modeling

Failures can be retried independently.

---

# Repository Structure

```
airflow/
  etl_flow.py
  FileOps.py

spark_jobs/
  etl_spark_emr.py

pyspark/
  legacy spark scripts

dbt/
  models/
  tests/
  dbt_project.yml
  profiles.yml

sql/
  create_target_table.sql
  create_extras_table.sql

conf/
  env

test/
  airflow/
  config tests
```

---

# Running the Platform

## Airflow (recommended)

Deploy DAG:

```
airflow/etl_flow.py
```

Trigger:

```
NYC_taxi_flow
```

Schedule:

```
0 1 1 * *
```

Runs monthly.

---

## Manual Spark Run

```
python -m spark_jobs.etl_spark_emr --run-date 2025-01-01
```

---

## dbt Execution

```
cd dbt

./run_dbt.sh 2025_01
./test_dbt.sh 2025_01
```

---

# Testing

Run tests:

```
pytest -q
```

Coverage includes:

* configuration parsing
* Airflow DAG logic
* file helper utilities
* configuration rendering

---

# Future Improvements

Potential platform extensions:

* automated CI/CD for Airflow DAGs
* data lineage tracking
* monitoring via Prometheus/Grafana
* automated backfill tooling
* advanced data validation with Great Expectations
* Redshift distribution and sort key optimization

---

# Summary

This project demonstrates a configurable batch data platform with:

* distributed Spark processing
* warehouse analytics modeling
* Airflow orchestration
* configuration-driven deployment
* reproducible batch processing

The architecture reflects patterns commonly used in modern production data platforms.


