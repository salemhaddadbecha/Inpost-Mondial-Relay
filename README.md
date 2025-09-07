# InPost / Mondial Relay Parcel Analytics

## Overview
This project implements an analytics pipeline for **InPost/Mondial Relay** parcel data (≈10M events/day), using **Apache Spark** with **Parquet. 

The goal is to produce key metrics on warehouse (APM) usage:

1. Parcel counts per APM *per day*, *week*, and *quarter*, with optional breakdown by **courier_id**, including geolocation details.
2. Average dwell time of parcels in destination APMs—from **ParcelStoredForDeliveryByCourier** to **ParcelCollectedByRecipient**.
3. Data quality handling, clear FK–PK relationships, and practical advice for scaling and streaming vs. batch strategies.

---

##  Data Models & FK–PK Relationship

- **fact_parcel_events**: Near-real-time stream or batch of parcel lifecycle events (e.g. `ParcelStoredForDeliveryByCourier`, `ParcelCollectedByRecipient`, etc.). Contains `apm_id`.
- **dim_apm**: A static dimension table listing each APM’s `apm_id` (primary key), along with **country**, **city**, **latitude**, and **longitude**.

 The `apm_id` field in `fact_parcel_events` acts as a **foreign key** referencing `dim_apm.apm_id`. In Spark code, we enforce this via consistent joins on `apm_id`, though Spark itself doesn’t enforce constraints.

---

##  How to Run (Local Dev)
1. Create a virtual environment: `python -m venv .venv && source .venv/bin/activate`
2. Install dependencies: `pip install -r requirements.txt`
3. Bootstrap sample data and run local batch job:
```bash
# Step 1: Bootstrap sample data
./setup.sh bootstrap-data

# Batch processing (Parquet format)
python src/main.py --mode batch --input data/batch_parquet/ --apm data/dim_apm.csv

# Stream processing (Parquet format)
python src/main.py --mode stream --input data/stream_input_parquet/ --apm data/dim_apm.csv
# On PowerShell, set PYTHONPATH if needed:
$env:PYTHONPATH = "$(pwd)"
```
---
**Important**: Note the corrected paths: data/batch_parquet/ and data/stream_input_parquet/. These must match the actual output locations created by the bootstrap script.

## Analysis Logic Overview
### Batch Mode (batch_jobs.py)
- Counts parcels stored for delivery:
  - Daily (compute_daily_counts)
  - Weekly (compute_weekly_counts)
  - Quarterly (compute_quarterly_counts)
  - Each supports optional courier split (by_courier=True/False)
- Computes average dwell time in each APM (compute_avg_time_in_apm)
- Each output is enriched with APM geo details via join with dim_apm.

### Stream Mode (stream_jobs.py)
- Reads events continuously with appropriate schema.
- Uses a 1-day watermark and deduplication to handle:
  - Late-arriving data (up to 1 day)
  - Duplicate event prevention
- Computes real-time streaming versions of daily, weekly, quarterly counts, and average dwell time; all enriched with APM data.
## Design decisions (summary)
- For local testing and prototyping: use Parquet format.
- For production: Delta Lake is recommended to get ACID transactions, schema enforcement, and time travel (not yet implemented in this repo).
- Use parcel_id as the unique identifier and deduplicate incoming event streams using last_value windowing or Delta MERGE operations (future work).

## Batch vs Stream
- Batch: simpler, reliable, good for daily aggregations.
- Stream: useful for near-real-time dashboards, but more complex to manage.

| Approach   | Pros                                                                                                                                                                                                                                                                                                            | Cons                                                                                                                                                                                                                                                         |
| ---------- |-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Batch**  | - Simpler to build and maintain<br>- Lower infrastructure overhead<br>- Easier to backfill or reprocess with full-day granularity. <br> - No need to manage watermarks. <br>- Can process large volumes of data efficiently because entire partitions are loaded and cached. <br>- Data Size is know and finite | - Higher latency (process daily or hourly)<br>- Not suitable for real-time dashboards. <br> Large batches may require more memory / CPU. <br>- Kafka or IoT data can't be consumed continuously without building a streaming layer.                          |
| **Stream** | - Delivers near real-time insights<br>- Continuous ingestion and processing into dashboards or monitoring systems. <br>- Automatically consumes Kafka, S3 without manual batch triggers. <br>- Supports aggregations, joins and windowing on live data streams.                                                 | - More complex infrastructure (state management, checkpointing)<br>- Ensuring no duplicate processing is tricky and needs careful setup <br>- Higher operational overhead (monitoring, alerting, scaling)  <br> Data Size is unkown and infinite in advance. |

## Scaling Strategy (When Volume Grows 10× or 100×)
To scale efficiently as data grows:
1. Data Partitioning
   - Partition by date (event_date) and optionally by apm_id for faster queries and joins.
2. Delta Lake for ACID & Performance
- Use Delta Lake to enforce schema, support time travel, compact small files, and optimize queries. 
- Use Z-order clustering on apm_id or event_date.

3. Optimized Spark Deployment
- Launch on autoscaling clusters (Databricks, AWS EMR, GCP Dataproc).
- Leverage dynamic allocation, caching, and partition pruning for performance.

4. Streaming Infrastructure Enhancements
- Ingest via Kafka, AWS Kinesis, or similar durable queues.
- Use transactional sinks (e.g. Delta) with idempotent writes or MERGE logic.
- Scale streaming state store and optimize watermark settings for high-throughput scenarios.

5. Monitoring & Cost Control
- Implement pipeline observability, metrics, and alerts for data lags or failures
- Archive older partitions (cold storage) if needed for cost efficiency.
## Data Quality & Malformed Data Handling
- Implemented:
  - Late & duplicate records: Managed in stream mode via watermarking and dropDuplicates().
  
- Possible improvements (future work):
  - Handle nulls/malformed records with dropna() or fillna().
  - Quarantine invalid records into a dead-letter location.
  - Schema drift detection using validation logic (e.g., isIn() checks).
    - Use validation logic (e.g. isIn() checks or UDF-based rules) to accept only valid event_type values.
    - Maintain an audit stream for unknown or invalid events.
  
## Test Suite
A simple PyTest suite validates:
- Daily / Weekly / Quarterly counts work when splitting by courier.
- Average dwell time calculations produce positive average durations.
- Join with dim_apm successfully enriches results.

To run in a standalone way:
## Setup
Create a new conda environment and install dependencies:

```bash
conda create -n spark-env python=3.11
conda activate spark-env
pip install -r requirements.txt
```bash
pytest -s tests/test_functions.py 
#Or one function: 
pytest -s tests/test_functions.py::test_daily_counts

```
## Continuous Integration (CI)
This project uses GitHub Actions for continuous integration, running automatically on push and pull request events.

The CI pipeline performs:
- Python environment setup (Python 3.11)
- Dependency installation
- Unit tests execution with pytest

The workflow is defined in .github/workflows/python-app.yml.

## Read parquet file: 
```bash
import pyarrow.parquet as pq
import pandas as pd
df = pq.read_table('output\parcel_counts_daily\part-00000-4f5ceda4-b249-4d99-8ee2-aebbeea80c7f-c000.snappy.parquet').to_pandas()
print(df.head())
``` 