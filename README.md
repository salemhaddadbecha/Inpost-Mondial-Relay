# InPost / Mondial Relay Parcel Analytics

## Overview
This project is a solution to the InPost / Mondial Relay homework assignment.  
It demonstrates how to process parcel event data (10M+ rows/day) using **Spark + Parquet/Delta Lake** to produce analytics tables and KPIs.

---

## Tasks

### 1. Parcel Counts
- Create downstream tables summarizing how many parcels were stored for delivery by courier in each APM.
- Aggregations: **per day, per week, per quarter**.
- Option to split results by **courier_id**.
- Enriched with **APM geo details** (country, city, latitude, longitude).

### 2. Average Time in APM
- Compute the **average time a parcel spends in destination APM**:
  - From `ParcelStoredForDeliveryByCourier`
  - To `ParcelCollectedByRecipient`.

### 3. Data Quality & Scaling
- Handle **late arrivals, duplicates, malformed data**.
- Discuss **batch vs. streaming pros/cons**.
- Propose scaling solutions if data volume grows **10x or 100x**.

---

## 🛠️ Tech Stack
- Apache Spark (PySpark)
- Parquet / Delta Lake
- Python 
- GitHub and Git

## How to run (local dev)
1. Create a virtual environment: `python -m venv .venv && source .venv/bin/activate`
2. Install dependencies: `pip install -r requirements.txt`
3. Bootstrap sample data and run local batch job:
```bash
./setup.sh bootstrap-data
#For batch processing
python src/main.py --mode batch --input data/batch/ --apm data/dim_apm.csv
#For streaming processing: 
python src/main.py --mode stream --input data/stream_input/ --apm data/dim_apm.csv
```

## Design decisions (summary)
-  Use Delta Lake for production to get ACID transactions, schema enforcement, and time travel. Use Parquet for simple local testing.
- Use parcel_id as the unique identifier and deduplicate incoming event streams using last_value windowing or Delta MERGE operations.
- 


## Batch vs Stream
- Batch: simpler, reliable, good for daily aggregations.
- Stream: useful for near-real-time dashboards, but more complex to manage.

## Scaling Considerations
- Partition by event_date and possibly apm_id.
- Use Delta Lake for schema enforcement and ACID guarantees.
- Enable Z-ordering / clustering for query performance.
- For 100x data: consider data lakehouse + distributed compute (Databricks, EMR, GCP Dataproc).
---

## ⚡ Example Code (Batch Processing)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, weekofyear, quarter, datediff

spark = SparkSession.builder.appName("InPostHomework").getOrCreate()

# Load tables
events = spark.read.format("delta").load("data/fact_parcel_events")
apm = spark.read.parquet("data/dim_apm")

# 1. Aggregations
daily_counts = (
    events.filter(col("event_type") == "ParcelStoredForDeliveryByCourier")
    .groupBy("apm_id", "event_date")
    .count()
)

# 2. Avg time in APM
stored = events.filter(col("event_type") == "ParcelStoredForDeliveryByCourier") \
               .select("parcel_id", col("event_time").alias("stored_time"))
collected = events.filter(col("event_type") == "ParcelCollectedByRecipient") \
                  .select("parcel_id", col("event_time").alias("collected_time"))

duration = stored.join(collected, "parcel_id") \
                 .withColumn("duration_days", datediff("collected_time", "stored_time")) \
                 .groupBy().agg(avg("duration_days"))
