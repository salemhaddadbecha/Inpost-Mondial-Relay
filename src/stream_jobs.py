from src.utils import compute_weekly_counts, compute_daily_counts, compute_quarterly_counts
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import col, unix_timestamp, avg, to_date
from pyspark.sql import functions as F


def start_stream_processing(spark, input_path, apm_df):
    schema = StructType([
        StructField("parcel_id", StringType(), True),
        StructField("apm_id", StringType(), True),
        StructField("courier_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("event_date", StringType(), True)
    ])

    events_stream = spark.readStream.schema(schema).parquet(input_path)
    events_stream = events_stream.withColumn("event_date", to_date("event_date"))

    # Handles late data (up to 1 day) and Removes duplicate events for the same parcel
    deduped = events_stream.withWatermark('event_time', '1 day') \
        .dropDuplicates(['parcel_id', 'event_type', 'event_time'])

    stored = deduped.filter(col('event_type') == 'ParcelStoredForDeliveryByCourier')
    """First KPI"""
    daily_counts = compute_daily_counts(stored, apm_df, by_courier=True)
    weekly_counts = compute_weekly_counts(stored, apm_df, by_courier=True)
    quarterly_counts = compute_quarterly_counts(stored, apm_df, by_courier=True)

    """Second KPI"""
    # Pivot events so that each parcel has stored_time and collected_time columns
    pivoted = deduped.groupBy("parcel_id", "apm_id") \
        .pivot("event_type", ["ParcelStoredForDeliveryByCourier", "ParcelCollectedByRecipient"]) \
        .agg(F.first("event_time"))
    # Compute duration (in hours) when both events exist
    durationed = pivoted.withColumn(
        "duration_hours",
        (unix_timestamp("ParcelCollectedByRecipient") - unix_timestamp("ParcelStoredForDeliveryByCourier")) / 3600.0
    ).filter(col("duration_hours").isNotNull())
    avg_time_in_apm = durationed.groupBy("apm_id").agg(avg("duration_hours").alias("avg_hours"))

    daily_query = daily_counts.writeStream.format('console').outputMode('complete').start()
    weekly_query = weekly_counts.writeStream.format('console').outputMode('complete').start()
    quarterly_query = quarterly_counts.writeStream.format('console').outputMode('complete').start()
    avg_query = avg_time_in_apm.writeStream.format("console").outputMode("complete").start()

    return [daily_query, weekly_query, quarterly_query, avg_query]
