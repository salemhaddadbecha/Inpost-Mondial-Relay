from pyspark.sql.functions import to_timestamp, to_date
from pyspark.sql.functions import col, count, avg, to_date, weekofyear, quarter, unix_timestamp


def read_events(spark, path, format='parquet'):
    if format == 'csv':
        events = spark.read.csv(path, header=True, inferSchema=True)
    else:
        events = spark.read.parquet(path)

    if "event_time" in events.columns:
        events = events.withColumn("event_time", to_timestamp("event_time"))
    if "event_date" in events.columns:
        events = events.withColumn("event_date", to_date("event_date"))

    return events

def read_apm(spark, path):
    return spark.read.csv(path, header=True, inferSchema=True)

def compute_daily_counts(stored_df, apm_df, by_courier=False):
    grouping = ['apm_id', 'event_date']
    if by_courier:
        grouping.append('courier_id')
    daily = stored_df.groupBy(*grouping).agg(count('*').alias('stored_count'))
    return daily.join(apm_df, on='apm_id', how='left')


def compute_weekly_counts(stored_df, apm_df, by_courier=False):
    grouping = ['apm_id', 'week']
    if by_courier:
        grouping.append('courier_id')
    weekly = stored_df.withColumn('week', weekofyear(to_date(col('event_date')))) \
        .groupBy(*grouping).agg(count('*').alias('stored_count_week'))
    return weekly.join(apm_df, on='apm_id', how='left')


def compute_quarterly_counts(stored_df, apm_df, by_courier=False):
    grouping = ['apm_id', 'quarter']
    if by_courier:
        grouping.append('courier_id')
    quarterly = stored_df.withColumn('quarter', quarter(to_date(col('event_date')))) \
        .groupBy(*grouping).agg(count('*').alias('stored_count_quarter'))
    return quarterly.join(apm_df, on='apm_id', how='left')


from pyspark.sql.functions import col, unix_timestamp, avg


def compute_avg_time_in_apm(events_df, apm_df):
    stored = events_df.filter(col('event_type') == 'ParcelStoredForDeliveryByCourier') \
        .select('parcel_id', col('apm_id').alias("apm_id"), col('event_time').alias('stored_time'))

    collected = events_df.filter(col('event_type') == 'ParcelCollectedByRecipient') \
        .select('parcel_id', col('apm_id').alias("apm_id"), col('event_time').alias('collected_time'))

    joined = stored.join(
        collected,
        (stored.parcel_id == collected.parcel_id) & (stored.apm_id == collected.apm_id),
        how="inner"
    ).select(
        stored.parcel_id,
        stored.apm_id,
        stored.stored_time,
        collected.collected_time
    )

    durationed = joined.withColumn(
        "duration_hours",
        (unix_timestamp(col("collected_time")) - unix_timestamp(col("stored_time"))) / 3600.0
    )

    avg_duration = durationed.groupBy('apm_id').agg(avg('duration_hours').alias('avg_hours'))

    return avg_duration.join(apm_df, on='apm_id', how='left')
