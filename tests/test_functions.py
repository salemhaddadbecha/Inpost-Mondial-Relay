import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.utils import compute_daily_counts, compute_weekly_counts, compute_quarterly_counts, compute_avg_time_in_apm


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("pytest-spark")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_data(spark):
    data = [
        {"parcel_id": "P1", "apm_id": "APM_001", "courier_id": "C1", "event_type": "ParcelStoredForDeliveryByCourier",
         "event_time": "2025-08-20 10:00:00", "event_date": "2025-08-20"},
        {"parcel_id": "P1", "apm_id": "APM_001", "courier_id": "C1", "event_type": "ParcelCollectedByRecipient",
         "event_time": "2025-08-21 12:00:00", "event_date": "2025-08-21"},
        {"parcel_id": "P2", "apm_id": "APM_002", "courier_id": "C2", "event_type": "ParcelStoredForDeliveryByCourier",
         "event_time": "2025-08-21 09:00:00", "event_date": "2025-08-21"},
        {"parcel_id": "P2", "apm_id": "APM_002", "courier_id": "C2", "event_type": "ParcelCollectedByRecipient",
         "event_time": "2025-08-21 18:30:00", "event_date": "2025-08-21"},
    ]
    df = spark.createDataFrame(pd.DataFrame(data))

    apm_data = [
        {"apm_id": "APM_001", "country": "FR", "city": "Paris"},
        {"apm_id": "APM_002", "country": "FR", "city": "Lyon"},
    ]
    apm_df = spark.createDataFrame(pd.DataFrame(apm_data))

    return df, apm_df


def test_daily_counts(sample_data):
    df, apm_df = sample_data
    stored = df.filter(col("event_type") == "ParcelStoredForDeliveryByCourier")
    daily = compute_daily_counts(stored, apm_df, by_courier=True).collect()
    assert len(daily) == 2
    assert any(row.apm_id == "APM_001" for row in daily)
    assert any(row.apm_id == "APM_002" for row in daily)


def test_weekly_counts(sample_data):
    df, _ = sample_data
    stored = df.filter(col("event_type") == "ParcelStoredForDeliveryByCourier")
    weekly = compute_weekly_counts(stored).collect()
    assert len(weekly) == 2


def test_quarterly_counts(sample_data):
    df, _ = sample_data
    stored = df.filter(col("event_type") == "ParcelStoredForDeliveryByCourier")
    quarterly = compute_quarterly_counts(stored).collect()
    assert len(quarterly) == 2


def test_avg_time_in_apm(sample_data):
    df, _ = sample_data
    avg_time = compute_avg_time_in_apm(df).collect()
    assert len(avg_time) == 2
    # check that avg_hours > 0
    for row in avg_time:
        assert row.avg_hours > 0
