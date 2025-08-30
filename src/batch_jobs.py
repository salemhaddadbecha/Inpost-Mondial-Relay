from pyspark.sql.functions import col
from src.utils import compute_weekly_counts, compute_daily_counts, compute_quarterly_counts, compute_avg_time_in_apm


def run_parcel_counts(events_df, apm_df, by_courier=False):
    stored = events_df.filter(col('event_type') == 'ParcelStoredForDeliveryByCourier')

    daily = compute_daily_counts(stored, apm_df, by_courier)
    weekly = compute_weekly_counts(stored)
    quarterly = compute_quarterly_counts(stored)
    avg_time = compute_avg_time_in_apm(events_df)

    return {'daily': daily,
            'weekly': weekly,
            'quarterly': quarterly,
            'avg_time': avg_time
            }



