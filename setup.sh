#!/usr/bin/env bash
set -e

#run it using  ./setup.sh bootstrap-data
case "$1" in
bootstrap-data)
python - <<'PY'
import pandas as pd
from pathlib import Path


Path('data').mkdir(parents=True, exist_ok=True)
# create small dim_apm
apm = pd.DataFrame([
    {"apm_id": "APM_001", "country": "FR", "city": "Paris", "latitude": 48.8566, "longitude": 2.3522},
    {"apm_id": "APM_002", "country": "FR", "city": "Lyon", "latitude": 45.7640, "longitude": 4.8357},
    {"apm_id": "APM_003", "country": "FR", "city": "Marseille", "latitude": 43.2965, "longitude": 5.3698},
    {"apm_id": "APM_004", "country": "BE", "city": "Brussels", "latitude": 50.8503, "longitude": 4.3517},
    {"apm_id": "APM_005", "country": "DE", "city": "Berlin", "latitude": 52.5200, "longitude": 13.4050},
])
apm.to_csv('data/dim_apm.csv', index=False)


# create sample events in parquet using pyarrow/pandas
import pyarrow as pa, pyarrow.parquet as pq
import datetime

events = [
    # Parcel 1 lifecycle
    {"parcel_id": "P1", "apm_id": "APM_001", "courier_id": "C1", "event_type": "ParcelStoredForDeliveryByCourier",
     "event_time": datetime.datetime(2025, 8, 20, 10, 0), "event_date": "2025-08-20"},
    {"parcel_id": "P1", "apm_id": "APM_001", "courier_id": "C1", "event_type": "ParcelCollectedByRecipient",
     "event_time": datetime.datetime(2025, 8, 21, 12, 0), "event_date": "2025-08-21"},

    # Parcel 2 lifecycle
    {"parcel_id": "P2", "apm_id": "APM_002", "courier_id": "C2", "event_type": "ParcelStoredForDeliveryByCourier",
     "event_time": datetime.datetime(2025, 8, 21, 9, 0), "event_date": "2025-08-21"},
    {"parcel_id": "P2", "apm_id": "APM_002", "courier_id": "C2", "event_type": "ParcelCollectedByRecipient",
     "event_time": datetime.datetime(2025, 8, 21, 18, 30), "event_date": "2025-08-21"},

    # Parcel 3 lifecycle
    {"parcel_id": "P3", "apm_id": "APM_003", "courier_id": "C3", "event_type": "ParcelStoredForDeliveryByCourier",
     "event_time": datetime.datetime(2025, 8, 22, 8, 0), "event_date": "2025-08-22"},
    {"parcel_id": "P3", "apm_id": "APM_003", "courier_id": "C3", "event_type": "ParcelCollectedByRecipient",
     "event_time": datetime.datetime(2025, 8, 23, 9, 0), "event_date": "2025-08-23"},

    # Parcel 4 lifecycle
    {"parcel_id": "P4", "apm_id": "APM_004", "courier_id": "C1", "event_type": "ParcelStoredForDeliveryByCourier",
     "event_time": datetime.datetime(2025, 8, 24, 14, 0), "event_date": "2025-08-24"},
    {"parcel_id": "P4", "apm_id": "APM_004", "courier_id": "C1", "event_type": "ParcelCollectedByRecipient",
     "event_time": datetime.datetime(2025, 8, 25, 8, 30), "event_date": "2025-08-25"},

    # Parcel 5 lifecycle
    {"parcel_id": "P5", "apm_id": "APM_005", "courier_id": "C2", "event_type": "ParcelStoredForDeliveryByCourier",
     "event_time": datetime.datetime(2025, 8, 20, 17, 0), "event_date": "2025-08-20"},
    {"parcel_id": "P5", "apm_id": "APM_005", "courier_id": "C2", "event_type": "ParcelCollectedByRecipient",
     "event_time": datetime.datetime(2025, 8, 21, 11, 0), "event_date": "2025-08-21"},

    # Additional events to reach 20 rows (agency flow)
    {"parcel_id": "P6", "apm_id": "APM_001", "courier_id": "C1", "event_type": "ParcelStoredBySenderInAPM",
     "event_time": datetime.datetime(2025, 8, 20, 9, 0), "event_date": "2025-08-20"},
    {"parcel_id": "P6", "apm_id": "APM_001", "courier_id": "C1", "event_type": "ParcelPickedUpByCourierFromAPM",
     "event_time": datetime.datetime(2025, 8, 20, 12, 0), "event_date": "2025-08-20"},
    {"parcel_id": "P6", "apm_id": "APM_001", "courier_id": "C1", "event_type": "ParcelArrivedAtAgency",
     "event_time": datetime.datetime(2025, 8, 20, 18, 0), "event_date": "2025-08-20"},
    {"parcel_id": "P6", "apm_id": "APM_001", "courier_id": "C1", "event_type": "ParcelSortedInAgency",
     "event_time": datetime.datetime(2025, 8, 21, 6, 0), "event_date": "2025-08-21"},
    {"parcel_id": "P6", "apm_id": "APM_001", "courier_id": "C1", "event_type": "ParcelDepartedFromAgency",
     "event_time": datetime.datetime(2025, 8, 21, 12, 0), "event_date": "2025-08-21"},

    {"parcel_id": "P7", "apm_id": "APM_002", "courier_id": "C3", "event_type": "ParcelStoredBySenderInAPM",
     "event_time": datetime.datetime(2025, 8, 22, 7, 30), "event_date": "2025-08-22"},
    {"parcel_id": "P7", "apm_id": "APM_002", "courier_id": "C3", "event_type": "ParcelPickedUpByCourierFromAPM",
     "event_time": datetime.datetime(2025, 8, 22, 12, 0), "event_date": "2025-08-22"},
    {"parcel_id": "P7", "apm_id": "APM_002", "courier_id": "C3", "event_type": "ParcelArrivedAtAgency",
     "event_time": datetime.datetime(2025, 8, 22, 18, 0), "event_date": "2025-08-22"},
    {"parcel_id": "P7", "apm_id": "APM_002", "courier_id": "C3", "event_type": "ParcelSortedInAgency",
     "event_time": datetime.datetime(2025, 8, 23, 7, 0), "event_date": "2025-08-23"},
    {"parcel_id": "P7", "apm_id": "APM_002", "courier_id": "C3", "event_type": "ParcelDepartedFromAgency",
     "event_time": datetime.datetime(2025, 8, 23, 13, 0), "event_date": "2025-08-23"},
]


df = pd.DataFrame(events)
tbl = pa.Table.from_pandas(df, preserve_index=False,
                           schema=pa.schema([
                               ('parcel_id', pa.string()),
                               ('apm_id', pa.string()),
                               ('courier_id', pa.string()),
                               ('event_type', pa.string()),
                               ('event_time', pa.timestamp('ms')),   # <-- milliseconds
                               ('event_date', pa.string())
                           ]))
pq.write_table(tbl, 'data/batch/sample_fact_parcel_events.parquet')
pq.write_table(tbl, 'data/stream_input/sample_fact_parcel_events.parquet')
print('Bootstrapped data in data/ folder')
PY
;;
*)
echo "Usage: $0 bootstrap-data"
exit 1
;;
esac

#Make it executable with chmod +x setup.sh