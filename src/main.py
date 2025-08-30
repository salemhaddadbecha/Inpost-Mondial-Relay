import argparse
from pyspark.sql import SparkSession
from src.utils import read_events, read_apm
from src.batch_jobs import run_parcel_counts
from src.stream_jobs import start_stream_processing


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['batch','stream'], default='batch')
    parser.add_argument('--input', required=True)
    parser.add_argument('--apm', required=True)
    parser.add_argument('--format', choices=['parquet','delta'], default='parquet')
    args = parser.parse_args()

    spark = SparkSession.builder.appName('InPostHomework').getOrCreate()
    events = read_events(spark, args.input, format=args.format)
    apm = read_apm(spark, args.apm)

    if args.mode == 'batch':
        """First KPI"""
        results = run_parcel_counts(events, apm, by_courier=True)
        #results['daily'].show(20, False)
        results['daily'].write.mode('overwrite').parquet('output/parcel_counts_daily')
        #results['weekly'].show(20, False)
        results['weekly'].write.mode('overwrite').parquet('output/parcel_counts_weekly')
        #results['quarterly'].show(20, False)
        results['quarterly'].write.mode('overwrite').parquet('output/parcel_counts_quarterly')
        """ Second KPI"""
        #results['avg_time'].show(20, False)
        results['avg_time'].write.mode('overwrite').parquet('output/avg_time_in_apm')

    else:
        spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        q = start_stream_processing(spark, args.input, apm)
        for query in q:
            query.awaitTermination()


if __name__ == '__main__':
    main()
