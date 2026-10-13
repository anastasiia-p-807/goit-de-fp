"""Чищу текст і прибираю повтори перед записом у silver."""
import argparse
from pyspark.sql import SparkSession
from settings import DATA_DIR
from transformations import clean_text_columns

TABLES = ('athlete_bio', 'athlete_event_results')


def run(tables):
    spark = SparkSession.builder.appName('Olympic Bronze to Silver').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        for table in tables:
            # Етап 2.а): читаю таблицю з bronze.
            bronze = spark.read.parquet(str(DATA_DIR / 'bronze' / table))
            # Етап 2.б): чищу текст; етап 2.в): прибираю однакові рядки.
            silver = clean_text_columns(bronze).dropDuplicates().cache()
            try:
                # Етап 2.г): записую очищену таблицю в silver.
                silver.write.mode('overwrite').parquet(str(DATA_DIR / 'silver' / table))
                print(f'SILVER {table}: {silver.count()} rows', flush=True)
                silver.show(5, truncate=40)
            finally:
                silver.unpersist()
    finally:
        spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', choices=TABLES, help='Default: both source tables')
    args = parser.parse_args()
    run([args.table] if args.table else TABLES)
