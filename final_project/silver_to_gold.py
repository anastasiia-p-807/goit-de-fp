"""Об’єдную таблиці й рахую середні для gold."""
from pyspark.sql import SparkSession, functions as F
from settings import DATA_DIR
from transformations import average_stats, join_athletes, valid_bio


def run():
    spark = SparkSession.builder.appName('Olympic Silver to Gold').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        # Етап 3.а): читаю обидві таблиці з silver.
        bio = valid_bio(spark.read.parquet(str(DATA_DIR / 'silver/athlete_bio')))
        events = spark.read.parquet(str(DATA_DIR / 'silver/athlete_event_results'))
        # Етап 3.б): з’єдную таблиці за athlete_id.
        joined = join_athletes(events, bio)
        # Етап 3.в): рахую середній зріст і вагу для кожної групи.
        # Етап 3.г): додаю час розрахунку.
        gold = average_stats(joined).withColumn('timestamp', F.current_timestamp()).cache()
        try:
            # Етап 3.ґ): зберігаю результат у gold/avg_stats.
            gold.write.mode('overwrite').parquet(str(DATA_DIR / 'gold/avg_stats'))
            print(f'GOLD avg_stats: {gold.count()} rows', flush=True)
            gold.orderBy('sport', 'medal', 'sex', 'country_noc').show(20, truncate=False)
        finally:
            gold.unpersist()
    finally:
        spark.stop()


if __name__ == '__main__':
    run()
