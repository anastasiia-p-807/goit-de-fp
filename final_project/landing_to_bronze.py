"""Завантажую CSV і зберігаю перший шар у Parquet."""
import argparse
import shutil
import urllib.request
from pyspark.sql import SparkSession
from settings import DATA_DIR, CSV_BASE_URL

TABLES = ('athlete_bio', 'athlete_event_results')


def run(tables):
    spark = SparkSession.builder.appName('Olympic Landing to Bronze').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        landing = DATA_DIR / 'landing'
        landing.mkdir(parents=True, exist_ok=True)
        for table in tables:
            # Етап 1.а): завантажую CSV як є.
            target = landing / f'{table}.csv'
            temporary = target.with_suffix('.csv.part')
            with urllib.request.urlopen(f'{CSV_BASE_URL}/{table}.csv', timeout=60) as response:
                with temporary.open('wb') as output:
                    shutil.copyfileobj(response, output)
            # Замінюю після завантаження.
            temporary.replace(target)
            # Етап 1.б): читаю CSV і записую Parquet у bronze.
            df = (spark.read.option('header', True).option('multiLine', True)
                  .option('quote', '"').option('escape', '"').option('mode', 'FAILFAST').csv(str(target)))
            # Поки читаю все як текст.
            df.write.mode('overwrite').parquet(str(DATA_DIR / 'bronze' / table))
            print(f'BRONZE {table}: {df.count()} rows', flush=True)
            df.show(5, truncate=40)
    finally:
        spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', choices=TABLES, help='Default: both source tables')
    args = parser.parse_args()
    run([args.table] if args.table else TABLES)
