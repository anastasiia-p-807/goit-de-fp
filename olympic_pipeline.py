"""Перша частина: MySQL → Kafka → обробка в Spark → Kafka і MySQL."""
import argparse
from pyspark.sql import SparkSession, functions as F
from settings import (CHECKPOINT_DIR, DATA_DIR, INPUT_TOPIC, OUTPUT_DATABASE, OUTPUT_TABLE, OUTPUT_TOPIC,
                      identifier, jdbc_options, kafka_config, kafka_options, mysql_config)
from transformations import average_stats, join_athletes, valid_bio


def prepare_outputs():
    import mysql.connector
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    admin = KafkaAdminClient(**kafka_config(), request_timeout_ms=30000)
    try:
        existing = set(admin.list_topics())
        for name in (INPUT_TOPIC, OUTPUT_TOPIC):
            if name not in existing:
                try:
                    admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)])
                except TopicAlreadyExistsError:
                    pass
    finally:
        admin.close()
    identifier(OUTPUT_DATABASE)
    with mysql.connector.connect(**mysql_config()) as db:
        cursor = db.cursor()
        cursor.execute(f'CREATE DATABASE IF NOT EXISTS `{OUTPUT_DATABASE}`')
        cursor.execute(f'''CREATE TABLE IF NOT EXISTS `{OUTPUT_DATABASE}`.`{OUTPUT_TABLE}` (
            sport VARCHAR(255), medal VARCHAR(32), sex VARCHAR(32), country_noc VARCHAR(16),
            avg_height DOUBLE, avg_weight DOUBLE, timestamp DATETIME(6), batch_id BIGINT
        )''')
        cursor.close()


def read_mysql(spark, table):
    return spark.read.format('jdbc').options(**jdbc_options()).option('dbtable', table).load()


def run(args):
    prepare_outputs()
    spark = SparkSession.builder.appName('Olympic Streaming Pipeline').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print(f'Spark {spark.version}; MySQL output: {OUTPUT_DATABASE}.{OUTPUT_TABLE}', flush=True)
    print(f'Kafka: {INPUT_TOPIC} -> {OUTPUT_TOPIC}', flush=True)
    try:
        # Етап 1: читаю дані атлетів із MySQL.
        bio_df = read_mysql(spark, 'athlete_bio')
        # Етап 2: залишаю тільки числові зріст і вагу.
        bio_df = valid_bio(bio_df).cache()
        # Етап 3.а): читаю результати з MySQL і відправляю їх у Kafka.
        event_df = read_mysql(spark, 'athlete_event_results')
        # --seed додаю лише для першої відправки.
        if args.seed:
            seed = event_df if args.limit is None else event_df.orderBy('athlete_id', 'result_id').limit(args.limit)
            (seed.select(F.col('athlete_id').cast('string').alias('key'),
                         F.to_json(F.struct('*')).alias('value')).write.format('kafka')
             .options(**kafka_options()).option('topic', INPUT_TOPIC).save())
            print('Source events published to Kafka', flush=True)
        # Етап 3.б): читаю Kafka й розкладаю JSON на колонки.
        raw = (spark.readStream.format('kafka').options(**kafka_options())
               .option('subscribe', INPUT_TOPIC).option('startingOffsets', 'earliest')
               .option('maxOffsetsPerTrigger', args.max_offsets).load())
        events = raw.select(F.from_json(F.col('value').cast('string'), event_df.schema).alias('data')).select('data.*')
        # Етап 4: з’єдную результати з даними атлетів за athlete_id.
        joined = join_athletes(events, bio_df)
        # Етап 5: рахую середні за спортом, медаллю, статтю та країною.
        features = average_stats(joined)

        def write_batch(batch_df, batch_id):
            # Етап 5: додаю час розрахунку для цієї порції.
            # Кешую, щоб не рахувати двічі.
            stats = (batch_df.withColumn('timestamp', F.current_timestamp())
                     .withColumn('batch_id', F.lit(batch_id).cast('long')).cache())
            try:
                rows = stats.count()
                if not rows:
                    return
                # Етап 6.а): записую результати у вихідний Kafka-топік.
                (stats.select(F.to_json(F.struct('sport', 'medal', 'sex', 'country_noc')).alias('key'),
                              F.to_json(F.struct('*')).alias('value')).write.format('kafka')
                 .options(**kafka_options()).option('topic', OUTPUT_TOPIC).save())
                # Етап 6.б): ті самі результати записую в MySQL.
                (stats.write.format('jdbc').options(**jdbc_options(OUTPUT_DATABASE))
                 .option('dbtable', OUTPUT_TABLE).mode('append').save())
                print(f'BATCH {batch_id}: {rows} aggregate rows written to Kafka and MySQL', flush=True)
                stats.show(20, truncate=False)
            finally:
                stats.unpersist()

        # Етап 6: запускаю запис кожної порції через foreachBatch.
        # Продовжую з останньої порції.
        writer = (features.writeStream.foreachBatch(write_batch).outputMode('update')
                  .option('checkpointLocation', str(CHECKPOINT_DIR)))
        writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime='10 seconds')
        writer.start().awaitTermination()
        bio_df.unpersist()
    finally:
        spark.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--seed', action='store_true', help='Publish source MySQL rows to Kafka once')
    parser.add_argument('--limit', type=int, help='Optional source row count for a smoke run')
    parser.add_argument('--available-now', action='store_true', help='Process available events, then stop')
    parser.add_argument('--max-offsets', type=int, default=50000)
    args = parser.parse_args()
    if (args.limit is not None and args.limit <= 0) or args.max_offsets <= 0:
        parser.error('--limit and --max-offsets must be positive')
    if args.limit is not None and not args.seed:
        parser.error('--limit is only used with --seed')
    run(args)
