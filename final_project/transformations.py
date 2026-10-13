"""Спільні функції для очищення даних і розрахунку середніх."""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def clean_text_columns(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            # Дефіси залишаю.
            cleaned = F.regexp_replace(F.trim(F.col(field.name)), r"[^\p{L}\p{N}\s,.\x27-]", '')
            cleaned = F.regexp_replace(cleaned, r'\s+', ' ')
            df = df.withColumn(field.name, F.when(F.lower(cleaned).isin('', 'nan', 'null', 'none'),
                                                F.lit(None)).otherwise(cleaned))
    return df


def valid_bio(df):
    # Діапазони не беру.
    numeric = r'^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)$'
    for name in ('height', 'weight'):
        text = F.trim(F.col(name).cast('string'))
        df = df.withColumn(name, F.when(text.rlike(numeric), text.cast('double')))
    return df.filter(F.col('height').isNotNull() & F.col('weight').isNotNull())


def join_athletes(events, bio):
    # Стать і країну беру з bio, щоб колонки не дублювалися.
    fields = [F.col(f'e.`{name}`').alias(name) for name in events.columns
              if name not in ('sex', 'country_noc', 'height', 'weight')]
    return (events.alias('e').join(bio.alias('b'), F.col('e.athlete_id') == F.col('b.athlete_id'), 'inner')
            .select(*fields, F.col('b.sex'), F.col('b.country_noc'), F.col('b.height'), F.col('b.weight'))
            .withColumn('medal', F.when(F.col('medal').isNull() |
                        F.lower(F.trim(F.col('medal'))).isin('', 'nan', 'none', 'null'),
                        F.lit('No medal')).otherwise(F.col('medal'))))


def average_stats(joined):
    return joined.groupBy('sport', 'medal', 'sex', 'country_noc').agg(
        F.avg('height').alias('avg_height'), F.avg('weight').alias('avg_weight'))
