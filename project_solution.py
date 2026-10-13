"""DAG для другої частини. Файли зі Spark мають лежати поруч."""
import os
from pathlib import Path
import pendulum
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from settings import (PROJECT_DIR, DATA_DIR, SPARK_HOME, PYTHON_PATH, CONFIG_PATH,
                      SPARK_SHUFFLE_PARTITIONS, SPARK_LOCAL_IP)
PYTHON = str(PYTHON_PATH)

with DAG(
    dag_id='olympic_batch_datalake',
    description='Course CSV -> bronze -> cleaned silver -> average athlete stats gold',
    start_date=pendulum.datetime(2024, 1, 1, tz='Europe/Kyiv'),
    schedule=None, catchup=False, max_active_runs=1,
    default_args={'owner': 'apatsatsiya', 'retries': 0},
    tags=['final_project', 'spark', 'datalake'],
) as dag:
    jobs = []
    for stage in ('landing_to_bronze', 'bronze_to_silver', 'silver_to_gold'):
        jobs.append(SparkSubmitOperator(
            task_id=stage,
            # Файли поруч із DAG.
            application=str(PROJECT_DIR / f'{stage}.py'),
            py_files=','.join(str(PROJECT_DIR / f) for f in ('settings.py', 'transformations.py')),
            conn_id='spark_default',
            spark_binary='spark-submit',
            conf={'spark.sql.shuffle.partitions': str(SPARK_SHUFFLE_PARTITIONS), 'spark.ui.enabled': 'false',
                  'spark.pyspark.python': PYTHON},
            env_vars={'SPARK_HOME': str(SPARK_HOME), 'PYSPARK_PYTHON': PYTHON,
                      'SPARK_LOCAL_IP': SPARK_LOCAL_IP, 'OLYMPIC_DATA_DIR': str(DATA_DIR),
                      'OLYMPIC_PROJECT_DIR':str(PROJECT_DIR),'OLYMPIC_CONFIG_FILE':str(CONFIG_PATH)},
            name=f'olympic-{stage}', verbose=False,
        ))
    # Етап 4: запускаю три кроки по черзі.
    jobs[0] >> jobs[1] >> jobs[2]
