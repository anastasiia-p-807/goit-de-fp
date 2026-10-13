"""Беру налаштування всіх ресурсів з одного config.ini."""
import argparse
import configparser
import os
from pathlib import Path
import re
import shlex

PROJECT_DIR = Path(os.getenv('OLYMPIC_PROJECT_DIR', str(Path(__file__).resolve().parent))).resolve()
CONFIG_PATH = Path(os.getenv('OLYMPIC_CONFIG_FILE', str(PROJECT_DIR / 'config.ini'))).resolve()
CONFIG = configparser.ConfigParser(interpolation=None)
if not CONFIG.read(CONFIG_PATH, encoding='utf-8'):
    raise RuntimeError(f'Не знайшла файл налаштувань: {CONFIG_PATH}')


def resource_path(section, key):
    path = Path(CONFIG[section][key]).expanduser()
    # Зберігаю шлях до Python у venv, навіть якщо це symlink.
    return Path(os.path.abspath(PROJECT_DIR / path if not path.is_absolute() else path))


DATA_DIR = resource_path('runtime', 'data_dir')
STATE_DIR = resource_path('runtime', 'state_dir')
GENERATED_DIR = resource_path('runtime', 'generated_dir')
PROCESS_REGISTRY = resource_path('runtime', 'process_registry')
CHECKPOINT_DIR = resource_path('runtime', 'checkpoint_dir')
LOCAL_LOG_DIR = resource_path('runtime', 'logs_dir')
SOURCE_DIR = resource_path('csv', 'source_dir')
SOURCE_DATABASE = CONFIG['mysql']['source_database']
OUTPUT_DATABASE = CONFIG['mysql']['output_database']
OUTPUT_TABLE = CONFIG['mysql']['output_table']
MYSQL_HOST = CONFIG['mysql']['host']
MYSQL_PORT = CONFIG.getint('mysql', 'port')
INPUT_TOPIC = CONFIG['kafka']['input_topic']
OUTPUT_TOPIC = CONFIG['kafka']['output_topic']
KAFKA_HOST = CONFIG['kafka']['host']
KAFKA_PORT = CONFIG.getint('kafka', 'port')
KAFKA_BOOTSTRAP = f'{KAFKA_HOST}:{KAFKA_PORT}'
KAFKA_SECURITY_PROTOCOL = CONFIG['kafka']['security_protocol']
CSV_HOST = CONFIG['csv']['host']
CSV_PORT = CONFIG.getint('csv', 'port')
CSV_BASE_URL = f'http://{CSV_HOST}:{CSV_PORT}'
SPARK_HOME = resource_path('spark', 'home')
PYTHON_PATH = resource_path('spark', 'python')
SPARK_MASTER = CONFIG['spark']['master']
SPARK_SHUFFLE_PARTITIONS = CONFIG.getint('spark', 'shuffle_partitions')
SPARK_KAFKA_PACKAGE = CONFIG['spark']['kafka_package']
SPARK_JDBC_JAR = resource_path('spark', 'jdbc_jar')
SPARK_LOCAL_IP = CONFIG['spark']['local_ip']
AIRFLOW_HOME_PATH = resource_path('airflow', 'home')
AIRFLOW_BINARY = PYTHON_PATH.parent / 'airflow'
AIRFLOW_HOST = CONFIG['airflow']['host']
AIRFLOW_PORT = CONFIG.getint('airflow', 'port')
AIRFLOW_URL = f'http://{AIRFLOW_HOST}:{AIRFLOW_PORT}'


def identifier(name):
    if not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*', name):
        raise ValueError(f'Неправильна назва для SQL: {name}')
    return name


def credentials():
    # Паролі лише в config.ini.
    values = {'MYSQL_USER': CONFIG['mysql']['user'], 'MYSQL_PASSWORD': CONFIG['mysql']['password']}
    if not all(values.values()):
        raise RuntimeError('Перевір користувача й пароль у блоці [mysql]')
    return values


def mysql_config(database=SOURCE_DATABASE):
    secret = credentials()
    return dict(host=MYSQL_HOST, port=MYSQL_PORT, user=secret['MYSQL_USER'],
                password=secret['MYSQL_PASSWORD'], database=database, connection_timeout=10)


def jdbc_options(database=SOURCE_DATABASE):
    secret = credentials()
    identifier(database)
    return {'url': f'jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{database}'
                   '?connectTimeout=10000&socketTimeout=120000&serverTimezone=UTC'
                   '&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true&useSSL=false',
            'driver': 'com.mysql.cj.jdbc.Driver',
            'user': secret['MYSQL_USER'], 'password': secret['MYSQL_PASSWORD']}


def kafka_config():
    return {'bootstrap_servers': [KAFKA_BOOTSTRAP], 'security_protocol': KAFKA_SECURITY_PROTOCOL}


def kafka_options():
    return {'kafka.bootstrap.servers': KAFKA_BOOTSTRAP,
            'kafka.security.protocol': KAFKA_SECURITY_PROTOCOL,
            'kafka.request.timeout.ms': '30000'}


def runtime_env():
    # Паролі в shell не передаю.
    return {
        'SPARK_HOME': str(SPARK_HOME), 'PYSPARK_PYTHON': str(PYTHON_PATH),
        'SPARK_LOCAL_IP': SPARK_LOCAL_IP, 'SPARK_MASTER': SPARK_MASTER,
        'SPARK_SHUFFLE_PARTITIONS': str(SPARK_SHUFFLE_PARTITIONS),
        'SPARK_KAFKA_PACKAGE': SPARK_KAFKA_PACKAGE, 'SPARK_JDBC_JAR': str(SPARK_JDBC_JAR),
        'OLYMPIC_PROJECT_DIR': str(PROJECT_DIR), 'OLYMPIC_CONFIG_FILE': str(CONFIG_PATH),
        'OLYMPIC_DATA_DIR': str(DATA_DIR), 'AIRFLOW_HOME': str(AIRFLOW_HOME_PATH),
        'AIRFLOW__CORE__DAGS_FOLDER': str(PROJECT_DIR), 'AIRFLOW__CORE__LOAD_EXAMPLES': 'False',
        'AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS': 'False',
        'AIRFLOW_CONN_SPARK_DEFAULT': CONFIG['airflow']['spark_connection'],
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--shell', action='store_true')
    args = parser.parse_args()
    if args.shell:
        for key, value in runtime_env().items():
            print(f'export {key}={shlex.quote(value)}')
