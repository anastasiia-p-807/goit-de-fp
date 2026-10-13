# Фінальний проєкт з Data Engineering

У проєкті дві частини: streaming pipeline та batch data lake. Для перевірки запускала MySQL, Kafka, CSV-сервер і Airflow локально.

У першій частині Spark читає дані атлетів із MySQL, прибирає записи з порожнім або нечисловим зростом і вагою. Результати змагань передаю через Kafka, з'єдную їх із даними атлетів за `athlete_id` і рахую середній зріст та вагу для кожної комбінації `sport`, `medal`, `sex`, `country_noc`. Додаю час розрахунку й записую результат у Kafka та MySQL через `foreachBatch`. Код у `olympic_pipeline.py`.

У другій частині CSV проходять через шари bronze, silver і gold:

- `landing_to_bronze.py` завантаження CSV та запис у Parquet;
- `bronze_to_silver.py` очищення тексту й видалення повторів;
- `silver_to_gold.py` об'єднання таблиць, середні значення та timestamp;
- `project_solution.py` Airflow DAG, який запускає ці кроки по черзі.

Спільні налаштування читаються через `settings.py`, а обробку даних винесла в `transformations.py`. Перед запуском потрібно скопіювати `config.example.ini` і створити реальний `config.ini`. Використовувала Spark 3.5.1, MySQL JDBC Connector і Airflow зі Spark provider.

Скрипти запуску локального середовища та локальні перевірки до здачі не включала. За потреби можу їх додати.
