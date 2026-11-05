# Лаба 2 (ХАСД): ELT-пайплайн для пакетной аналитики над visits_v1

Контекст проекта для AI-ассистента и для меня.

## Задание (кратко)

ELT-пайплайн для пакетной аналитики:
- extract считается готовым: данные лежат в файле на диске;
- данные загружаются в хранилище **в сжатом формате**, затем преобразуются в оптимизированный для аналитики вид;
- преобразование нужно **обосновать**, аналитику — продемонстрировать и отразить в отчёте;
- ориентир по стеку: Spark + S3-like хранилище + Jupyter, локально в контейнерах;
- датасет из лабы 1 (≥ 500 МБ): Яндекс.Метрика `visits_v1`, TSV без заголовка, 2.53 ГБ, 1 686 859 строк, 181 колонка.

**Принцип: делать только то, что явно просит задание.** Отказоустойчивость, Iceberg,
витрины, свой формат из лабы 1 — сознательно убраны.

## Решения

- **Стек:** Garage (1 узел) как S3; Spark в local mode внутри контейнера Jupyter; Airflow (DAG `load → transform`, SQLite).
- **Слои:**
  - `s3://raw/visits/batch=<id>/part-NNNNN.parquet` — Parquet+zstd, все 181 колонка `binary` как в TSV
    (снято только TSV-экранирование), части по 131 072 строки, `_part`/`_row`, `manifest.json`;
  - `s3://warehouse/visits/` — Parquet+zstd, типы, схлопнуто, партиции по `start_date`, сортировка `(counter_id, start_time)`.
- **Transform:** типы по схеме ClickHouse; `0000-00-00` и `''` → NULL; `UInt64` → `bigint` с сохранением битов;
  массивы → `array`, Nested-группы → `array<struct>`; схлопывание CollapsingMergeTree по ключу
  `(CounterID, StartDate, UserID, VisitID)` (`VisitVersion` нет, `VisitID = 0` — заглушка): группы с `sum(Sign) > 0`,
  последняя по порядку в файле строка с `Sign = +1`.
- **Бенчмарк:** все запросы аналитики по несжатому TSV (`s3://raw/tsv/`) и по `warehouse`.

## Структура

```
.env                  # версии и учебные ключи
docker-compose.yml    # garage + jupyter + airflow
airflow/Dockerfile    # Airflow поверх образа Spark
dags/visits_elt.py    # load → transform
garage/               # garage.toml, init.sh (layout, ключ, бакеты)
spark/                # Dockerfile, spark-defaults.conf (S3A → Garage)
jobs/                 # schema.py, raw.py, load.py, transform.py
notebooks/            # build_notebook.py → analytics.ipynb
report/               # report.md, analytics.html, *.csv
```

## Подводные камни

- `hadoop-aws` = версия Hadoop внутри Spark (3.3.4 для Spark 3.5.x), `aws-java-sdk-bundle` 1.12.262.
- S3A + Garage: `fs.s3a.multiobjectdelete.enable=false` и `fs.s3a.directory.marker.retention=keep`,
  иначе бесконечные ретраи на `POST ?delete`.
- В данных много невалидного UTF-8 → сырые колонки `binary`.
- `F.transform` передаёт индекс второй функции с двумя параметрами → оборачивать в `lambda x: ...`.
- Рядом с частями raw лежит `manifest.json` → читать с `pathGlobFilter=*.parquet`.

Сохранять промпты и логи AI-сессий для защиты.
