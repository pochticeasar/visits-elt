# ELT-пайплайн для пакетной аналитики над visits_v1

Лабораторная работа 2 (ХАСД). Данные Яндекс.Метрики `visits_v1` (TSV, 2.53 ГБ,
1 686 859 строк, 181 колонка) загружаются в S3 в сжатом виде, преобразуются
Spark-ом в аналитический слой и анализируются в Jupyter.

Подробный отчёт с цифрами: [report/report.md](report/report.md).
Ноутбук с результатами: [report/analytics.html](report/analytics.html).

## Архитектура

```
visits_v1.tsv ──load.py──▶ s3://raw ──transform.py──▶ s3://warehouse ──▶ Jupyter
 (диск, текст)              Parquet+zstd,                Parquet+zstd, типы,
                            как в TSV                    схлопнуто, по дням
                 └──────────── Airflow: DAG visits_elt ────────────┘
```

| Сервис | Порт | Что это |
|---|---|---|
| `garage` | 3900 | S3 API (Garage, 1 узел), бакеты `raw` и `warehouse`. Веб-интерфейса нет: в браузере будет `AccessDenied`, это нормально. Содержимое смотреть командами `/garage bucket ...` (см. «Проверить, что всё на месте») |
| `jupyter` | 8888, 4040 | JupyterLab + Spark 3.5.7 (local mode), Spark UI на 4040 |
| `airflow` | 8090 | Airflow 2.10, DAG `visits_elt`, логин `admin` / `admin` |

## Слои

| Слой | Где | Формат | Что внутри |
|---|---|---|---|
| исходник | `/data/visits_v1.tsv` | TSV | 1 686 859 строк, 2534 МБ |
| raw | `s3://raw/visits/batch=2014-03/` | Parquet + zstd, 13 частей | те же значения, все колонки `binary`, плюс `_part`/`_row` и `manifest.json`; 453 МБ |
| warehouse | `s3://warehouse/visits/start_date=.../` | Parquet + zstd, 7 партиций | типы, массивы и структуры, схлопнутые визиты (1 676 581); 448 МБ |

Это медальонная архитектура: `raw` = **бронза** (данные как есть, только сжаты),
`warehouse` = **серебро** (очищено: типы, без дублей, по дням). Золото (агрегаты)
отдельно не хранится: его считают запросы в ноутбуке.

**Load (`jobs/load.py`).** Читает TSV потоком, режет на части по 131 072 строки и
пишет каждую в Parquet. Значения не меняются: снимается только TSV-экранирование.
Колонки хранятся как байты, потому что в данных есть невалидный UTF-8.
`manifest.json` пишется последним.

**Transform (`jobs/transform.py`).**
1. Типы по схеме ClickHouse ([jobs/schema.py](jobs/schema.py)): числа, даты, `0000-00-00` и `''` → `NULL`.
2. Массивы `[1,2]` → `array`, вложенные группы (`Goals.*` и др.) → `array<struct>`.
3. Схлопывание CollapsingMergeTree: по ключу `(counter_id, start_date, user_id, visit_id)`
   остаются группы с `sum(sign) > 0`, из группы берётся последняя строка с `sign = +1`.
4. Запись: партиции по `start_date`, сортировка `(counter_id, start_time)`, zstd.

Оба шага идемпотентны. Повторный запуск перезаписывает те же файлы и партиции.

## Структура репозитория

```
.env.example            шаблон версий и локальных учебных ключей S3
docker-compose.yml      garage + jupyter + airflow
garage/                 garage.toml, init.sh (layout, ключ, бакеты)
spark/                  Dockerfile (Spark + S3A + Python), spark-defaults.conf
airflow/Dockerfile      Airflow поверх образа Spark
dags/visits_elt.py      DAG: load → transform
jobs/
  schema.py             181 колонка visits_v1 с типами ClickHouse
  raw.py                имена колонок, снятие экранирования, чтение raw
  load.py               TSV → s3://raw
  transform.py          s3://raw → s3://warehouse
notebooks/
  analytics.ipynb       слои, путь одного визита, аналитика, TSV против warehouse
report/                 report.md, analytics.html, benchmark.csv, layer_sizes.csv
```

## Запуск с нуля

Нужен Docker (≥ 10 ГБ памяти) и файл `visits_v1.tsv`. Путь к папке с ним задаётся
в `.env` (`DATASET_DIR`, в шаблоне — `./dataset`). Сам датасет в репозиторий не включён.

```bash
# 0. Создать локальные настройки и положить visits_v1.tsv в DATASET_DIR
cp .env.example .env

# 1. Поднять сервисы (первая сборка образов ~5 мин)
docker compose up -d --build

# 2. Настроить Garage: layout, ключ, бакеты raw и warehouse (один раз)
./garage/init.sh
```

### 3а. Пайплайн через Airflow

1. Открыть http://localhost:8090 (`admin` / `admin`).
2. Включить DAG `visits_elt` переключателем слева.
3. Нажать ▶ (Trigger DAG). Можно поменять параметры `batch` и `src`.
4. В Graph видно две задачи: `load` (~30 с) → `transform` (~45 с). Логи открываются кликом по задаче.

То же из терминала:

```bash
docker compose exec airflow /opt/airflow-venv/bin/airflow dags unpause visits_elt
docker compose exec airflow /opt/airflow-venv/bin/airflow dags trigger visits_elt
```

### 3б. Пайплайн вручную (без Airflow)

```bash
docker compose exec jupyter python3 /opt/jobs/load.py --src /data/visits_v1.tsv --batch 2014-03
docker compose exec jupyter spark-submit /opt/jobs/transform.py --batch 2014-03
```

### 4. Аналитика

Открыть http://localhost:8888 → `notebooks/analytics.ipynb` → **Run → Run All Cells** (~3 мин).
При первом запуске раздел 4 загружает исходный TSV в `s3://raw/tsv/` для сравнения.

## Проверить, что всё на месте

```bash
docker compose ps                                       # garage, jupyter, airflow — Up
docker compose exec garage /garage bucket list          # raw, warehouse
docker compose exec garage /garage bucket info raw      # ~3 ГБ: части + manifest + tsv
docker compose exec garage /garage bucket info warehouse   # ~450 МБ
```

## Остановить

```bash
docker compose down        # остановить, данные в S3 сохраняются
docker compose down -v     # остановить и удалить данные (потом снова init.sh и пайплайн)
```

## Результаты (кратко)

| | |
|---|---|
| Сжатие при загрузке | 2534 МБ → 453 МБ (17.9%) |
| Время load / transform | ~30 с / ~45 с |
| Визитов после схлопывания | 1 676 581 из 1 686 859 строк |
| Запросы по warehouse против TSV | в 11–26 раз быстрее, читается 3–26 МБ вместо 2.5 ГБ |
| Ответы TSV и warehouse | совпадают во всех 7 запросах |

## Частые проблемы

| Симптом | Причина и решение |
|---|---|
| Spark висит на записи в `s3a://`, в логах Garage повторяется `POST /?delete` | S3A 3.3.4 не понимает ответ Garage на пакетное удаление. В `spark-defaults.conf` уже стоят `fs.s3a.multiobjectdelete.enable=false` и `fs.s3a.directory.marker.retention=keep` |
| `manifest.json is not a Parquet file` | raw читать через `raw.read_raw()`: там фильтр `*.parquet` |
| `init.sh`: `required arguments were not provided: <node-ids>` | у `garage layout assign` флаг `-t` забирает следующие аргументы; node id должен идти первым |
| `AccessDenied` / `NoSuchBucket` | не выполнен `./garage/init.sh` после `docker compose down -v` |
| в Airflow DAG не виден | подождать ~30 с после старта или проверить `docker compose logs airflow` |
