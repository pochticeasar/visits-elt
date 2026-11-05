"""Сырой слой (bronze): s3a://raw/visits/batch=<id>/part-NNNNN.parquet.

Значения TSV лежат как есть, все 181 колонка — binary (в датасете много
невалидного UTF-8, а типы проставляет transform). Изменение одно: снято
TSV-экранирование ClickHouse у скалярных строк. Плюс служебные _part
(batch=<id>/part-NNNNN.parquet) и _row (номер строки в части) — по ним
восстанавливается исходный порядок строк.
"""

import re

from schema import COLUMNS, NAMES


def col_name(name):
    """CounterID → counter_id, UTCStartTime → utc_start_time, Goals.ID → goals__id."""
    snake = lambda s: re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", s).lower()
    return "__".join(snake(part) for part in name.split("."))


RAW_NAMES = [col_name(n) for n in NAMES]
# Скалярные строки в TSV ClickHouse экранированы (\t, \n, \\, \0, ...); массивы
# остаются как есть и разбираются в transform вместе с кавычками.
UNESCAPE = {col_name(n) for n, t in COLUMNS if t == "String" or t.startswith("FixedString")}

_ESC = re.compile(rb"\\(.)", re.S)
_ESC_MAP = {b"b": b"\b", b"f": b"\f", b"r": b"\r", b"n": b"\n", b"t": b"\t", b"0": b"\0"}


def _unescape_value(v):
    return _ESC.sub(lambda m: _ESC_MAP.get(m.group(1), m.group(1)), v)


def unescape(arr):
    """Снимает TSV-экранирование. Медленный Python-путь — только для значений с '\\'."""
    import pyarrow as pa
    import pyarrow.compute as pc

    has = pc.fill_null(pc.match_substring(arr, "\\"), False)
    if not pc.any(has).as_py():
        return arr
    vals = arr.to_pylist()
    for i in pc.indices_nonzero(has).to_pylist():
        vals[i] = _unescape_value(vals[i])
    return pa.array(vals, pa.binary())


def read_raw(spark, path):
    """DataFrame сырого слоя: 181 binary-колонка + _part, _row."""
    # Рядом с частями лежит manifest.json — читаем только .parquet.
    return spark.read.option("pathGlobFilter", "*.parquet").parquet(path)
