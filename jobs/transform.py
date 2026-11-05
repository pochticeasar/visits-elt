"""Transform: s3a://raw/visits/batch=<id>/ → s3a://warehouse/visits/ (Parquet, партиции по дню).

1. Чтение сырого слоя: Parquet, все колонки binary, как в TSV.
2. Типы по схеме ClickHouse: числа, Date/DateTime (0000-00-00 → NULL), '' → NULL,
   UInt64 → bigint с сохранением битов, массивы [..] → array<..>,
   Nested (Goals, TraficSource, ParsedParams, Market) → array<struct<..>>.
3. Схлопывание CollapsingMergeTree: ключ (counter_id, start_date, user_id, visit_id),
   группы с sum(sign) > 0, из группы — последняя по порядку (_part, _row) строка с sign = +1.
4. Запись: партиции по start_date, внутри — сортировка (counter_id, start_time), zstd.
   partitionOverwriteMode=dynamic: перезаписываются только дни из батча.

    spark-submit transform.py --batch 2014-03
"""

import argparse
import time
from collections import OrderedDict
from decimal import Decimal

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from raw import col_name, read_raw
from schema import COLUMNS

JSON_OPTS = {"allowSingleQuotes": "true", "allowBackslashEscapingAnyCharacter": "true"}
KEY = ["counter_id", "start_date", "user_id", "visit_id"]

SCALAR = {
    "UInt8": "int", "UInt16": "int", "Int8": "int", "Int16": "int", "Int32": "int",
    "UInt32": "bigint", "Int64": "bigint", "Float64": "double",
}


def u64(d):
    """decimal(20,0) из UInt64 → bigint с теми же 64 битами (для ID и хэшей важна только идентичность)."""
    return F.when(d >= F.lit(Decimal(2**63)), d - F.lit(Decimal(2**64))).otherwise(d).cast("bigint")


def typed(c, t):
    """Колонка c (binary из TSV) → Spark-тип по типу ClickHouse t."""
    s = F.col(c).cast("string")
    if t in SCALAR:
        return s.cast(SCALAR[t])
    if t == "UInt64":
        return u64(s.cast("decimal(20,0)"))
    if t == "Date":
        return F.to_date(s)
    if t == "DateTime":
        return F.to_timestamp_ntz(s)
    if t == "String":
        return F.nullif(s, F.lit(""))
    if t.startswith("FixedString"):
        return F.col(c)
    if t.startswith("Array("):
        e = t[len("Array("):-1]
        if e in SCALAR:
            return F.from_json(s, f"array<{SCALAR[e]}>", JSON_OPTS)
        if e == "UInt64":
            return F.transform(F.from_json(s, "array<decimal(20,0)>", JSON_OPTS), u64)
        if e == "DateTime":
            # lambda обязательна: F.transform передаёт 2 аргумента функции с 2 параметрами
            # (элемент и индекс), и индекс ушёл бы в to_timestamp_ntz как формат.
            return F.transform(F.from_json(s, "array<string>", JSON_OPTS), lambda x: F.to_timestamp_ntz(x))
        if e == "String":
            return F.from_json(s, "array<string>", JSON_OPTS)
    raise ValueError(f"{c}: unsupported type {t}")


def typed_visits(raw):
    """Типизация + свёртка Nested-групп в array<struct>."""
    cols, nested = [], OrderedDict()
    for name, t in COLUMNS:
        c = col_name(name)
        if "." in name:
            group, field = c.split("__")
            nested.setdefault(group, []).append((field, typed(c, t)))
        else:
            cols.append(typed(c, t).alias(c))
    for group, fields in nested.items():
        # arrays_zip берёт имена полей из выражений; alias даёт короткие имена.
        cols.append(F.arrays_zip(*[e.alias(f) for f, e in fields]).alias(group))
    return raw.select(*cols, "_part", "_row")


def collapse(df):
    w = Window.partitionBy(*KEY)
    last = Window.partitionBy(*KEY).orderBy(F.col("_part").desc(), F.col("_row").desc())
    return (
        df.withColumn("_sum_sign", F.sum("sign").over(w))
        .where("sign = 1")
        .withColumn("_rn", F.row_number().over(last))
        .where("_rn = 1 AND _sum_sign > 0")
        .drop("_sum_sign", "_rn", "sign")
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--batch", required=True)
    p.add_argument("--raw", default="s3a://raw/visits")
    p.add_argument("--out", default="s3a://warehouse/visits")
    a = p.parse_args()

    spark = SparkSession.builder.appName(f"transform-{a.batch}").getOrCreate()
    t0 = time.time()
    visits = collapse(typed_visits(read_raw(spark, f"{a.raw}/batch={a.batch}/")))
    (visits.repartition("start_date")
        .sortWithinPartitions("counter_id", "start_time")
        .write.mode("overwrite").partitionBy("start_date")
        .option("compression", "zstd").parquet(a.out))

    out = spark.read.parquet(a.out)
    print(f"visits: {out.count()} rows, {time.time() - t0:.0f}s")
    spark.stop()


if __name__ == "__main__":
    main()
