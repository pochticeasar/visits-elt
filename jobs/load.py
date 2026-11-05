"""Load: TSV → части по N строк → Parquet (zstd) → s3://raw/visits/batch=<id>/.

TSV читается потоково (pyarrow.csv.open_csv), в памяти — одна часть. Значения
не типизируются: все колонки binary, снято только TSV-экранирование (raw.py).
Пути детерминированные (part-00000.parquet, ...), а manifest.json пишется
последним: пока его нет, батч считается незагруженным. Повторный запуск
перезаписывает те же ключи и удаляет лишние part-файлы.

    python load.py --src visits_v1.tsv --batch 2014-03
"""

import argparse
import hashlib
import io
import json
import os
import sys
import time

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

from raw import RAW_NAMES, UNESCAPE, unescape


def read_parts(path, rows_per_part):
    """Потоковое чтение TSV, части ровно по rows_per_part строк (последняя — остаток)."""
    reader = pcsv.open_csv(
        path,
        read_options=pcsv.ReadOptions(column_names=RAW_NAMES, block_size=16 << 20),
        parse_options=pcsv.ParseOptions(delimiter="\t", quote_char=False, escape_char=False),
        convert_options=pcsv.ConvertOptions(
            column_types={n: pa.binary() for n in RAW_NAMES}, strings_can_be_null=False
        ),
    )
    buf, n = [], 0
    for batch in reader:
        while batch.num_rows:
            take = min(rows_per_part - n, batch.num_rows)
            buf.append(batch.slice(0, take))
            batch, n = batch.slice(take), n + take
            if n == rows_per_part:
                yield pa.Table.from_batches(buf)
                buf, n = [], 0
    if n:
        yield pa.Table.from_batches(buf)


def to_parquet(table, part):
    cols = [unescape(table.column(c).combine_chunks()) if c in UNESCAPE else table.column(c) for c in RAW_NAMES]
    n = table.num_rows
    t = pa.Table.from_arrays(
        cols + [pa.array([part] * n, pa.string()), pa.array(range(n), pa.int32())],
        names=RAW_NAMES + ["_part", "_row"],
    )
    out = io.BytesIO()
    pq.write_table(t, out, compression="zstd", compression_level=3, row_group_size=n)
    dates = pc.cast(t.column("start_date"), pa.string())
    return out.getvalue(), pc.min(dates).as_py(), pc.max(dates).as_py()


def s3_client(endpoint):
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=os.environ.get("AWS_REGION", "garage"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 10, "mode": "standard"}),
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--src", required=True)
    p.add_argument("--batch", required=True)
    p.add_argument("--rows-per-part", type=int, default=131072)
    p.add_argument("--endpoint", default=os.environ.get("S3_ENDPOINT", "http://garage:3900"))
    p.add_argument("--bucket", default="raw")
    p.add_argument("--prefix", default="visits")
    a = p.parse_args()

    prefix = f"{a.prefix}/batch={a.batch}/"
    s3 = s3_client(a.endpoint)
    t0 = time.time()
    parts, tsv_bytes = [], os.path.getsize(a.src)
    for i, table in enumerate(read_parts(a.src, a.rows_per_part)):
        name = f"part-{i:05d}.parquet"
        body, lo, hi = to_parquet(table, f"batch={a.batch}/{name}")
        s3.put_object(Bucket=a.bucket, Key=prefix + name, Body=body)
        parts.append({
            "file": name, "rows": table.num_rows, "bytes": len(body),
            "sha256": hashlib.sha256(body).hexdigest(), "min_start_date": lo, "max_start_date": hi,
        })
        print(f"{name}: {table.num_rows} rows -> {len(body) / 2**20:.1f} MB", flush=True)

    manifest = {
        "batch": a.batch,
        "source": os.path.basename(a.src),
        "format": "parquet/zstd, all columns binary",
        "rows_per_part": a.rows_per_part,
        "rows": sum(m["rows"] for m in parts),
        "tsv_bytes": tsv_bytes,
        "bytes": sum(m["bytes"] for m in parts),
        "parts": parts,
    }
    # Хвосты прошлого запуска (другой размер части или формат).
    keep = {prefix + m["file"] for m in parts} | {prefix + "manifest.json"}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=a.bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            if o["Key"] not in keep:
                s3.delete_object(Bucket=a.bucket, Key=o["Key"])
    s3.put_object(Bucket=a.bucket, Key=prefix + "manifest.json", Body=json.dumps(manifest, indent=2).encode())
    print(
        f"batch={a.batch}: {len(parts)} parts, {manifest['rows']} rows, "
        f"{tsv_bytes / 2**20:.1f} MB -> {manifest['bytes'] / 2**20:.1f} MB in {time.time() - t0:.1f}s",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
