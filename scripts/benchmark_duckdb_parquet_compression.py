#!/usr/bin/env python

import os
import time
import duckdb
import pandas as pd

from helper.public_bi import PublicBI
from helper.nextia_jd import NextiaJD

# -----------------------------------------------------------------------------
# 1) Version check (DuckDB 1.2.x)
# -----------------------------------------------------------------------------
required_version_prefix = "1.2"
try:
    duckdb_version = duckdb.__version__
except AttributeError:
    from importlib.metadata import version as _get_version

    duckdb_version = _get_version("duckdb")

if not duckdb_version.startswith(required_version_prefix):
    print(f"Error: DuckDB version {required_version_prefix}.x is required. "
          f"Found version: {duckdb_version}")
    exit(1)

# -----------------------------------------------------------------------------
# 2) Benchmark helper functions
# -----------------------------------------------------------------------------
FULL_SCAN_REPETITIONS = 10
RANDOM_ACCESS_REPETITIONS = 10
OFFSET = 0  # row offset used for random-access queries


def measure_parquet_decompression_time_ms(path: str, repetitions: int) -> float:
    total = 0.0
    for _ in range(repetitions):
        conn = duckdb.connect()
        start = time.time()
        conn.execute(f"SELECT * FROM read_parquet('{path}')").fetchall()
        total += (time.time() - start) * 1000.0
        conn.close()
    return round(total / repetitions, 2)


def measure_parquet_random_access_time_ms(path: str, offset: int, repetitions: int) -> float:
    total = 0.0
    for _ in range(repetitions):
        conn = duckdb.connect()
        start = time.time()
        conn.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 1 OFFSET {offset}").fetchall()
        total += (time.time() - start) * 1000.0
        conn.close()
    return round(total / repetitions, 2)


def save_duckdb_table_as_parquet(conn, table_name: str, out_prefix: str):
    """
    Writes `table_name` to Parquet with snappy/zstd (V2), measures:
      - full-scan decompression (ms)
      - single-row random-access (ms)
    Returns a list of dicts (one per codec).
    """
    results = []
    os.makedirs(os.path.dirname(out_prefix), exist_ok=True)

    for compression in ('snappy', 'zstd'):
        path = f"{out_prefix}_{compression}_V2.parquet"
        conn.execute(f"""
            COPY "{table_name}" TO '{path}'
            (FORMAT PARQUET, COMPRESSION '{compression}', PARQUET_VERSION 'V2');
        """)
        size = os.path.getsize(path)
        dec_ms = measure_parquet_decompression_time_ms(path, FULL_SCAN_REPETITIONS)
        rand_ms = measure_parquet_random_access_time_ms(path, OFFSET, RANDOM_ACCESS_REPETITIONS)
        os.remove(path)

        results.append({
            'table_name': table_name,
            'version': f"autoschema_{compression}_v2",
            'file_size': size,
            'decompression_time_ms': dec_ms,
            'n_repetition_full_scan': FULL_SCAN_REPETITIONS,
            'random_access_time_ms': rand_ms,
            'n_repetition_random_access': RANDOM_ACCESS_REPETITIONS
        })

    return results


def process_table(csv_path: str, table_name: str, out_dir: str, accumulator: list):
    """
    - Attempts to load CSV (auto schema) into DuckDB table
    - On success, calls save_duckdb_table_as_parquet(...)
    - Appends each result row to `accumulator`
    - On CSV-sniff failure, logs and skips the table
    """
    conn = duckdb.connect()
    conn.execute("PRAGMA disable_progress_bar")

    try:
        conn.execute(f"""
            CREATE OR REPLACE TABLE "{table_name}" AS
            SELECT *
            FROM READ_CSV_AUTO(
                '{csv_path}',
                sep='|',
                header=False,
                nullstr='null',
                ignore_errors=False,
                sample_size=-1,
                quote='',
                escape='\\'
            );
        """)
        print(f"✅ Loaded '{table_name}'")
    except duckdb.InvalidInputException as e:
        print(f"❌ Skipping '{table_name}': could not auto-detect CSV schema ({e})")
        conn.close()
        return

    out_prefix = os.path.join(out_dir, table_name)
    rows = save_duckdb_table_as_parquet(conn, table_name, out_prefix)
    accumulator.extend(rows)
    conn.close()


# -----------------------------------------------------------------------------
# 3) PublicBI workflow
# -----------------------------------------------------------------------------
def public_bi():
    results = []
    out_dir = os.path.join('..', 'result', 'compression_ratio', 'public_bi')
    os.makedirs(out_dir, exist_ok=True)

    for tbl in PublicBI.table_list:
        csv = PublicBI.get_file_path(tbl)
        try:
            process_table(csv, tbl, out_dir, results)
        except Exception as e:
            # catch any other errors to keep going
            print(f"❌ Error processing PublicBI '{tbl}': {e}")
            continue

    df = pd.DataFrame(results)
    csv_out = os.path.join(out_dir, 'parquet_compressed_with_duckdb_parquet.csv')
    df.to_csv(csv_out, index=False)
    print(f"✅ PublicBI results written to {csv_out}")


# -----------------------------------------------------------------------------
# 4) NextiaJD workflow
# -----------------------------------------------------------------------------
def nextia_jd():
    results = []
    out_dir = os.path.join('..', 'result', 'compression_ratio', 'nextia_jd')
    os.makedirs(out_dir, exist_ok=True)

    for tbl in NextiaJD.table_list:
        csv = NextiaJD.get_file_path(tbl)
        try:
            process_table(csv, tbl, out_dir, results)
        except Exception as e:
            print(f"❌ Error processing NextiaJD '{tbl}': {e}")
            continue

    df = pd.DataFrame(results)
    csv_out = os.path.join(out_dir, 'parquet_compressed_with_duckdb_parquet.csv')
    df.to_csv(csv_out, index=False)
    print(f"✅ NextiaJD results written to {csv_out}")


# -----------------------------------------------------------------------------
# 5) Entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # To run NextiaJD tables:
    nextia_jd()
    # To run PublicBI tables:
    public_bi()
