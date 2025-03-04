#!/usr/bin/env python

import os
import time
import duckdb
import pandas as pd
from helper.public_bi import *

# Ensure DuckDB version is 1.2.x
required_version_prefix = "1.2"
if not duckdb.__version__.startswith(required_version_prefix):
    print(f"Error: DuckDB version {required_version_prefix}.x is required. Found version: {duckdb.__version__}")
    exit(1)

# You could change these if you want different repetition counts
FULL_SCAN_REPETITIONS = 10
RANDOM_ACCESS_REPETITIONS = 10
OFFSET = 0  # the row offset used for random-access queries

def measure_parquet_decompression_time_ms(parquet_path: str, repetitions: int) -> float:
    """
    Full table-scan benchmark: read *all* rows from 'parquet_path'
    repeated 'repetitions' times, returning the average time (ms).
    """
    total_time_ms = 0.0
    for _ in range(repetitions):
        conn = duckdb.connect()
        start_time = time.time()
        conn.execute(f"SELECT * FROM read_parquet('{parquet_path}')").fetchall()
        elapsed = time.time() - start_time
        conn.close()
        total_time_ms += elapsed * 1000.0
    return round(total_time_ms / repetitions, 2)


def measure_parquet_random_access_time_ms(parquet_path: str, offset: int, repetitions: int) -> float:
    """
    Random access benchmark: fetch exactly 1 row from the Parquet file at OFFSET,
    repeated 'repetitions' times, returning the average time (ms).

    NOTE: Ensure your data has at least (offset+1) rows. Otherwise,
    this query will return 0 rows (but still measure partial I/O overhead).
    """
    total_time_ms = 0.0
    for _ in range(repetitions):
        conn = duckdb.connect()
        start_time = time.time()
        # We do LIMIT 1 OFFSET {offset} to retrieve exactly 1 row
        conn.execute(f"SELECT * FROM read_parquet('{parquet_path}') LIMIT 1 OFFSET {offset}").fetchall()
        elapsed = time.time() - start_time
        conn.close()
        total_time_ms += elapsed * 1000.0
    return round(total_time_ms / repetitions, 2)


def save_duckdb_table_as_parquet(duckdb_conn, table_name, parquet_file_path):
    """
    Writes the DuckDB table to Parquet with multiple compression codecs (snappy, zstd)
    using Parquet version V1. Measures both:
      1) Full scan time (decompression_time_ms).
      2) Random access time at offset=32000 (random_access_time_ms).

    Returns one row per (compression, version) with:
      - table_name
      - version
      - file_size
      - decompression_time_ms
      - n_repetition_full_scan (constant)
      - random_access_time_ms
      - n_repetition_random_access (constant)
    """
    compressions = ['snappy', 'zstd']
    parquet_sizes = []

    # Ensure output directory exists
    output_dir = os.path.dirname(parquet_file_path)
    os.makedirs(output_dir, exist_ok=True)

    versions = ["V2"]  # If you want to also do V2, add it here
    for version in versions:
        for compression in compressions:
            parquet_path = f"{parquet_file_path}_{compression}_{version}.parquet"

            # Write the table to Parquet with specified version/compression
            duckdb_conn.execute(f"""
                COPY "{table_name}" TO '{parquet_path}'
                (FORMAT PARQUET, COMPRESSION '{compression}', PARQUET_VERSION '{version}');
            """)

            file_size = os.path.getsize(parquet_path)

            # 1) Full-table scan (average across n_repetition_full_scan)
            avg_decompression_ms = measure_parquet_decompression_time_ms(
                parquet_path,
                repetitions=FULL_SCAN_REPETITIONS
            )

            # 2) Random-access query (1 row at OFFSET)
            avg_random_access_ms = measure_parquet_random_access_time_ms(
                parquet_path,
                offset=OFFSET,
                repetitions=RANDOM_ACCESS_REPETITIONS
            )

            parquet_sizes.append({
                'table_name': table_name,
                'version': f"autoschema_{compression.lower()}_{version.lower()}",
                'file_size': file_size,
                'decompression_time_ms': avg_decompression_ms,
                'n_repetition_full_scan': FULL_SCAN_REPETITIONS,
                'random_access_ms': avg_random_access_ms,
                'n_repetition_random_access': RANDOM_ACCESS_REPETITIONS
            })

            # Remove the Parquet file to save space
            os.remove(parquet_path)

    return parquet_sizes


def process_table(csv_path, table_name, results):
    """
    Loads CSV data into DuckDB (auto-detected schema), writes multiple
    Parquet files (compression + version combos), measures both full-scan
    decompression time and random access time, then appends the info.
    """
    conn = duckdb.connect()
    conn.execute("PRAGMA disable_progress_bar")

    # Load CSV with auto-detection of schema
    conn.execute(f"""
        CREATE TABLE "{table_name}" AS
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
    print(f"✅ Loaded '{table_name}' into DuckDB with auto-detected schema.")

    parquet_file_path = f"output/{table_name}"
    parquet_sizes = save_duckdb_table_as_parquet(conn, table_name, parquet_file_path)

    results.extend(parquet_sizes)
    conn.close()


def public_bi():
    """Processes all tables in PublicBI and writes combined results to a CSV."""
    results = []

    for table in PublicBI.table_list:
        csv_path = PublicBI.get_file_path(table)
        process_table(csv_path, table, results)

    df_results = pd.DataFrame(results)

    output_dir = '../result/compression_ratio/public_bi'
    os.makedirs(output_dir, exist_ok=True)

    # Final CSV with columns like:
    # table_name, version, file_size, decompression_time_ms, n_repetition_full_scan,
    # random_access_time_ms, n_repetition_random_access
    output_csv_path = os.path.join(output_dir, 'parquet_compressed_with_duckdb_parquet.csv')
    df_results.to_csv(output_csv_path, index=False)

    print(f"✅ Final results saved successfully: {output_csv_path}")


if __name__ == "__main__":
    public_bi()