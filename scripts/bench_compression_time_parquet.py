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

# Number of times to repeat the COPY/Parquet compression benchmark
N_REPEATS = 10

def save_duckdb_table_as_parquet(duckdb_conn, table_name, parquet_file_path):
    """
    Writes the DuckDB table to Parquet with multiple compression codecs (snappy, zstd).
    For each combination, we do the COPY operation N_REPEATS times and take the average.

    Returns one row per (compression, version) with:
      - table_name
      - version
      - file_size
      - compression_time_ms  (average time over N_REPEATS)
    """
    compressions = ['snappy', 'zstd']
    results = []

    # Ensure output directory exists
    output_dir = os.path.dirname(parquet_file_path)
    os.makedirs(output_dir, exist_ok=True)

    # You can add more Parquet versions if needed. Here we use only V2 for simplicity.
    parquet_versions = ["V2"]

    for version in parquet_versions:
        for compression in compressions:
            # We'll use the same path for each repeat so we can measure from scratch each time.
            parquet_path = f"{parquet_file_path}_{compression}_{version}.parquet"

            total_time_ms = 0.0
            file_size = None

            for _ in range(N_REPEATS):
                start_time = time.time()

                # Perform the COPY (compression) step
                duckdb_conn.execute(f"""
                    COPY "{table_name}" TO '{parquet_path}'
                    (FORMAT PARQUET, COMPRESSION '{compression}', PARQUET_VERSION '{version}');
                """)

                # Calculate time for this single run
                elapsed_ms = (time.time() - start_time) * 1000.0
                total_time_ms += elapsed_ms

                # Grab file size (same each run, but we do it to be consistent).
                file_size = os.path.getsize(parquet_path)

                # Remove the Parquet file so the next run starts fresh
                os.remove(parquet_path)

            # Compute average compression time over the N_REPEATS
            avg_compression_time_ms = round(total_time_ms / N_REPEATS, 2)

            results.append({
                'table_name': table_name,
                'version': f"autoschema_{compression.lower()}_{version.lower()}",
                'file_size': file_size,
                'compression_time_ms': avg_compression_time_ms
            })

    return results


def process_table(csv_path, table_name, all_results):
    """
    Loads CSV data into DuckDB (auto-detected schema), writes Parquet files with different
    compressions (snappy, zstd), measures the compression time (averaged over N_REPEATS),
    then appends the results to `all_results`.
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
    compression_results = save_duckdb_table_as_parquet(conn, table_name, parquet_file_path)

    all_results.extend(compression_results)
    conn.close()


def public_bi():
    """Processes all tables in PublicBI and writes combined results to a CSV."""
    all_results = []

    for table_name in PublicBI.table_list:
        csv_path = PublicBI.get_file_path(table_name)
        process_table(csv_path, table_name, all_results)

    df_results = pd.DataFrame(all_results)

    # Example output directory
    output_dir = '../result/compression_ratio/public_bi'
    os.makedirs(output_dir, exist_ok=True)

    # Final CSV with columns like:
    # table_name, version, file_size, compression_time_ms
    output_csv_path = os.path.join(output_dir, 'parquet_compression_time_result.csv')
    df_results.to_csv(output_csv_path, index=False)
    print(f"✅ Final results saved successfully: {output_csv_path}")


if __name__ == "__main__":
    public_bi()
