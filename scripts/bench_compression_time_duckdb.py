#!/usr/bin/env python

import os
import time
import duckdb
import pandas as pd
import json
from helper.public_bi import *
from helper.system_info import *

OFFSET = 0

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

# Paths
SCHEMA_MAPPING_PATH = "helper/schema_mappings.json"
OUTPUT_CSV_PATH = "../result/compression_ratio/public_bi/duckdb_compression_time_result.csv"


def load_schema_mappings():
    """Load and return the schema mappings JSON if it exists, else return an empty dict."""
    if not os.path.exists(SCHEMA_MAPPING_PATH):
        print(f"⚠️ Schema mapping file {SCHEMA_MAPPING_PATH} not found. Proceeding without schema enforcement.")
        return {}
    with open(SCHEMA_MAPPING_PATH, "r") as json_file:
        return json.load(json_file)


def measure_duckdb_size(duckdb_file_path):
    """Returns the file size in bytes if the file exists, else 0."""
    return os.path.getsize(duckdb_file_path) if os.path.exists(duckdb_file_path) else 0


def get_multiplier_at_least_10(original_row_count, block_size=120 * 1024):
    """
    Find the smallest integer multiplier m >= 10 such that
    (m * original_row_count) is a multiple of block_size.
    """
    m = 10
    while (m * original_row_count) % block_size != 0:
        m += 1
    return m


def process_table(csv_path, table_name, schema_mappings, results, detailed_results):
    """
    Load CSV data into DuckDB, replicate it to >=10×, measure how long that takes
    (compression time), and record the final DuckDB file size.
    """
    duckdb_file_path = f"output/{table_name}.duckdb"

    # Count lines in CSV (simple validation of row count)
    with open(csv_path, "r", encoding="utf-8") as f:
        line_count = sum(1 for _ in f)

    print(f"Total lines for table {table_name}: {line_count}")

    # Remove any existing DuckDB file to ensure a fresh start
    if os.path.exists(duckdb_file_path):
        os.remove(duckdb_file_path)

    # Connect to DuckDB
    conn = duckdb.connect(duckdb_file_path)
    conn.execute("PRAGMA disable_progress_bar")

    # If we have a schema mapping for this table, use it
    table_mapping = schema_mappings.get(table_name, {})

    # -- Start timing the compression/load process --
    start_load_time = time.time()

    if table_mapping:
        # Load with explicit schema
        mapped_columns = {col: col_type for col, col_type in table_mapping.items()}
        conn.execute(f"""
            CREATE TABLE "{table_name}" AS
            SELECT * 
            FROM READ_CSV(
                '{csv_path}',
                delim='|',
                header=False,
                nullstr='null',
                quote='',
                escape='\\',
                sample_size=-1,
                columns={mapped_columns}
            );
        """)
    else:
        # Fallback to autodetection
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

    # Check how many rows were actually loaded
    original_row_count = conn.execute(f"SELECT COUNT(*) FROM \"{table_name}\"").fetchone()[0]
    print(f"DEBUG: '{table_name}' initially loaded row count = {original_row_count}")

    if original_row_count != line_count:
        raise RuntimeError(
            f"Row count for {table_name} ({original_row_count}) does not match line count ({line_count}). "
            "Possible parsing issue?"
        )

    # Skip if row count is not a multiple of 1024 (per your logic)
    if original_row_count % 1024 != 0:
        print(f"X problematic '{table_name}' with {original_row_count} rows (not multiple of 1024). Skipping.")
        conn.close()
        return

    # Find multiplier to get >=10× data, aligned to multiples of 120*1024
    multiplier = get_multiplier_at_least_10(original_row_count, 120 * 1024)

    # Make a temp copy of original data and insert until we reach multiplier × original_row_count
    conn.execute(f"CREATE TEMP TABLE original_data AS SELECT * FROM \"{table_name}\";")
    for _ in range(multiplier - 1):
        conn.execute(f"INSERT INTO \"{table_name}\" SELECT * FROM original_data;")

    # Loading/replication done => measure time
    end_load_time = time.time()
    compression_time_ms = (end_load_time - start_load_time) * 1000.0

    final_row_count = conn.execute(f"SELECT COUNT(*) FROM \"{table_name}\"").fetchone()[0]
    print(f"✅ Written '{table_name}' into its own DuckDB file, {multiplier}× rows.")
    print(f"DEBUG: Final row count = {final_row_count}\n")

    # Gather column-level info
    table_info = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    type_map = {row[1]: row[2] for row in table_info}

    storage_df = conn.execute(f"PRAGMA storage_info('{table_name}')").fetchdf()
    # Filter out VALIDITY segments for clarity
    storage_df = storage_df[storage_df["segment_type"] != "VALIDITY"]
    grouped = storage_df.groupby(["column_id", "column_name", "compression"], dropna=False).first().reset_index()

    for _, rowdata in grouped.iterrows():
        detailed_results.append({
            "compression": "duckdb",
            "version": "duckdb_native_12",
            "table_name": table_name,
            "id": rowdata["column_id"],
            "name": rowdata["column_name"],
            "data_type": type_map.get(rowdata["column_name"], "UNKNOWN"),
            "compression_scheme": rowdata["compression"] if rowdata["compression"] else "N/A"
        })

    # Measure final DuckDB file size (bytes)
    db_size = measure_duckdb_size(duckdb_file_path)

    # Close connection
    conn.close()

    # DuckDB file size per 1×
    file_size_per_1x = round(db_size / multiplier, 2)

    # Save table-level results (focusing on compression speed)
    results.append({
        'table_name': table_name,
        'duckdb_file_size_1x': file_size_per_1x,
        'compression_time_ms': round(compression_time_ms, 2),
        'version': 'duckdb_native_12',
        'times_data_repeated': multiplier
    })


def public_bi():
    """
    Main entrypoint that:
     1) Loads schema mappings
     2) Iterates over each table in PublicBI.table_list
     3) Measures compression speed (time to load/replicate)
     4) Outputs a summary CSV + a detailed CSV of column-level compression metadata
    """
    results = []
    detailed_results = []

    schema_mappings = load_schema_mappings()

    # Ensure output directory exists
    os.makedirs("output", exist_ok=True)

    # Process each table
    for table in PublicBI.table_list:
        csv_path = PublicBI.get_file_path(table)
        process_table(csv_path, table, schema_mappings, results, detailed_results)

    # Write main table-level results
    df_results = pd.DataFrame(results)
    os.makedirs(os.path.dirname(OUTPUT_CSV_PATH), exist_ok=True)
    df_results.to_csv(OUTPUT_CSV_PATH, index=False)
    print(f"✅ Final summary (compression speed) results saved: {OUTPUT_CSV_PATH}")


if __name__ == "__main__":
    print_system_info()
    public_bi()
