#!/usr/bin/env python

import pandas as pd

def main():
    # 1. Input CSV paths (adjust these to your actual file paths)
    fastlanes_csv = "../fastlanes/benchmark/result/compression_time/public_bi/fastlanes.csv"
    btrblocks_csv = "../btrblocks/result/compression_speed/public_bi/btrblocks.csv"
    duckdb_csv    = "../result/compression_ratio/public_bi/duckdb_compression_time_result.csv"
    parquet_csv   = "../result/compression_ratio/public_bi/parquet_compression_time_result.csv"

    # ------------------------------------------------
    # FASTLANES: sum of compression_time_ms
    # ------------------------------------------------
    df_fastlanes = pd.read_csv(
        fastlanes_csv,
        header=0,  # or skiprows=1 if needed
        names=["table_name", "version", "compression_time_ms", "n_repetition"],
        dtype={
            "table_name": str,
            "version": str,
            "compression_time_ms": float,
            "n_repetition": float
        }
    )
    fastlanes_total = df_fastlanes["compression_time_ms"].sum()

    # ------------------------------------------------
    # BTRBLOCKS: sum of compression_time, then /10
    # ------------------------------------------------
    df_btrblocks = pd.read_csv(
        btrblocks_csv,
        header=0,
        names=["table_name", "version", "file_size", "compression_time", "n_repetition"],
        dtype={
            "table_name": str,
            "version": str,
            "file_size": float,
            "compression_time": float,
            "n_repetition": float
        }
    )
    btrblocks_total_raw = df_btrblocks["compression_time"].sum()
    btrblocks_total = btrblocks_total_raw / 10.0  # user-specified division by 10

    # ------------------------------------------------
    # DUCKDB: for each row, compression_time_ms / times_data_repeated, then sum
    # ------------------------------------------------
    df_duckdb = pd.read_csv(
        duckdb_csv,
        header=0,
        names=["table_name", "duckdb_file_size_1x", "compression_time_ms", "version", "times_data_repeated"],
        dtype={
            "table_name": str,
            "duckdb_file_size_1x": float,
            "compression_time_ms": float,
            "version": str,
            "times_data_repeated": float
        }
    )

    # Create a new column that’s per-row: compression_time_ms / times_data_repeated
    df_duckdb["adjusted_compression_time_ms"] = (
            df_duckdb["compression_time_ms"] / df_duckdb["times_data_repeated"]
    )

    # Sum these adjusted values across all rows
    duckdb_total = df_duckdb["adjusted_compression_time_ms"].sum()

    # ------------------------------------------------
    # PARQUET: sum compression_time_ms for snappy and zstd
    # ------------------------------------------------
    df_parquet = pd.read_csv(
        parquet_csv,
        header=0,
        names=["table_name", "version", "file_size", "compression_time_ms"],
        dtype={
            "table_name": str,
            "version": str,
            "file_size": float,
            "compression_time_ms": float
        }
    )

    parquet_snappy_total = df_parquet[
        df_parquet["version"] == "autoschema_snappy_v2"
        ]["compression_time_ms"].sum()

    parquet_zstd_total = df_parquet[
        df_parquet["version"] == "autoschema_zstd_v2"
        ]["compression_time_ms"].sum()

    # ------------------------------------------------
    # Build a single DataFrame row with these totals
    # ------------------------------------------------
    data = {
        "fastlanes_total_ms":  [fastlanes_total],
        "btrblocks_total_ms":  [btrblocks_total],   # after dividing by 10
        "duckdb_total_ms":     [duckdb_total],      # sum of row-wise compression_time_ms / times_data_repeated
        "parquet_snappy_ms":   [parquet_snappy_total],
        "parquet_zstd_ms":     [parquet_zstd_total],
    }
    df_single = pd.DataFrame(data)

    # 4. Save to CSV
    df_single.to_csv("all_systems_compression_time_totals.csv", index=False)
    print("✅ Wrote all_systems_compression_time_totals.csv")
    print(df_single)

    # 5. Optionally save to LaTeX (one-row table)
    latex_str = df_single.to_latex(index=False, float_format="%.2f")
    with open("all_systems_compression_time_totals.tex", "w") as f:
        f.write(latex_str)
    print("✅ Wrote all_systems_compression_time_totals.tex")


if __name__ == "__main__":
    main()
