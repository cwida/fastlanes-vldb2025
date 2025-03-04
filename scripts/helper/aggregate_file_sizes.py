#!/usr/bin/env python

import pandas as pd
from pathlib import Path


def load_csv(file_path):
    """Loads a CSV file if it exists, otherwise returns an empty DataFrame."""
    file = Path(file_path)
    if file.exists():
        df = pd.read_csv(file)
        expected_columns = {"table_name", "version", "file_size"}

        # Ensure expected columns exist
        if not expected_columns.issubset(df.columns):
            print(f"⚠️ File {file_path} does not have expected columns. Skipping.")
            return pd.DataFrame(columns=list(expected_columns))

        return df
    else:
        print(f"⚠️ File not found: {file_path}")
        return pd.DataFrame(columns=["table_name", "version", "file_size"])


def main():
    script_dir = Path(__file__).parent  # Get script directory
    public_bi_result_path = script_dir / "../../paper_result/compression_ratio/public_bi" # TODO change paper_result to result
    fastlanes_result_path = script_dir / "../../fastlanes/benchmark/result/compression_ratio/public_bi/fastlanes.csv"
    btrblocks_path = script_dir / "../../btrblocks/result/compression_ratio/public_bi/btrblocks.csv"

    # Define file paths
    parquet_compressed_with_duckdb = public_bi_result_path / "parquet_compressed_with_duckdb_parquet.csv"
    parquet_compressed_with_duckdb_forced = public_bi_result_path / "parquet_compressed_with_duckdb_parquet_forced.csv"
    parquet_compressed_with_pyarrow = public_bi_result_path / "parquet_compressed_with_pyarrow.csv"

    # Load datasets
    fastlanes_df = load_csv(fastlanes_result_path)
    btrblocks_df = load_csv(btrblocks_path)
    parquet_compressed_with_duckdb_df = load_csv(parquet_compressed_with_duckdb)
    parquet_compressed_with_duckdb_forced_df = load_csv(parquet_compressed_with_duckdb_forced)
    parquet_compressed_with_pyarrow_df = load_csv(parquet_compressed_with_pyarrow)

    # Check if all dataframes are empty
    if fastlanes_df.empty and parquet_compressed_with_duckdb_df.empty and parquet_compressed_with_duckdb_forced_df.empty:
        print("⚠️ No valid data to process. Exiting.")
        return

    # Combine all datasets
    combined_df = pd.concat([
        #
        parquet_compressed_with_duckdb_df,  #
        parquet_compressed_with_duckdb_forced_df,  #
        fastlanes_df,
        parquet_compressed_with_pyarrow_df,  #
        btrblocks_df,  #
    ],
        ignore_index=True)

    # Aggregate total file size per version
    aggregated_df = combined_df.groupby("version", as_index=False)["file_size"].sum()

    # Sort for better readability
    aggregated_df = aggregated_df.sort_values(by="file_size", ascending=True)

    # Save final output
    output_file = public_bi_result_path / "aggregated_file_sizes.csv"
    aggregated_df.to_csv(output_file, index=False)

    print(f"✅ Aggregated file sizes saved successfully: {output_file}")


if __name__ == "__main__":
    main()
