###############################################################################
#                       SCRIPT 1 (Latex Generation)
#           (Renamed load_csv -> load_csv_for_latex to avoid conflict)
###############################################################################

import pandas as pd
from pathlib import Path


def load_csv_for_latex(file_path):
    """
    Loads a CSV file into a DataFrame for LaTeX generation.
    """
    file = Path(file_path)
    if file.exists():
        return pd.read_csv(file, index_col=0)
    else:
        print(f"⚠️ File not found: {file_path}")
        return pd.DataFrame()


def generate_latex_table(df, caption="Aggregated File Sizes", label="tab:table_1"):
    """
    Converts a DataFrame into a LaTeX table (forcing 2-decimal format for numeric columns)
    and returns it as a string.
    """
    if df.empty:
        print("⚠️ No data to generate LaTeX table.")
        return ""

    latex_str = df.to_latex(
        float_format="%.2f",  # default numeric columns -> 2 decimals
        column_format="|l|" + "c|" * len(df.columns),
        longtable=False,  # We do NOT use the longtable environment
        caption=caption,
        label=label,
        escape=False
    )

    # Replace underscores with '\_' so they appear properly in LaTeX
    latex_str = latex_str.replace('_', r'\_')

    return latex_str


def save_latex_table(latex_table, filename):
    """
    Saves the LaTeX table to a .tex file.
    """
    with open(filename, "w") as file:
        file.write(latex_table)
    print(f"✅ LaTeX table saved successfully: {filename}")


###############################################################################
#                       SCRIPT 2 (Data Aggregation + CSV Generation)
###############################################################################

import pandas as pd
from pathlib import Path


###############################################################################
#                            SHARED UTILITIES
###############################################################################

def load_csv(file_path):
    """
    Loads a CSV file if it exists, otherwise returns an empty DataFrame.
    Ensures 'table_name', 'version', and 'file_size' columns are present.
    """
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


###############################################################################
#                           COMBINED DATA LOADING
###############################################################################

def load_all_datasets(script_dir: Path) -> dict:
    """
    Loads all CSV files needed by both script 1 and script 2.
    Returns a dictionary of DataFrames by key.
    Adjust any paths as needed for your environment.
    """
    # Common base paths
    public_bi_result_path = script_dir / "../result/compression_ratio/public_bi"
    fastlanes_result_path = script_dir / "../fastlanes/benchmark/result/compression_ratio/public_bi/fastlanes.csv"
    btrblocks_path = script_dir / "../btrblocks/result/compression_ratio/public_bi/btrblocks.csv"
    uncompressed_path = script_dir / "../btrblocks/result/compression_ratio/public_bi/uncompressed.csv"

    # Additional/alternate input files
    parquet_compressed_with_duckdb_parquet = public_bi_result_path / "parquet_compressed_with_duckdb_parquet.csv"
    parquet_compressed_with_duckdb_parquet_forced = public_bi_result_path / "parquet_compressed_with_duckdb_parquet_forced.csv"
    parquet_compressed_with_pyarrow = public_bi_result_path / "parquet_compressed_with_pyarrow.csv"
    duckdb_table_size_csv = public_bi_result_path / "duckdb_table_size.csv"

    # Load all dataframes
    datasets = {
        # For script 1
        "fastlanes": load_csv(fastlanes_result_path),
        "btrblocks": load_csv(btrblocks_path),
        "dwdd_parquet": load_csv(parquet_compressed_with_duckdb_parquet),
        "dwdd_parquet_forced": load_csv(parquet_compressed_with_duckdb_parquet_forced),
        "dwdd_pyarrow": load_csv(parquet_compressed_with_pyarrow),
        "dwdd_table_size": load_csv(duckdb_table_size_csv),
        # For script 2
        "dwdd": load_csv(parquet_compressed_with_duckdb_parquet),
        "dwdd_forced": load_csv(parquet_compressed_with_duckdb_parquet_forced),
        "uncompressed": load_csv(uncompressed_path),
    }

    return datasets


###############################################################################
#                           SCRIPT 1 LOGIC
###############################################################################

def aggregate_file_sizes_1(dataframes):
    """
    Aggregates file sizes into a table with table names as rows and versions as columns.
    Filters out columns not in {0.0.2, btrblocks, duckdb_native_12, autoschema_zstd_v2,
    autoschema_snappy_v2}, then adds a 'Total' row, and returns the pivoted DataFrame.
    """
    combined_df = pd.concat(dataframes, ignore_index=True)
    if combined_df.empty:
        print("⚠️ No valid data to process (Script 1).")
        return pd.DataFrame()

    combined_df = combined_df.infer_objects(copy=False)

    pivot_df = combined_df.pivot_table(
        index="table_name",
        columns="version",
        values="file_size",
        aggfunc="sum",
        fill_value=0
    )

    # Filter to include only specified versions
    include_versions = {
        "0.0.2",
        "btrblocks",
        "duckdb_native_12",
        "autoschema_zstd_v2",
        "autoschema_snappy_v2"
    }
    pivot_df = pivot_df[[col for col in pivot_df.columns if col in include_versions]]

    # Add a row for the total size
    pivot_df.loc['Total'] = pivot_df.sum()

    return pivot_df


def normalize_file_sizes(df):
    """
    Normalizes file sizes by dividing each column (except '0.0.2')
    by the corresponding value in the '0.0.2' column, then rounding to 2 decimals.

    The '0.0.2' column itself remains UNALTERED.
    """
    if "0.0.2" in df.columns:
        for col in df.columns:
            if col != "0.0.2":
                df[col] = df[col] / df["0.0.2"]
        df = df.round(2)
    else:
        print("⚠️ Column '0.0.2' not found. Skipping normalization.")
    return df


def print_markdown_table_1(df):
    """
    Prints the aggregated DataFrame as a Markdown table (script 1 style).
    """
    if df.empty:
        print("⚠️ No data to display (Script 1).")
        return

    table_md = df.to_markdown()
    print("\nAggregated File Sizes Table (Script 1):\n")
    print(table_md)


def main_table_1(datasets: dict, script_dir: Path):
    """
    Recreates the logic from the first script's main().
    Takes a dictionary of dataframes from load_all_datasets().
    """
    public_bi_result_path = script_dir / "../result/compression_ratio/public_bi"

    # Grab references from the dictionary
    fastlanes_df = datasets["fastlanes"]
    btrblocks_df = datasets["btrblocks"]
    dwdd_parquet_df = datasets["dwdd_parquet"]
    dwdd_parquet_forced_df = datasets["dwdd_parquet_forced"]
    dwdd_pyarrow_df = datasets["dwdd_pyarrow"]
    duckdb_table_size_df = datasets["dwdd_table_size"]

    # Aggregate
    aggregated_df = aggregate_file_sizes_1([
        dwdd_parquet_df,
        dwdd_parquet_forced_df,
        fastlanes_df,
        dwdd_pyarrow_df,
        btrblocks_df,
        duckdb_table_size_df,
    ])

    if not aggregated_df.empty:
        output_file = public_bi_result_path / "table_1_raw_data.csv"
        aggregated_df.to_csv(output_file)
        print(f"✅ Aggregated file sizes (Script 1) saved successfully: {output_file}")

        # Normalize and print (this keeps '0.0.2' as original size)
        normalized_df = normalize_file_sizes(aggregated_df.copy())
        print_markdown_table_1(normalized_df)


###############################################################################
#                           SCRIPT 2 LOGIC
###############################################################################

def aggregate_file_sizes_2(dataframes):
    """
    Aggregates file sizes into a table with table names as rows and versions as columns.
    Filters out columns not in {0.0.2, btrblocks, uncompressed}.
    """
    combined_df = pd.concat(dataframes, ignore_index=True)
    if combined_df.empty:
        print("⚠️ No valid data to process (Script 2).")
        return pd.DataFrame()

    combined_df = combined_df.infer_objects(copy=False)

    pivot_df = combined_df.pivot_table(
        index="table_name",
        columns="version",
        values="file_size",
        aggfunc="sum",
        fill_value=0
    )

    # Filter to include only specified versions
    include_versions = {"0.0.2", "btrblocks", "uncompressed"}
    pivot_df = pivot_df[[col for col in pivot_df.columns if col in include_versions]]

    return pivot_df


def compute_compression_ratios(df):
    """
    Computes compression ratios by dividing the 'uncompressed' column by all other columns,
    rounding to 2 decimals, then adds an 'Overall Average' row.
    """
    if "uncompressed" in df.columns:
        df = df.apply(lambda x: df["uncompressed"] / x, axis=0).round(2)
        df.loc['Overall Average'] = df.mean()
    else:
        print("⚠️ Column 'uncompressed' not found. Skipping compression ratio computation.")
    return df


def print_markdown_table_2(df):
    """
    Prints the compressed ratio DataFrame as a Markdown table (script 2 style).
    """
    if df.empty:
        print("⚠️ No data to display (Script 2).")
        return

    table_md = df.to_markdown()
    print("\nCompression Ratio Table (Script 2):\n")
    print(table_md)


def main_table_2(datasets: dict, script_dir: Path):
    """
    Recreates the logic from the second script's main().
    Takes a dictionary of dataframes from load_all_datasets().
    """
    public_bi_result_path = script_dir / "../result/compression_ratio/public_bi"

    # Grab references
    fastlanes_df = datasets["fastlanes"]
    btrblocks_df = datasets["btrblocks"]
    uncompressed_df = datasets["uncompressed"]
    dwdd_df = datasets["dwdd"]
    dwdd_forced_df = datasets["dwdd_forced"]
    dwdd_pyarrow_df = datasets["dwdd_pyarrow"]

    # Aggregate
    aggregated_df = aggregate_file_sizes_2([
        dwdd_df,
        dwdd_forced_df,
        fastlanes_df,
        dwdd_pyarrow_df,
        btrblocks_df,
        uncompressed_df
    ])

    if not aggregated_df.empty:
        output_file = public_bi_result_path / "table_2_raw_data.csv"
        aggregated_df.to_csv(output_file)
        print(f"✅ Aggregated file sizes (Script 2) saved successfully: {output_file}")

        # Compute and print compression ratio
        compression_ratio_df = compute_compression_ratios(aggregated_df.copy())
        print_markdown_table_2(compression_ratio_df)


###############################################################################
#                  NEW FUNCTION: TRANSFORM EXCEPT '0.0.2'
###############################################################################

def transform_except_002(df):
    """
    For every column except the one named '0.0.2':
      - new_value = 1 - old_value
      - Multiply by 100 to convert to a percentage
      - If new_value >= 0, prefix '-' (means the column is that many percent smaller)
        otherwise prefix '+'
      - Round to 2 decimals in the string, then append '%'
    """
    transformed_df = df.copy()

    for col in transformed_df.columns:
        if col == "0.0.2":
            # Skip the '0.0.2' column entirely
            continue

        # 1) Subtract from 1
        transformed_df[col] = 1 - transformed_df[col]

        # 2) Multiply by 100 (percentage)
        transformed_df[col] = transformed_df[col] * 100

        # 3) Convert numeric -> signed string with 2 decimals + '%'
        def signed_string(x):
            sign = "-" if x >= 0 else "+"
            return f"{sign}{abs(x):.2f}\%"

        transformed_df[col] = transformed_df[col].apply(signed_string)

    return transformed_df


###############################################################################
#                      MASTER MAIN (Run Everything)
###############################################################################

def master_main():
    """
    1) Loads all dataset CSVs.
    2) Runs script 1 logic -> table_1_raw_data.csv (normalized to two decimals, but keeps '0.0.2' as original size).
    3) Runs script 2 logic -> table_2_raw_data.csv (compression ratio to two decimals).
    4) Reads table_1_raw_data.csv again:
       - Normalizes all columns except '0.0.2'
       - Transforms all columns except '0.0.2' (1 - value with inverted +/- sign)
       - Converts the '0.0.2' column from bytes to MG (megabytes) with 1 decimal (keeping column name unchanged)
       - Truncates the 'table_name' (index) to its first 5 letters
       - Saves as table_1.tex with 2-decimal display in LaTeX (no longtable).
    5) Reads table_2_raw_data.csv and saves as table_2.tex normally (no longtable).
    """
    # -------------------------------------------------------------------------
    # PART A: Aggregation (Table 1 + Table 2)
    # -------------------------------------------------------------------------
    script_dir = Path(__file__).parent
    datasets = load_all_datasets(script_dir)

    # 1) Create & save table_1_raw_data.csv
    main_table_1(datasets, script_dir)

    # 2) Create & save table_2_raw_data.csv
    main_table_2(datasets, script_dir)

    # -------------------------------------------------------------------------
    # PART B: Re-Load Table 1 -> Normalize -> Transform -> Convert to MG -> Truncate table_name -> Save LaTeX
    # -------------------------------------------------------------------------
    public_bi_result_path = script_dir / "../result/compression_ratio/public_bi"
    input_file_1 = public_bi_result_path / "table_1_raw_data.csv"
    output_file_1 = public_bi_result_path / "table_1.tex"

    df_1 = load_csv_for_latex(input_file_1)
    if not df_1.empty:
        df_1 = df_1.infer_objects(copy=False)

        # 1) Re-normalize all columns except '0.0.2'
        if "0.0.2" in df_1.columns:
            for col in df_1.columns:
                if col != "0.0.2":
                    df_1[col] = df_1[col] / df_1["0.0.2"]
            df_1 = df_1.round(2)

        # 2) Invert sign logic for all columns except '0.0.2'
        df_1 = transform_except_002(df_1)

        # 3) Convert '0.0.2' from bytes to MG (megabytes), 1 decimal (do not rename column)
        if "0.0.2" in df_1.columns:
            df_1["0.0.2"] = df_1["0.0.2"] / (1024 * 1024)

        # 4) Truncate table names (index) to first 4 letters only
        df_1.index = df_1.index.map(lambda x: x[:5] if isinstance(x, str) else x)

        # 5) Generate the LaTeX table
        latex_table_1 = generate_latex_table(
            df_1,
            caption="Table 1 (Inverted Sign, '0.0.2' in MG, table names truncated)",
            label="tab:table_1"
        )
        save_latex_table(latex_table_1, output_file_1)
    else:
        print("⚠️ No valid data found in table_1_raw_data.csv. Skipping LaTeX generation (table_1).")

    # -------------------------------------------------------------------------
    # PART C: Load Table 2 -> Standard LaTeX (NO longtable)
    # -------------------------------------------------------------------------
    input_file_2 = public_bi_result_path / "table_2_raw_data.csv"
    output_file_2 = public_bi_result_path / "table_2.tex"

    df_2 = load_csv_for_latex(input_file_2)
    if not df_2.empty:
        df_2 = df_2.infer_objects(copy=False)
        latex_table_2 = generate_latex_table(
            df_2,
            caption="Compression Ratios (Script 2)",
            label="tab:table_2"
        )
        save_latex_table(latex_table_2, output_file_2)
    else:
        print("⚠️ No valid data found in table_2_raw_data.csv. Skipping LaTeX generation (table_2).")


###############################################################################
#                          COMBINED ENTRY POINT
###############################################################################

if __name__ == "__main__":
    master_main()