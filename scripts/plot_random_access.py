###############################################################################
#                       SCRIPT 1 (Latex Generation) - Updated
###############################################################################

import pandas as pd
from pathlib import Path


###############################################################################
#                            CSV LOADING & LaTeX
###############################################################################

def load_csv_for_latex(file_path):
    file = Path(file_path)
    if file.exists():
        return pd.read_csv(file, index_col=0)
    else:
        print(f"⚠️ File not found: {file_path}")
        return pd.DataFrame()


def generate_latex_table(df, caption="Aggregated File Sizes", label="tab:table_1"):
    if df.empty:
        print("⚠️ No data to generate LaTeX table.")
        return ""

    # Ensure 'table_name' exists and truncate it to the first 5 characters
    if "table_name" in df.index.names or "table_name" in df.columns:
        df = df.rename(index=lambda x: x[:5] if isinstance(x, str) else x)  # Modify index if it's named "table_name"
        df.rename(columns={"table_name": "Table"}, inplace=True)  # Optional: Rename column header for clarity

    latex_str = df.to_latex(
        float_format="%.5f",
        column_format="|l|" + "c|" * len(df.columns),
        longtable=False,
        caption=caption,
        label=label,
        escape=False
    )

    # Ensure underscores are properly escaped in LaTeX
    latex_str = latex_str.replace('_', r'\_')

    return latex_str

def save_latex_table(latex_table, filename):
    with open(filename, "w") as file:
        file.write(latex_table)
    print(f"✅ LaTeX table saved successfully: {filename}")


###############################################################################
#                            SHARED UTILITIES
###############################################################################

def load_csv(file_path):
    """
    Loads a CSV file and checks for a minimum set of columns.
    Adjust 'expected_columns' if you need more or fewer required columns.
    """
    file = Path(file_path)
    if file.exists():
        df = pd.read_csv(file)
        # Basic assumption: each CSV has at least these columns:
        expected_columns = {"table_name", "version", "random_access_ms"}
        if not expected_columns.issubset(df.columns):
            print(f"⚠️ File {file_path} does not have expected columns {expected_columns}. Skipping.")
            return pd.DataFrame(columns=list(expected_columns))
        return df
    else:
        print(f"⚠️ File not found: {file_path}")
        return pd.DataFrame(columns=["table_name", "version", "random_access_ms"])


###############################################################################
#                     LOAD ALL DATASETS
###############################################################################

def load_all_datasets(script_dir: Path) -> dict:
    """
    Adjust these paths to point to the correct locations of your CSVs.
    """
    public_bi_result_path = script_dir / "../result/compression_ratio/public_bi"

    fastlanes_result_path = script_dir / "../fastlanes/benchmark/result/random_access/public_bi/fastlanes.csv"
    btrblocks_path = script_dir / "../btrblocks/result/random_access/public_bi/btrblocks.csv"
    uncompressed_path = script_dir / "../btrblocks/result/compression_ratio/public_bi/uncompressed.csv"

    parquet_compressed_with_duckdb_parquet = public_bi_result_path / "parquet_compressed_with_duckdb_parquet.csv"
    parquet_compressed_with_duckdb_parquet_forced = public_bi_result_path / "parquet_compressed_with_duckdb_parquet_forced.csv"
    parquet_compressed_with_pyarrow = public_bi_result_path / "parquet_compressed_with_pyarrow.csv"
    duckdb_table_size_csv = public_bi_result_path / "duckdb_table_size.csv"

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
#   HELPER: COMBINE & DIVIDE PER-ROW (No Check if Repetitions are Uniform)
###############################################################################

def combine_check_and_divide_by_repetition(dataframes):
    """
    1. Concatenate all DataFrames into one.
    2. If a row has 'n_repetition' and 'random_access_ms', we divide
       that row's 'random_access_ms' by row['n_repetition'] (if non-zero).
    3. Similarly for 'n_repetition_random_access' → 'random_access_ms',
       and 'n_repetition_random_access' → 'random_access_time_ms'.
    4. Return the combined DataFrame without verifying uniform repetition counts.
    """

    combined_df = pd.concat(dataframes, ignore_index=True)
    if combined_df.empty:
        return combined_df

    # For the typical single repetition:
    if "n_repetition" in combined_df.columns and "random_access_ms" in combined_df.columns:
        def _divide_decomp(row):
            if pd.notna(row.get("n_repetition", None)) and row["n_repetition"] != 0:
                return row["random_access_ms"] / row["n_repetition"]
            return row["random_access_ms"]

        combined_df["random_access_ms"] = combined_df.apply(_divide_decomp, axis=1)
        print("✅ Divided 'random_access_ms' by each row's 'n_repetition' if non-zero.")

    # For a separate full-scan repetition count
    if "n_repetition_random_access" in combined_df.columns and "random_access_ms" in combined_df.columns:
        def _divide_full_scan(row):
            if pd.notna(row.get("n_repetition_random_access", None)) and row["n_repetition_random_access"] != 0:
                return row["random_access_ms"] / row["n_repetition_random_access"]
            return row["random_access_ms"]

        combined_df["random_access_ms"] = combined_df.apply(_divide_full_scan, axis=1)
        print("✅ Divided 'random_access_ms' by each row's 'n_repetition_random_access' if non-zero.")

    # For random-access
    if "n_repetition_random_access" in combined_df.columns and "random_access_time_ms" in combined_df.columns:
        def _divide_rand_acc(row):
            if pd.notna(row.get("n_repetition_random_access", None)) and row["n_repetition_random_access"] != 0:
                return row["random_access_time_ms"] / row["n_repetition_random_access"]
            return row["random_access_time_ms"]

        combined_df["random_access_time_ms"] = combined_df.apply(_divide_rand_acc, axis=1)
        print("✅ Divided 'random_access_time_ms' by each row's 'n_repetition_random_access' if non-zero.")

    return combined_df


###############################################################################
#                           SCRIPT 1 LOGIC
###############################################################################

def aggregate_random_access_mss_1(dataframes):
    """
    1. Combine and (potentially) divide by repetition columns row-by-row.
    2. Pivot on 'random_access_ms'.
    3. Return the pivoted DataFrame.
    """
    combined_df = combine_check_and_divide_by_repetition(dataframes)
    if combined_df.empty:
        print("⚠️ No valid data to process (Script 1).")
        return pd.DataFrame()

    # convert to numeric, just in case
    combined_df = combined_df.infer_objects(copy=False)

    # Pivot only on 'random_access_ms' as the values
    pivot_df = combined_df.pivot_table(
        index="table_name",
        columns="version",
        values="random_access_ms",
        aggfunc="sum",
        fill_value=0
    )

    pivot_df.loc['Total'] = pivot_df.sum()
    return pivot_df


def normalize_random_access_mss(df):
    """
    Optional function to compare all columns to '0.0.2'.
    This divides each row's columns by the row's '0.0.2' value, and round(2).
    """
    if "0.0.2" in df.columns:
        df = df.div(df["0.0.2"], axis=0).round(2)
    else:
        print("⚠️ Column '0.0.2' not found. Skipping normalization.")
    return df


def print_markdown_table_1(df):
    """
    Simple helper to show a Markdown version of the pivoted table.
    """
    if df.empty:
        print("⚠️ No data to display (Script 1).")
        return
    table_md = df.to_markdown()
    print("\nAggregated File Sizes Table (Script 1):\n")
    print(table_md)


def main_table_1(datasets: dict, script_dir: Path):
    """
    Creates 'table_1_raw_data.csv' from multiple sources,
    dividing by n_repetition as needed, pivoting, etc.
    """
    public_bi_result_path = script_dir / "../result/random_access/public_bi"

    fastlanes_df = datasets["fastlanes"]
    btrblocks_df = datasets["btrblocks"]
    dwdd_parquet_df = datasets["dwdd_parquet"]
    dwdd_parquet_forced_df = datasets["dwdd_parquet_forced"]
    dwdd_pyarrow_df = datasets["dwdd_pyarrow"]
    duckdb_table_size_df = datasets["dwdd_table_size"]

    aggregated_df = aggregate_random_access_mss_1([
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

        # (Optional) Show a quick markdown view of the normalized data
        normalized_df = normalize_random_access_mss(aggregated_df.copy())
        print_markdown_table_1(normalized_df)


###############################################################################
#                           SCRIPT 2 LOGIC
###############################################################################

def aggregate_random_access_mss_2(dataframes):
    """
    Similar approach for a second pivot or second table creation.
    """
    combined_df = combine_check_and_divide_by_repetition(dataframes)
    if combined_df.empty:
        print("⚠️ No valid data to process (Script 2).")
        return pd.DataFrame()

    combined_df = combined_df.infer_objects(copy=False)

    pivot_df = combined_df.pivot_table(
        index="table_name",
        columns="version",
        values="random_access_ms",
        aggfunc="sum",
        fill_value=0
    )

    return pivot_df


def compute_random_accesss(df):
    """
    Example function that compares everything to 'uncompressed' if desired,
    computing ratio uncompressed/time for each cell.
    """
    if "uncompressed" in df.columns:
        df = df.apply(lambda col: df["uncompressed"] / col, axis=0).round(2)
        df.loc['Overall Average'] = df.mean()
    else:
        print("⚠️ Column 'uncompressed' not found. Skipping random access ratio computation.")
    return df


def print_markdown_table_2(df):
    """
    Show a Markdown table for Script 2 output
    """
    if df.empty:
        print("⚠️ No data to display (Script 2).")
        return
    table_md = df.to_markdown()
    print("\nRandom Access Time(ms):\n")
    print(table_md)


def main_table_2(datasets: dict, script_dir: Path):
    """
    Creates 'table_2_raw_data.csv' from multiple sources,
    dividing by n_repetition as needed, pivoting, etc.
    """
    public_bi_result_path = script_dir / "../result/random_access/public_bi"

    fastlanes_df = datasets["fastlanes"]
    btrblocks_df = datasets["btrblocks"]
    uncompressed_df = datasets["uncompressed"]
    dwdd_df = datasets["dwdd"]
    dwdd_forced_df = datasets["dwdd_forced"]
    dwdd_pyarrow_df = datasets["dwdd_pyarrow"]

    aggregated_df = aggregate_random_access_mss_2([
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

        # Example usage of compute_random_accesss:
        random_access_df = compute_random_accesss(aggregated_df.copy())
        print_markdown_table_2(random_access_df)


###############################################################################
#             NEW RATIO FUNCTION: KEEP '0.0.2' BUT TRANSFORM OTHERS
###############################################################################

def transform_ratios_keep_002(df):
    """
    1) Leaves the '0.0.2' column as-is (just rounding it for neat display).
    2) For every other column, do ratio = column_value / base_value (where base_value = row's '0.0.2').
    3) Round ratio to 2 decimals, appended with an "x".
    """
    result = df.copy()
    result = result.infer_objects()

    if '0.0.2' not in result.columns:
        print("⚠️ '0.0.2' column not found. Skipping ratio transformation.")
        return result

    for idx in result.index:
        base = result.at[idx, '0.0.2']
        if pd.notna(base):
            # Round '0.0.2' for neatness
            result.at[idx, '0.0.2'] = round(base, 5)

        for col in result.columns:
            if col == '0.0.2':
                continue
            val = result.at[idx, col]
            if pd.notna(base) and base != 0:
                ratio = val / base
            else:
                ratio = float('inf')
            ratio_2dec = round(ratio, 2)
            display_str = f"{ratio_2dec:.2f}x"
            result.at[idx, col] = display_str

    return result


###############################################################################
#                           MASTER MAIN
###############################################################################

def master_main():
    script_dir = Path(__file__).parent
    datasets = load_all_datasets(script_dir)

    # -------------------------------------------------------------------------
    # PART A: Aggregation (Table 1 + Table 2)
    # -------------------------------------------------------------------------
    main_table_1(datasets, script_dir)
    main_table_2(datasets, script_dir)

    # -------------------------------------------------------------------------
    # PART B: Re-Load Table 1 -> RATIO-Transform -> Save LaTeX
    # -------------------------------------------------------------------------
    public_bi_result_path = script_dir / "../result/random_access/public_bi"
    input_file_1 = public_bi_result_path / "table_1_raw_data.csv"
    output_file_1 = public_bi_result_path / "table_1.tex"

    df_1 = load_csv_for_latex(input_file_1)
    if not df_1.empty:
        df_1 = df_1.infer_objects(copy=False)
        df_1 = transform_ratios_keep_002(df_1)

        latex_table_1 = generate_latex_table(
            df_1,
            caption="Random Access Time(ms)",
            label="tab:random_access"
        )
        save_latex_table(latex_table_1, output_file_1)
    else:
        print("⚠️ No valid data found in table_1_raw_data.csv. Skipping LaTeX generation (table_1).")

    # -------------------------------------------------------------------------
    # PART C: Load Table 2 -> Standard LaTeX
    # -------------------------------------------------------------------------
    input_file_2 = public_bi_result_path / "table_2_raw_data.csv"
    output_file_2 = public_bi_result_path / "table_2.tex"

    df_2 = load_csv_for_latex(input_file_2)
    if not df_2.empty:
        df_2 = df_2.infer_objects(copy=False)
        latex_table_2 = generate_latex_table(
            df_2,
            caption="Random Access Time(ms)",
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