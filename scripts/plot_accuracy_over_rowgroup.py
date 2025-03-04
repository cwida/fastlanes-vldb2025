#!/usr/bin/env python3

import os
import pandas as pd

BASE_DIR = "../fastlanes/benchmark/result/accuracy_over_rowgroup"  # Adjust if needed
CSV_FILENAME = "fastlanes_detailed.csv"  # The file containing the 10-column format

def load_csv_as_df(file_path):
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading CSV {file_path}: {e}")
        return None
    if df.empty:
        return None
    return df

def main():
    # 1) Load the base CSV from directory '0'
    base_csv_path = os.path.join(BASE_DIR, "0", "public_bi", CSV_FILENAME)
    base_df = load_csv_as_df(base_csv_path)
    if base_df is None:
        print(f"Warning: Base CSV file not loaded or empty: {base_csv_path}")
        return

    if "expression" not in base_df.columns:
        print(f"CSV file {base_csv_path} does not contain the 'expression' column.")
        return
    base_expression_series = base_df["expression"]

    # 2) Compare each directory 1..45 to the base, row by row, comparing only the "expression" column.
    for i in range(1, 2):
        dir_str = str(i)
        current_csv_path = os.path.join(BASE_DIR, dir_str, "public_bi", CSV_FILENAME)
        current_df = load_csv_as_df(current_csv_path)
        if current_df is None:
            print(f"Warning: CSV file not loaded or empty: {current_csv_path}")
            continue

        if "expression" not in current_df.columns:
            print(f"CSV file {current_csv_path} does not contain the 'expression' column.")
            continue
        current_expression_series = current_df["expression"]

        diff_count = 0

        # Compare the expression values row by row up to the shortest file length.
        min_len = min(len(base_expression_series), len(current_expression_series))
        for idx in range(min_len):
            base_val = base_expression_series.iloc[idx]
            current_val = current_expression_series.iloc[idx]
            if base_val != current_val:
                print(f'base expression : {base_val}, current_val : {current_val}!')
                diff_count += 1


        print(f"Directory {dir_str}: {diff_count} differences in expression (compared to base 0).")

if __name__ == "__main__":
    main()
