#!/usr/bin/env python3

import os
import csv
import subprocess


def clone_repo(repo_url, commit_hash, target_dir):
    """
    Clones the repository at a specific commit if the target_dir does not already exist.
    """
    if not os.path.exists(target_dir):
        print(f"Cloning repo at commit {commit_hash} into '{target_dir}'...")
        subprocess.run(["git", "clone", repo_url, target_dir], check=True)
        subprocess.run(["git", "checkout", commit_hash], cwd=target_dir, check=True)
    else:
        print(f"Directory '{target_dir}' already exists. Skipping clone/checkout.")


def sum_third_column(csv_file):
    """
    Reads a CSV file with rows like:
      table_name,version,value
    and returns the SUM of the third column (int).
    """
    total_val = 0
    if not os.path.exists(csv_file):
        print(f"Warning: {csv_file} not found. Returning 0.")
        return 0

    with open(csv_file, newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            # Skip empty or malformed rows
            if len(row) < 3:
                continue
            try:
                val = float(row[2])
                total_val += val
            except ValueError:
                # Could be a header row or invalid row
                continue

    return total_val


def main():
    repo_url = "https://github.com/azimafroozeh/temp"

    # (commit_hash, expression_description), first is baseline
    commits = [
        ("9e227d2", "Baseline"),  # Baseline
        ("e3ba084", "Constant"),
        ("133c348", "Equality"),
        ("9f2dd75", "One-to-One Map"),
        ("55d316e", "FSST12"),
        ("23c1e21", "FSST"),
        ("2be7b92", "ALP"),
        ("c2820bc", "ALP RD"),
        ("74fbb0e", "Frequency"),
        ("12a81d5", "RLE"),
        ("3ec0297", "DELTA"),
        ("264cea9", "CROSS RLE"),
        ("ce85f8b", "FFOR"),
        ("35c65dc", "Dictionary"),
        ("2c96305", "Patch"),
        ("33b3238", "Cast")
    ]

    # We store two metrics for each commit:
    #   1) total_file_size
    #   2) total_decomp_time
    file_size_by_commit = {}
    decomp_time_by_commit = {}

    # Clone each commit and sum the relevant CSVs
    for (commit_hash, expression) in commits:
        target_dir = f"repo_{commit_hash}"
        clone_repo(repo_url, commit_hash, target_dir)

        # 1) File size from compression_ratio fastlanes.csv
        size_csv = os.path.join(
            target_dir,
            "benchmark",
            "result",
            "compression_ratio",
            "public_bi",
            "fastlanes.csv"
        )
        total_size = sum_third_column(size_csv)
        file_size_by_commit[commit_hash] = total_size

        # 2) Decompression time from decompression_time fastlanes.csv
        time_csv = os.path.join(
            target_dir,
            "benchmark",
            "result",
            "decompression_time",
            "public_bi",
            "fastlanes.csv"
        )
        print(time_csv)
        total_time = sum_third_column(time_csv)
        decomp_time_by_commit[commit_hash] = total_time

    # Identify baseline
    baseline_commit, _ = commits[0]
    baseline_size = file_size_by_commit[baseline_commit]
    baseline_time = decomp_time_by_commit[baseline_commit]

    # We'll gather rows with: (expression, diff_size_val, diff_size_str, diff_time_val, diff_time_str)
    result_rows = []
    for (commit_hash, expression) in commits[1:]:
        cur_size = file_size_by_commit[commit_hash]
        cur_time = decomp_time_by_commit[commit_hash]

        # --------- Size difference calculation ---------
        if baseline_size == 0:
            diff_size_val = 0.0
            diff_size_str = "N/A"
        else:
            diff_size_val = cur_size - baseline_size
            diff_size_pct = (diff_size_val / baseline_size) * 100.0

            epsilon = 1e-12
            if abs(diff_size_val) < epsilon:
                diff_size_val = 0.0
                diff_size_str = "0.00\\%"
            else:
                sign = "+" if diff_size_val > 0 else "-"
                diff_size_str = f"{sign}{abs(diff_size_pct):.2f}\\%"

        # --------- Time difference calculation ---------
        if baseline_time == 0:
            diff_time_val = 0.0
            diff_time_str = "N/A"
        else:
            diff_time_val = cur_time - baseline_time
            diff_time_pct = (diff_time_val / baseline_time) * 100.0

            epsilon = 1e-12
            if abs(diff_time_val) < epsilon:
                diff_time_val = 0.0
                diff_time_str = "0.00\\%"
            else:
                sign = "+" if diff_time_val > 0 else "-"
                diff_time_str = f"{sign}{abs(diff_time_pct):.2f}\\%"

        # Store
        result_rows.append((expression, diff_size_val, diff_size_str, diff_time_val, diff_time_str))

    # Sort the table by size difference descending:
    # largest positive difference (worst regression) first
    result_rows.sort(key=lambda x: x[1], reverse=True)

    # Build LaTeX lines
    latex_lines = []
    latex_lines.append(r"\begin{table}[ht]")
    latex_lines.append(r"\centering")
    # 3 columns: Expression, Improvement(Size), Improvement(Time)
    latex_lines.append(r"\begin{tabular}{l r r}")
    latex_lines.append(r"\toprule")
    latex_lines.append(r"Expression & FileSize & Decompression Time \\")
    latex_lines.append(r"  & Improvement & Improvement \\")
    latex_lines.append(r"\midrule")

    for (expression, diff_size_val, diff_size_str, diff_time_val, diff_time_str) in result_rows:
        # We'll just show the two difference strings
        latex_lines.append(rf"{expression} & {diff_size_str} & {diff_time_str} \\")

    latex_lines.append(r"\bottomrule")
    latex_lines.append(r"\end{tabular}")
    latex_lines.append(
        r"\caption{Comparison of file size and decompression time improvements (sorted by size difference).}")
    latex_lines.append(r"\label{tab:compression-decomptime-diff}")
    latex_lines.append(r"\end{table}")

    out_file = "expression_table.tex"
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("\n".join(latex_lines) + "\n")

    print("\n=== Generated LaTeX Table (saved to expression_table.tex) ===\n")
    print("\n".join(latex_lines))


if __name__ == "__main__":
    main()
