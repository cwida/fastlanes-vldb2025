#!/usr/bin/env python3

import os
import glob
import csv
from collections import defaultdict
from pathlib import Path

#
# --------------------------------------------------------------------------
# 1) GROUP-BY-FILE+VERSION (Needed for Snappy & Zstd times)
# --------------------------------------------------------------------------
#
def compute_times_by_file_and_version(directory_path, repetition_column):
    """
    Reads all CSV files in 'directory_path'. For each CSV file, we group
    rows by the 'version' column and sum:
        (decompression_time_ms / row[repetition_column])

    Returns a dict:
      {
         "filename1.csv": {
            "autoschema_snappy_V2": <sum>,
            "autoschema_zstd_V2":   <sum>,
            "some_other_version":   <sum>,
            ...
         },
         "filename2.csv": { ... },
         ...
      }
    """
    sums_by_file_and_version = {}

    csv_files = glob.glob(os.path.join(directory_path, '*.csv'))
    if not csv_files:
        print(f"No CSV files found in '{directory_path}'")
        return {}

    for csv_file in csv_files:
        version_sums = defaultdict(float)

        with open(csv_file, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if (
                        "decompression_time_ms" not in row
                        or repetition_column not in row
                        or "version" not in row
                ):
                    continue

                dec_str = row["decompression_time_ms"]
                rep_str = row[repetition_column]
                ver_str = row["version"]

                try:
                    dec_val = float(dec_str)
                    rep_val = float(rep_str)
                except ValueError:
                    continue

                if rep_val == 0:
                    continue

                version_sums[ver_str] += dec_val / rep_val

        sums_by_file_and_version[os.path.basename(csv_file)] = dict(version_sums)

    return sums_by_file_and_version

#
# --------------------------------------------------------------------------
# 2) WHOLE-FILE SUM (DuckDB & Fastlanes) for SSE-based speedups
# --------------------------------------------------------------------------
#
def compute_times_by_file(directory_path, repetition_column):
    """
    Reads all CSV files in 'directory_path', sums
    (decompression_time_ms / repetition_value) for each CSV,
    and returns { csv_filename: total_time_for_this_file }.
    """
    sums_by_file = {}

    csv_files = glob.glob(os.path.join(directory_path, '*.csv'))
    if not csv_files:
        print(f"No CSV files found in '{directory_path}'")
        return {}

    for csv_file in csv_files:
        total_time_for_this_file = 0.0
        with open(csv_file, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if "decompression_time_ms" not in row or repetition_column not in row:
                    continue

                dec_str = row["decompression_time_ms"]
                rep_str = row[repetition_column]

                try:
                    dec_val = float(dec_str)
                    rep_val = float(rep_str)
                except ValueError:
                    continue

                if rep_val == 0:
                    continue

                total_time_for_this_file += dec_val / rep_val

        sums_by_file[os.path.basename(csv_file)] = total_time_for_this_file

    return sums_by_file

#
# --------------------------------------------------------------------------
# 3) EXTENDED LATEX TABLE: SSE/AVX2/AVX512 + Snappy/Zstd (TIME + SPEEDUP)
# --------------------------------------------------------------------------
#
def create_latex_table_extended_with_snappy_zstd_speedup(
        duckdb_times,           # dict {filename -> total_time} from compute_times_by_file
        fastlanes_times,        # dict {filename -> total_time}
        duckdb_file_and_version # dict {filename -> {version -> sum_of_times}} from compute_times_by_file_and_version
):
    """
    Produce a single LaTeX table with 9 columns:

    1) ISA
    2) DuckDB Time (ms)
    3) DuckDB Speedup (%)
    4) Fastlanes Time (ms)
    5) Fastlanes Speedup (%)
    6) Snappy Time (ms)
    7) Snappy Speedup (%)
    8) Zstd Time (ms)
    9) Zstd Speedup (%)

    All speedups are calculated using SSE as the baseline for each category.
    """

    # 1) Map DuckDB filenames -> ISA
    duckdb_isa_map = {
        "parquet_compressed_with_duckdb_parquet_cpp.csv":         "SSE",
        "parquet_compressed_with_duckdb_parquet_cpp_avx2.csv":    "AVX2",
        "parquet_compressed_with_duckdb_parquet_cpp_avx512f.csv": "AVX512"
    }
    # Build inverse map: ISA -> filename
    inv_duckdb_isa_map = {v: k for k,v in duckdb_isa_map.items()}

    # 2) Map Fastlanes filenames -> ISA
    fastlanes_isa_map = {
        "fastlanes_20_o3.csv":       "SSE",
        "fastlanes_20_avx2.csv":     "AVX2",
        "fastlanes_20_avx512dq.csv": "AVX512"
    }

    # 3) Build dict: times_duckdb[ISA], times_fastlanes[ISA]
    times_duckdb = {}
    for fname, tval in duckdb_times.items():
        if fname in duckdb_isa_map:
            isa = duckdb_isa_map[fname]
            times_duckdb[isa] = tval

    times_fastlanes = {}
    for fname, tval in fastlanes_times.items():
        if fname in fastlanes_isa_map:
            isa = fastlanes_isa_map[fname]
            times_fastlanes[isa] = tval

    # 4) Baseline SSE times
    duckdb_sse_time    = times_duckdb.get("SSE", 0.0)
    fastlanes_sse_time = times_fastlanes.get("SSE", 0.0)

    # For Snappy + Zstd, we look in the SSE file’s version dictionary
    sse_filename   = inv_duckdb_isa_map.get("SSE", None)
    snappy_sse_val = 0.0
    zstd_sse_val   = 0.0
    if sse_filename and (sse_filename in duckdb_file_and_version):
        version_dict_sse = duckdb_file_and_version[sse_filename]
        snappy_sse_val   = version_dict_sse.get("autoschema_snappy_V2", 0.0)
        zstd_sse_val     = version_dict_sse.get("autoschema_zstd_V2",   0.0)

    def pct_speedup(baseline, current):
        if baseline == 0.0 or current == 0.0:
            return 0.0
        return (baseline / current - 1.0) * 100.0

    # 5) Build row data for SSE, AVX2, AVX512
    row_data = []
    for isa in ["SSE", "AVX2", "AVX512"]:
        # Whole-file times for DuckDB & Fastlanes
        duckdb_t = times_duckdb.get(isa, 0.0)
        fast_t   = times_fastlanes.get(isa, 0.0)

        duckdb_speedup = pct_speedup(duckdb_sse_time, duckdb_t)
        fast_speedup   = pct_speedup(fastlanes_sse_time, fast_t)

        # Snappy & Zstd times from the file+version dictionary
        duckdb_filename = inv_duckdb_isa_map.get(isa, None)
        snappy_val      = 0.0
        zstd_val        = 0.0
        if duckdb_filename and (duckdb_filename in duckdb_file_and_version):
            ver_dict = duckdb_file_and_version[duckdb_filename]
            snappy_val = ver_dict.get("autoschema_snappy_V2", 0.0)
            zstd_val   = ver_dict.get("autoschema_zstd_V2",   0.0)

        # Speedups for Snappy & Zstd vs SSE
        snappy_sp = pct_speedup(snappy_sse_val, snappy_val)
        zstd_sp   = pct_speedup(zstd_sse_val,   zstd_val)

        row_data.append((
            isa,
            duckdb_t, duckdb_speedup,
            fast_t,   fast_speedup,
            snappy_val, snappy_sp,
            zstd_val,   zstd_sp
        ))

    # 6) Build the LaTeX table (9 columns)
    latex_lines = []
    latex_lines.append(r"\begin{table}[ht]")
    latex_lines.append(r"\centering")
    latex_lines.append(
        r"\begin{tabular}{l"       # ISA
        r"                S[table-format=7.2]"  # DuckDB time
        r"                S[table-format=5.2]"  # DuckDB speedup
        r"                S[table-format=7.2]"  # Fastlanes time
        r"                S[table-format=5.2]"  # Fastlanes speedup
        r"                S[table-format=7.2]"  # Snappy time
        r"                S[table-format=5.2]"  # Snappy speedup
        r"                S[table-format=7.2]"  # Zstd time
        r"                S[table-format=5.2]}" # Zstd speedup
    )
    latex_lines.append(r"\toprule")

    latex_lines.append(
        r"\textbf{ISA} & "
        r"\multicolumn{2}{c}{\textbf{DuckDB (Whole-file)}} & "
        r"\multicolumn{2}{c}{\textbf{FastLanes}} & "
        r"\multicolumn{2}{c}{\textbf{Snappy}} & "
        r"\multicolumn{2}{c}{\textbf{Zstd}} \\"
    )
    latex_lines.append(
        r" & \textbf{Time (ms)} & \textbf{Speedup} "
        r" & \textbf{Time (ms)} & \textbf{Speedup} "
        r" & \textbf{Time (ms)} & \textbf{Speedup} "
        r" & \textbf{Time (ms)} & \textbf{Speedup} \\"
    )
    latex_lines.append(r"\midrule")

    for (isa, duckdb_t, duckdb_sp, fast_t, fast_sp, snappy_val, snappy_sp, zstd_val, zstd_sp) in row_data:
        latex_lines.append(
            rf"{isa}"
            rf" & {duckdb_t:7.2f} & {duckdb_sp:5.2f}\%"
            rf" & {fast_t:7.2f} & {fast_sp:5.2f}\%"
            rf" & {snappy_val:7.2f} & {snappy_sp:5.2f}\%"
            rf" & {zstd_val:7.2f} & {zstd_sp:5.2f}\% \\"
        )

    latex_lines.append(r"\bottomrule")
    latex_lines.append(r"\end{tabular}")
    latex_lines.append(
        r"\caption{SSE/AVX2/AVX512 comparison, with time \& speedup for Snappy "
        r"and Zstd (all speedups use SSE as baseline).}"
    )
    latex_lines.append(r"\label{tab:comparison-snappy-zstd-speedups}")
    latex_lines.append(r"\end{table}")

    return "\n".join(latex_lines)

#
# --------------------------------------------------------------------------
# 4) MAIN
# --------------------------------------------------------------------------
#

def main():
    repo_dir = Path(__file__).parent.resolve() / ".."

    # ----------------------------------------------------------------------
    # A) Collect File+Version data from DuckDB (for Snappy/Zstd)
    # ----------------------------------------------------------------------
    duckdb_directory = repo_dir / "duckdb" / "result" / "simd_benchmark" / "public_bi"
    duckdb_file_and_version = compute_times_by_file_and_version(
        directory_path = duckdb_directory,
        repetition_column = "n_repetition_full_scan"
    )

    # ----------------------------------------------------------------------
    # B) Collect Whole-file times from DuckDB and Fastlanes for SSE speedups
    # ----------------------------------------------------------------------
    duckdb_times = compute_times_by_file(
        directory_path = duckdb_directory,
        repetition_column = "n_repetition_full_scan"
    )

    fastlanes_directory = repo_dir / "fastlanes" / "benchmark" / "result" / "simd_benchmark" / "public_bi"
    fastlanes_times = compute_times_by_file(
        directory_path = fastlanes_directory,
        repetition_column = "n_repetition"
    )

    if not duckdb_times and not fastlanes_times:
        print("\nNo data found. Check your CSV paths/columns.\n")
        return

    # ----------------------------------------------------------------------
    # C) Produce the Extended Table (SSE/AVX2/AVX512) + Snappy/Zstd Time+Speedup
    # ----------------------------------------------------------------------
    latex_code = create_latex_table_extended_with_snappy_zstd_speedup(
        duckdb_times           = duckdb_times,
        fastlanes_times        = fastlanes_times,
        duckdb_file_and_version= duckdb_file_and_version
    )

    print("\n=== Extended Table: SSE/AVX2/AVX512 + Snappy/Zstd (Time + Speedup) ===\n")
    print(latex_code)

if __name__ == "__main__":
    main()
