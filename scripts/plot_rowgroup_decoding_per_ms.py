#!/usr/bin/env python3

import os
import pandas as pd
import numpy as np
from pathlib import Path
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.font_manager as fm
import matplotlib as mpl

# ---- FONT SETUP ----
FONT_FAMILY = "Times New Roman"

# Check if the font is available
available_fonts = sorted(set(f.name for f in fm.fontManager.ttflist))
if FONT_FAMILY in available_fonts:
    print(f"✅ '{FONT_FAMILY}' is available.")
else:
    print(f"⚠️ '{FONT_FAMILY}' not found. Falling back to default.")

# Set the font globally
mpl.rcParams['font.family'] = FONT_FAMILY  # Force font globally
mpl.rcParams.update({'mathtext.default': 'regular'})  # Ensure font applies to math text

# Define common font dictionaries
FONT_DICT = {'family': FONT_FAMILY, 'size': 14}
LABEL_FONT_DICT = {'family': FONT_FAMILY, 'size': 16}


###############################################################################
#                            CSV LOADING
###############################################################################

def load_csv_for_latex(file_path):
    file = Path(file_path)
    if file.exists():
        return pd.read_csv(file, index_col=0)
    else:
        print(f"⚠️ File not found: {file_path}")
        return pd.DataFrame()


###############################################################################
#                            MAKE LATEX TABLE
###############################################################################

def make_latex_table_from_total_row(df, table_caption="Comparison of Total Row"):
    """
    Builds a small LaTeX table from the 'Total' row of a DataFrame.
    Each column in 'Total' becomes a row in the LaTeX table.
    """
    if "Total" not in df.index:
        print("⚠️ The DataFrame has no row named 'Total'. Skipping table generation.")
        return ""

    # Extract the 'Total' row as a Series
    total_series = df.loc["Total"]

    # Build LaTeX lines
    latex_lines = []
    latex_lines.append(r"\begin{table}[ht]")
    latex_lines.append(r"\centering")
    latex_lines.append(r"\begin{tabular}{l r}")
    latex_lines.append(r"\toprule")
    latex_lines.append(r"Format & Total \\")  # Table header
    latex_lines.append(r"\midrule")

    # For each column in the 'Total' row, we create one row in the table
    for column_name, value in total_series.items():
        # Format the value as needed, e.g. 2 decimal places
        latex_lines.append(rf"{column_name} & {value:.2f} \\")

    latex_lines.append(r"\bottomrule")
    latex_lines.append(r"\end{tabular}")
    latex_lines.append(rf"\caption{{{table_caption}}}")
    latex_lines.append(r"\label{tab:total-row}")
    latex_lines.append(r"\end{table}")

    # Join everything into a single LaTeX string
    return "\n".join(latex_lines)


###############################################################################
#                            PLOTTING LOGIC
###############################################################################

def plot_decompression_time(df, output_file="../result/decompression_time/public_bi/rowgroup_decoding_per_ms_ticker.png"):
    """
    Plots decoding throughput (rowgroups/ms) by taking 1 / decompression_time,
    includes a new 'Total' row, and uses thicker bars with an outlined edge.
    """
    if df.empty:
        print("⚠️ No data available to plot.")
        return

    # Convert decompression time to throughput
    df = 1 / df

    # Add a "Total" row at the bottom
    total = df.sum()
    normalized_total = total / total.max()
    # We scale by #rows to emphasize it visually, but you can adjust or remove if preferred
    print(df.loc["Total"])
    df.loc["Total"] = df.loc["Total"] * 36
    df.rename(index={"Total": "AVG"}, inplace=True)

    plt.figure(figsize=(18, 4))
    sns.set_style("whitegrid")

    # Define name mappings
    name_mapping = {"0.0.2": "FastLanes"}
    default_labels = ["Parquet+Snappy", "Parquet+Zstd", "BtrBlocks", "DuckDB"]
    index = 0

    # Map column names
    mapped_columns = []
    for col in df.columns:
        if col in name_mapping:
            mapped_columns.append(name_mapping[col])
        else:
            mapped_columns.append(default_labels[index % len(default_labels)])
            index += 1

    df.columns = mapped_columns  # Apply new names

    # Define custom colors in order
    custom_colors = [
        "#2ca02c",  # Green
        "#008CFF",  # Blue
        "#ff7f0e",  # Orange
        "#d62728",  # Red
        "#9467bd"   # Purple
    ]

    # Cycle through colors if needed
    color_mapping = {col: custom_colors[i % len(custom_colors)] for i, col in enumerate(df.columns)}

    # Plot with thicker bars (linewidth=2) and black edges
    ax = df.plot(
        kind="bar",
        figsize=(18, 5),
        color=[color_mapping[col] for col in df.columns],
        linewidth=2,
        edgecolor='black',
        width=0.8  # Adjust bar width if you want them thinner or thicker
    )

    plt.xlabel("Public Bi Dataset", **LABEL_FONT_DICT)
    plt.ylabel("Decoding Throughput (rowgroups/ms)", **LABEL_FONT_DICT)

    # Truncate each table name on the x-axis to 5 characters (except "Total")
    short_labels = []
    for row_name in df.index:
        if row_name == "Total":
            short_labels.append("Total")
        else:
            short_labels.append(row_name[:5])

    ax.set_xticklabels(short_labels, rotation=45, ha="right", **FONT_DICT)

    plt.yticks(**FONT_DICT)

    # Use symlog scale to give more space near zero
    ax.set_yscale("symlog", linthresh=0.5)

    # Manually set y-ticks for better control
    y_ticks = [0, 0.1, 0.5, 1, 5, 10, 50, 100, 500, 1000]
    ax.set_yticks(y_ticks)
    ax.set_yticklabels([str(tick) for tick in y_ticks])

    # Remove x-axis grid lines
    ax.grid(axis="x", linestyle="")
    ax.tick_params(axis='x', length=0)
    ax.spines['bottom'].set_visible(False)

    # Legend inside the plot
    ax.legend(title="File Formats", loc="upper right", frameon=True, fontsize=12, bbox_to_anchor=(0.95, 0.95))

    plt.grid(axis="y", linestyle="--", alpha=0.7)

    # Add a black frame around the plot
    for spine in ax.spines.values():
        spine.set_visible(True)
        spine.set_color("black")
        spine.set_linewidth(2)

    plt.tight_layout()

    # Save plots in multiple formats (e.g. PNG, SVG)
    output_path_svg = output_file.replace(".png", ".svg")
    plt.savefig(output_file, format='png', bbox_inches='tight')
    plt.savefig(output_path_svg, format='svg', bbox_inches='tight')
    plt.close()

    print(f"✅ Throughput plot saved: {output_file}")
    print(f"✅ Throughput plot also saved as: {output_path_svg}")


###############################################################################
#                            MAIN
###############################################################################

def main():
    script_dir = Path(__file__).parent
    public_bi_result_path = script_dir / "../result/decompression_time/public_bi"
    input_file_1 = public_bi_result_path / "table_1_raw_data.csv"

    # 1) Load CSV
    df_1 = load_csv_for_latex(input_file_1)

    if not df_1.empty:
        # 2) Plot decoding time throughput
        plot_decompression_time(df_1)

        # 3) Create a LaTeX table from the "Total" row
        latex_code = make_latex_table_from_total_row(df_1, "Throughput Totals by Format")
        if latex_code:
            print("\n=== LaTeX Table for 'Total' Row ===")
            print(latex_code)
            # Optionally write the LaTeX table to a file
            with open("total_row_table.tex", "w", encoding="utf-8") as f:
                f.write(latex_code + "\n")
    else:
        print("⚠️ No valid data found in table_1_raw_data.csv. Skipping plot generation.")


if __name__ == "__main__":
    main()
