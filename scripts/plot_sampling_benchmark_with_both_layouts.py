import csv
import os
import matplotlib.pyplot as plt
import subprocess
import shutil
import seaborn as sns
from matplotlib.ticker import FixedLocator
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
mpl.rcParams.update({'mathtext.default': 'regular'})  # Ensure font applies to all text

# Define a common font dictionary for customization
FONT_DICT = {'family': FONT_FAMILY, 'size': 14}  # Bigger font size for tick labels
LABEL_FONT_DICT = {'family': FONT_FAMILY, 'size': 16}  # Bigger font size for axis labels


def clone_repo(commit_hash, target_dir):
    """
    Clones the Fastlanes repository at a specific commit if it does not already exist.
    """
    repo_url = "https://github.com/azimafroozeh/temp.git"  # Replace with actual repo URL
    if not os.path.exists(target_dir):
        subprocess.run(["git", "clone", repo_url, target_dir], check=True)
        subprocess.run(["git", "checkout", commit_hash], cwd=target_dir, check=True)


def process_fastlanes_csv(directory, repo_dir):
    """
    Reads the fastlanes.csv file from sampling_benchmark/<directory>/public_bi/
    and sums the file_size (assumed to be the third column).
    """
    filepath = os.path.join(
        repo_dir, "benchmark/result/sampling_benchmark",
        directory, "public_bi", "fastlanes.csv"
    )
    total_size = 0

    try:
        with open(filepath, newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                if not row:
                    continue
                try:
                    file_size = int(row[2])  # file_size in 3rd column
                    total_size += file_size
                except ValueError:
                    continue  # Skip header/malformed rows
    except FileNotFoundError:
        print(f"Warning: File {filepath} not found.")

    return total_size


def compute_accuracy(total_size, baseline):
    """
    Computes accuracy as the percentage difference from the baseline.
    """
    if baseline == 0:
        return 0
    diff_ratio = abs(total_size - baseline) / baseline
    accuracy = (1 - diff_ratio) * 100
    return max(0, accuracy)


def helper_plot(x_values, accuracy_values_1, accuracy_values_2, method_1, method_2):
    """
    Plots accuracy values for two different methods with x and y-axis tick lines.
    Adds a dark border around the figure.
    """
    sns.set_style("white")
    fig, ax = plt.subplots(figsize=(12, 4))

    # Plot lines
    # New eye-friendly colors
    color_teal = "#1b9e77"  # Soft teal instead of green
    color_brick = "#d95f02"  # Brick red instead of bright red

    # Plot lines
    ax.plot(x_values, accuracy_values_1, marker='o', linestyle='-', color=color_teal, label=method_1)
    ax.plot(x_values, accuracy_values_2, marker='o', linestyle='-', color=color_brick, label=method_2)

    # Labels and ticks
    ax.set_xlabel('Number of Vectors', **LABEL_FONT_DICT)
    ax.set_ylabel('Accuracy (%)', **LABEL_FONT_DICT)
    ax.set_xticks([1, 3, 7, 16, 32, 64])
    ax.set_xticklabels([1, 3, 7, 16, 32, 64], **FONT_DICT)
    ax.set_yticks([79, 84, 99, 100])
    ax.set_yticklabels([79, 84, 99, 100], **FONT_DICT)

    # Grid lines
    ax.grid(axis='x', linestyle='--', linewidth=0.5)
    ax.grid(axis='y', linestyle='--', linewidth=0.5)

    # Legend
    ax.legend(fontsize=14)

    # Add a dark border around the figure
    for spine in ax.spines.values():
        spine.set_edgecolor('black')  # Set color to black
        spine.set_linewidth(2)  # Set border thickness

    # Save the figure
    plt.savefig("sampling_benchmark_accuracy_comparison.svg", bbox_inches='tight', facecolor='white')


def main():
    commit_1 = "4c893f0"
    commit_2 = "62fa6b4"
    method_1 = "Three-way"
    method_2 = "Sequential"

    # Clone repositories at specified commits
    clone_repo(commit_1, method_1)
    clone_repo(commit_2, method_2)

    # Initialize results dictionaries
    results_1 = {}
    results_2 = {}

    for i in range(65):
        dir_name = str(i)
        results_1[dir_name] = process_fastlanes_csv(dir_name, method_1)
        results_2[dir_name] = process_fastlanes_csv(dir_name, method_2)

    all_data_total_1 = results_1.pop("0", 0)
    all_data_total_2 = results_2.pop("0", 0)

    x_values = list(range(1, 65))
    accuracy_values_1 = [compute_accuracy(results_1.get(str(i), 0), all_data_total_1) for i in x_values]
    accuracy_values_2 = [compute_accuracy(results_2.get(str(i), 0), all_data_total_2) for i in x_values]

    # Generate the plot with a dark frame
    helper_plot(x_values, accuracy_values_1, accuracy_values_2, method_1, method_2)


if __name__ == "__main__":
    main()
