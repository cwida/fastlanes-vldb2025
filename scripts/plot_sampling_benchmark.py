import csv
import os
import matplotlib.pyplot as plt
from helper.plot_sampling_benchmark_helper import *


def process_fastlanes_csv(directory):
    """
    Reads the fastlanes.csv file from sampling_benchmark/<directory>/public_bi/
    and sums the file_size (assumed to be the third column).
    """
    filepath = os.path.join(
        "../fastlanes/benchmark/result/sampling_benchmark",
        directory,
        "public_bi",
        "fastlanes.csv"
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
                    # Skip header/malformed rows
                    continue
    except FileNotFoundError:
        print(f"Warning: File {filepath} not found.")

    return total_size


def set_style():
    """
    Attempts to set the Matplotlib style.
    Falls back if 'seaborn-whitegrid' is unavailable.
    """
    try:
        plt.style.use('seaborn-whitegrid')
    except OSError:
        available = plt.style.available
        if 'seaborn' in available:
            plt.style.use('seaborn')
        else:
            plt.style.use('default')


def plot_all_accuracy(x_values, accuracy_values):
    """
    Plots all accuracy values vs. sample sizes as a simple line plot,
    ensuring that all x-axis ticks are shown.
    """

    plt.figure(figsize=(12, 4))
    plt.plot(x_values, accuracy_values, marker='o', linestyle='-', color='red', label='Accuracy (%)')

    plt.xlabel('Sample Size as Number of Vectors', fontsize=14)
    plt.ylabel('Accuracy (%)', fontsize=14)
    plt.title('Overall Accuracy Trend')

    plt.xticks(x_values)  # Ensure all x-axis ticks are shown
    plt.grid(True)
    plt.legend(fontsize=12)

    result_dir = "../result/sampling_benchmark/"
    os.makedirs(result_dir, exist_ok=True)
    plot_path = os.path.join(result_dir, "sampling_benchmark_accuracy_simple.svg")
    plt.savefig(plot_path)
    print(f"Plot saved to: {plot_path}")


def main():
    results = {}
    # Process directories '0' through '64'
    for i in range(65):
        dir_name = str(i)
        total = process_fastlanes_csv(dir_name)
        results[dir_name] = total

    # Rename directory '0' as 'all_data'
    all_data_total = results.pop("0", 0)
    results["all_data"] = all_data_total  # reinsert with new key

    # Print header
    # print("sample_size,total_size,accuracy(%)")

    # Sort keys so 'all_data' appears first
    sorted_keys = sorted(
        results.keys(),
        key=lambda k: (0, k) if k == 'all_data' else (1, k)
    )

    # Accuracy function
    def compute_accuracy(total_size, baseline):
        if baseline == 0:
            return 0
        diff_ratio = abs(total_size - baseline) / baseline
        accuracy = (1 - diff_ratio) * 100
        return max(0, accuracy)

    # Print CSV lines
    # for key in sorted_keys:
    #     total_size = results[key]
    #     accuracy = compute_accuracy(total_size, all_data_total)
    #     print(f"{key},{total_size},{accuracy:.2f}")

    # Prepare data for plotting
    x_values = list(range(1, 65))  # 1..64
    accuracy_values = [
        compute_accuracy(results.get(str(i), 0), all_data_total)
        for i in x_values
    ]

    helper_plot(x_values, accuracy_values)
    plot_all_accuracy(x_values, accuracy_values)


if __name__ == "__main__":
    main()
