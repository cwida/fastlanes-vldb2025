#!/usr/bin/env python

import shutil
import subprocess
import sys
from pathlib import Path
import pandas as pd


def run_command(command, error_message, log_file_path, cwd=None):
    """Run a shell command, store output in a log file, and handle errors."""
    try:
        with open(log_file_path, "a") as log_file:
            result = subprocess.run(
                command, shell=True, check=True, cwd=cwd,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
            )

            log_file.write(f"Command: {command}\n")
            log_file.write(f"Output:\n{result.stdout}\n")
            log_file.write(f"{'-' * 50}\n")

            return result.stdout.strip()

    except subprocess.CalledProcessError as e:
        log_error(error_message, e.stderr, log_file_path)
        print(f"\033[31m-- Error: {error_message}\033[0m", file=sys.stderr)
        sys.exit(1)


def log_error(message, details, log_file_path):
    """Log errors to a file."""
    with open(log_file_path, "a") as log_file:
        log_file.write(f"ERROR: {message}\n")
        log_file.write(f"DETAILS: {details}\n")
        log_file.write(f"{'-' * 50}\n")


def colored_echo(color, message):
    """Print colored messages to the terminal."""
    colors = {"green": "32", "brown": "33", "black": "30", "red": "31"}
    print(f"\033[{colors[color]}m-- {message}\033[0m")


def setup_workspace():
    """Set up workspace variables."""
    colored_echo("green", "Setting up workspace variables...")
    workspace = Path.cwd()
    repos = [
        ("https://github.com/cwida/FastLanes.git", "fastlanes", "release_v0.1"),
        ("https://github.com/cwida/FastLanes_Data.git", "data_repo", "main"),
        ("https://github.com/cwida/btrblocks-vldb2025.git", "btrblocks", "main"),
        ("https://github.com/cwida/duckdb-vldb2025.git", "duckdb", "main"),
    ]
    log_dir = workspace / "logs"
    log_dir.mkdir(exist_ok=True)
    scripts_dir = workspace / "scripts"
    result_dir = workspace / "result"

    # Clean up result directory before execution
    clean_result_directory(result_dir)

    # Ensure that result/compression_ratio/public_bi exists
    public_bi_result_dir = result_dir / "compression_ratio" / "public_bi"
    public_bi_result_dir.mkdir(parents=True, exist_ok=True)
    colored_echo("green", f"Ensured directory exists: {public_bi_result_dir}")

    return workspace, repos, log_dir, scripts_dir, result_dir


def clean_result_directory(result_dir):
    """Remove all files and subdirectories inside the result directory."""
    if result_dir.exists():
        for item in result_dir.iterdir():
            if item.is_dir():
                # Use rmtree to remove subdirectory and all its contents
                shutil.rmtree(item)
            else:
                # Remove file
                item.unlink()
        colored_echo("green", f"Cleaned up all files in {result_dir}.")
    else:
        colored_echo("brown", f"Result directory {result_dir} does not exist. Creating it now...")
        result_dir.mkdir(parents=True)


def clone_or_update_repo(repo_url, repo_dir, branch_or_commit, log_file_path):
    """Clone or update a Git repository and check out a specified commit if given."""
    if repo_dir.exists():
        colored_echo("green", f"{repo_dir.name} repository already exists. Checking for updates...")
        run_command("git fetch origin", "Failed to fetch changes", log_file_path, cwd=repo_dir)

        current_commit = run_command("git rev-parse HEAD", "Failed to get current commit", log_file_path, cwd=repo_dir)
        current_commit_message = run_command("git log -1 --pretty=%B", "Failed to get current commit message",
                                             log_file_path, cwd=repo_dir)

        colored_echo("brown", f"Current commit: {current_commit[:7]}")
        colored_echo("brown", f"Commit message: {current_commit_message}")

        if len(branch_or_commit) == 40:  # Likely a commit hash
            colored_echo("green", f"Checking out commit {branch_or_commit} in {repo_dir.name}...")
            run_command(f"git checkout {branch_or_commit}", f"Failed to checkout commit {branch_or_commit}",
                        log_file_path, cwd=repo_dir)
        else:
            latest_commit = run_command(f"git rev-parse origin/{branch_or_commit}",
                                        "Failed to get latest commit", log_file_path, cwd=repo_dir)
            latest_commit_message = run_command(f"git log -1 --pretty=%B origin/{branch_or_commit}",
                                                "Failed to get latest commit message", log_file_path, cwd=repo_dir)

            colored_echo("brown", f"Latest commit on branch {branch_or_commit}: {latest_commit[:7]}")
            colored_echo("brown", f"Commit message: {latest_commit_message}")

            if current_commit != latest_commit:
                colored_echo("green", f"Pulling latest changes for {repo_dir.name}...")
                run_command(f"git checkout {branch_or_commit} && git pull origin {branch_or_commit}",
                            f"Failed to pull changes in {repo_dir}", log_file_path, cwd=repo_dir)
            else:
                colored_echo("green", f"{repo_dir.name} is already up to date.")

    else:
        colored_echo("green", f"Cloning {repo_dir.name} repository...")
        run_command(f"git clone {repo_url} {repo_dir}", f"Failed to clone {repo_dir.name}", log_file_path)

        if len(branch_or_commit) == 40:  # If it's a commit hash
            colored_echo("green", f"Checking out commit {branch_or_commit}...")
            run_command(f"git checkout {branch_or_commit}",
                        f"Failed to checkout commit {branch_or_commit}", log_file_path, cwd=repo_dir)
        else:
            run_command(f"git checkout {branch_or_commit}",
                        f"Failed to switch to branch {branch_or_commit}", log_file_path, cwd=repo_dir)
            run_command(f"git pull origin {branch_or_commit}",
                        f"Failed to pull latest changes", log_file_path, cwd=repo_dir)

        latest_commit = run_command("git rev-parse HEAD", "Failed to get cloned commit hash", log_file_path,
                                    cwd=repo_dir)
        latest_commit_message = run_command("git log -1 --pretty=%B", "Failed to get cloned commit message",
                                            log_file_path, cwd=repo_dir)

        colored_echo("brown", f"Checked out commit: {latest_commit[:7]}")
        colored_echo("brown", f"Commit message: {latest_commit_message}")


def run_scripts(scripts_dir, log_dir):
    """Run scripts and store their output in log files."""
    scripts = [
        # "bench_compression_time_duckdb.py",
        # "bench_compression_time_parquet.py",
        # "compress_public_bi_duckdb.py",
        "compress_public_bi_duckdb_parquet.py",
        # "btrblocks_total.py",
        # "plot_compression_ratio.py",
        # "plot_decompression_time.py",
        # "plot_random_access.py",
        # "plot_accuracy_over_rowgroup.py",
        # "plot_rowgroup_decoding_per_ms.py",
        # "plot_sampling_benchmark.py",
        # "plot_sampling_benchmark_with_both_layouts.py",
        # "plot_expression_analyzed.py",
        # "plot_simd_benchmark.py",
        # "report_average_compression_speed.py",
        # "popularity.py",
    ]

    for script_name in scripts:
        script_path = scripts_dir / script_name
        log_file_path = log_dir / f"{script_name}.log"
        if not script_path.exists():
            colored_echo("red", f"Script {script_name} not found in {scripts_dir}.")
            sys.exit(1)
        colored_echo("green", f"Running script: {script_name}")
        run_command(f"python3 {script_path}", f"Failed to execute {script_name}", log_file_path, cwd=scripts_dir)


def main():
    workspace, repos, log_dir, scripts_dir, result_dir = setup_workspace()
    repo_log_file = log_dir / "repo_update.log"
    for repo_url, repo_name, branch_or_commit in repos:
        clone_or_update_repo(repo_url, workspace / repo_name, branch_or_commit, repo_log_file)

    # Uncomment if script execution is needed
    run_scripts(scripts_dir, log_dir)

    colored_echo("green", "Script execution complete. Logs are saved in the 'logs' directory.")


if __name__ == "__main__":
    main()
