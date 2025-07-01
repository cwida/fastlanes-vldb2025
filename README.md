# **The FastLanes File Format · Artifact & Benchmark Suite**

Experiments for the VLDB 2025 paper *“The FastLanes File Format”*

---

## 1 Overview

This repository automates **everything needed to run benchmarks and replicate the numbers and figures reported in the
paper**. It does **not** contain the source or the data directly. Instead, during setup it clones and/or updates several
external repositories (on the branches shown) and runs their benchmarks:

* **FastLanes** (`https://github.com/cwida/FastLanes.git`): C++ implementation of the FastLanes File Format library.
* **FastLanes\_Data** (`https://github.com/azimafroozeh/FastLanes_Data.git`): Keeps data used for this paper.
* **BtrBlocks** (`https://github.com/cwida/btrblocks-vldb2025.git`): C++ implementation of the BtrBlocks file format +
  new benchmarks.
* **DuckDB FastLanes Plugin** (`https://github.com/cwida/duckdb-vldb2025.git`): C++ implementation of DuckDB + new
  benchmarks.

When you run `make` or `master_script.py`, the master script will clone (or update) each repo into these local folders
and invoke its benchmarks in sequence:

* **FastLanes** → `fastlanes`
* **FastLanes\_Data** → `data_repo`
* **BtrBlocks** → `btrblocks`
* **DuckDB FastLanes Plugin** → `duckdb`

---

## FastLanes CMake Benchmark Targets & Paper Mapping
| CMake Target                     | Paper Table / Figure      |
|----------------------------------|---------------------------|
| **bench_compression_ratio**      | Table 3                   |
| **benchmark_decompression_time** | Table 4 (decoding)        |
| **bench_compression_time**       | Table 4 (encoding)        |
| **benchmark_random_access**      | Table 5                   |
| **micro_benchmark_decompression**| Table 6                   |
| **bench_sample_size**            | Figure 2                  |


> **Note:** These CMake targets run *only* the FastLanes side of each experiment—comparative numbers (Parquet, BtrBlocks, DuckDB) are produced by their own benchmark suites in the other repos.

Below are the **CMake target names** (as defined in the `fastlanes` repo) for each FastLanes benchmark executable. Running `<target>` executes its FastLanes-only measurement; the description shows which Table or Figure in the VLDB ’25 paper the results feed into:

- **bench_compression_ratio**  
  Runs the FastLanes compression‐ratio experiment → produces data for **Table 3**: Compression ratios of file formats relative to FastLanes on the Public BI and TPC-H datasets.

- **benchmark_decompression_time**  
  Runs the FastLanes end‐to‐end decompression throughput experiment → produces data for **Table 4 (decoding)**: Row-groups decoded per second for FastLanes vs. Parquet, BtrBlocks, and DuckDB.

- **bench_compression_time**  
  Runs the FastLanes compression‐time experiment → produces data for **Table 4 (encoding)**: CPU time (ms/row-group) to compress into FastLanes, compared to other formats.

- **benchmark_random_access**  
  Runs the FastLanes random‐access latency experiment → produces data for **Table 5**: Latency (ms per value) of random value retrieval across file formats.

- **micro_benchmark_decompression**  
  Runs the FastLanes SIMD‐accelerated decompression micro‐benchmark → produces data for **Table 6**: Total decoding time under SSE, AVX2, and AVX-512 flags.

- **bench_sample_size**  
  Runs the FastLanes sample‐size sensitivity experiment → produces data for **Figure 2**: Accuracy of compression-ratio estimates vs. sample-index size (three-way sampling achieves > 99 % accuracy).

---

## 2 Quick Start

Choose between the one-step `make` workflow or the manual setup below.

### Option A: Using `make`

```bash
# 1. Clone this repo
git clone https://github.com/azimafroozeh/fastlanes-vldb2025.git
cd fastlanes-vldb2025

# 2. Build, install, and run everything in one command
make
```

By default, `make` will:

* Create a Python virtual environment in `.venv`
* Upgrade `pip` and install `pandas` and `duckdb~=1.2.0`
* Invoke `python master_script.py`

To wipe and rebuild:

```bash
make clean
make
```

### Option B: Manual Setup

```bash
# 1. Clone this repo
git clone https://github.com/azimafroozeh/fastlanes-vldb2025.git
cd fastlanes-vldb2025

# 2. Create and activate a fresh venv
python3 -m venv .venv
source .venv/bin/activate

# 3. Install runtime dependencies
pip install --upgrade pip
pip install pandas duckdb~=1.2.0 numpy

# 4. Run the master script
python master_script.py
```

*Estimated time*: \~30 min on an 8-core laptop (first run dominated by dataset downloads). All console messages are
colourised; detailed logs go to `logs/`.

---

## 3 Repository Layout

| Path               | Purpose                                                           |
|--------------------|-------------------------------------------------------------------|
| `master_script.py` | Top-level orchestrator (clone/update repos, run selected scripts) |
| `scripts/`         | Individual benchmark & helper scripts                             |
| `logs/`            | Runtime logs (one file per script plus `repo_update.log`)         |
| `result/`          | Outputs generated by the master script                            |
| `paper_result/`    | Authoritative CSVs/figures matching the paper (§8)                |
| `.github/`         | CI workflows for GitHub Actions                                   |

---

## 4 Dependencies

| Package / Tool       | Tested Version(s) | Why Needed                          |
|----------------------|-------------------|-------------------------------------|
| Python               | 3.9 – 3.12        | Runtime for all scripts             |
| `git`                | ≥ 2.30            | Clone/checkout sub-repos            |
| `pandas`             | 2.x               | Data wrangling & CSV output         |
| `duckdb`             | 1.2.x **(exact)** | Parquet I/O & SQL in the benchmarks |
| `numpy`              | 1.25+             | Helper calculations                 |
| **C/C++ tool-chain** | gcc / clang       | Building FastLanes & BtrBlocks      |

Install Python deps with:

```bash
pip install --upgrade duckdb==1.2.* pandas numpy
```

---

## 5 Running Individual Experiments

1. **Edit** `master_script.py` → `run_scripts()`
   *Uncomment* any script you wish to execute (e.g. plots, compression speed, SIMD micro-benchmarks).
2. **Re-run**:

   ```bash
   python master_script.py
   ```

Each script writes one log (`logs/<script>.log`) and stores artefacts under `result/<script>/`.

---

## 6 Citation

If you use this code or the dataset preparations in academic work, please cite:

```bibtex
@article{afroozeh2025fastlanes,
    author = {Azim Afroozeh and Peter A. Boncz},
    title = {The FastLanes File Format},
    journal = {Proc.\ VLDB Endow.},
    volume = {18},
    number = {9},
    year = {2025}
}
```

---

## 7 License

This repository is released under the **MIT License**. Individual sub-repositories carry their own licenses—please
consult each project.

---

*Questions? Open an issue or join us on Discord*
[![Join Our Discord](https://img.shields.io/discord/1282716959099588651?label=Join%20Our%20Discord\&logo=discord\&color=7289da)](https://discord.gg/SpTHkCQ7uh)
