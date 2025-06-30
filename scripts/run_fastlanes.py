#!/usr/bin/env python3
"""
Build FastLanes in Release mode with benchmarking enabled, run the
`bench_compression_ratio` benchmark, **then copy its result files** from

    fastlanes/benchmark/result/compression_ratio/public_bi

to the project‑level directory

    result/compression_ratio/public_bi

Directory layout assumed (any absolute path is fine so long as it matches this
shape):

project_root/
├── run_bench_and_collect.py  ← this script
├── fastlanes/                ← the FastLanes checkout
│   └── benchmark/result/compression_ratio/public_bi/…
└── result/                   ← will be created if missing
    └── compression_ratio/public_bi/…  ← copied here

Extra options:
  • -j/--jobs N : parallel build jobs (defaults to detected CPU count)

Requires: CMake ≥ 3.20 and a C++ toolchain on PATH, and that
`bench_compression_ratio` is a valid CMake target.
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional


# ——————————————————————————————————————————————————————————— helpers ————

def sh(cmd: str, cwd: Path | None = None) -> None:
    """Run *cmd* (shell=True) and abort on non‑zero exit status."""
    print(f"$ {cmd}")
    subprocess.run(cmd, shell=True, cwd=cwd, check=True)


def find_exe(root: Path, name: str) -> Optional[Path]:
    """First executable file called *name* beneath *root*, else None."""
    for p in root.rglob(name):
        if p.is_file() and os.access(p, os.X_OK):
            return p
    return None


def locate_fastlanes(script_dir: Path) -> Path:
    """Return the fastlanes/ directory either beside this script or one up."""
    for candidate in (script_dir.parent / "fastlanes", script_dir / "fastlanes"):
        if candidate.is_dir():
            return candidate
    sys.exit("❌ FastLanes repo not found (expected 'fastlanes' sibling dir)")


# ————————————————————————————————————————————————————————————— main ————

def main() -> None:
    ap = argparse.ArgumentParser(description="Build, run benchmark, collect results")
    ap.add_argument("-j", "--jobs", type=int, default=os.cpu_count() or 2,
                    help="parallel build jobs (default: %(default)s)")
    args = ap.parse_args()

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent

    repo_dir = locate_fastlanes(script_dir)
    build_dir = repo_dir / "build"
    build_dir.mkdir(exist_ok=True)

    # 1. Configure CMake (Release build + benchmarking)
    sh(
        "cmake .. -DFLS_BUILD_BENCHMARKING=ON -DCMAKE_BUILD_TYPE=Release",
        cwd=build_dir,
    )

    # 2. Build the benchmark target
    sh(
        f"cmake --build . --config Release --target bench_compression_ratio -- -j{args.jobs}",
        cwd=build_dir,
    )

    # 3. Locate and run the benchmark binary
    bench = find_exe(build_dir, "bench_compression_ratio")
    if not bench:
        sys.exit(f"❌ bench_compression_ratio not found under {build_dir}")
    sh(str(bench), cwd=build_dir)

    # 4. Copy result files
    src_results = repo_dir / "benchmark" / "result" / "compression_ratio" / "public_bi"
    if not src_results.is_dir():
        sys.exit(f"❌ Expected results directory not found: {src_results}")

    dest_results = project_root / "result" / "compression_ratio" / "public_bi"
    dest_results.mkdir(parents=True, exist_ok=True)

    # Copy tree (overwrite existing files)
    for item in src_results.rglob("*"):
        rel = item.relative_to(src_results)
        target = dest_results / rel
        if item.is_dir():
            target.mkdir(parents=True, exist_ok=True)
        else:
            shutil.copy2(item, target)
    print(f"✅ Copied results to {dest_results}")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        sys.exit(f"❌ Command failed with exit code {e.returncode}")
