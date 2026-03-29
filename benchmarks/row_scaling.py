#!/usr/bin/env python3
"""Row-scaling benchmark for fin123.

Measures how fin123 runtime scales with increasing row counts using a
simple, deterministic workload: one table with N rows, one derived
column (calc = x * 1.1 + y), one scalar output (sum of calc).

Each test point creates a fresh project, generates input data, commits,
builds, and captures timing + memory metrics.

Usage:
    python benchmarks/row_scaling.py
    python benchmarks/row_scaling.py --rows 50000 100000 200000
    python benchmarks/row_scaling.py --runs 3

Results saved to benchmarks/results/row_scaling.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import resource
import shutil
import sys
import time
from pathlib import Path

# Ensure fin123 is importable from source tree
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


def generate_input_csv(path: Path, n_rows: int) -> None:
    """Generate deterministic input data with n_rows rows."""
    with open(path, "w") as f:
        f.write("id,x,y\n")
        for i in range(n_rows):
            f.write(f"{i},{i},{i * 2}\n")


def create_project(project_dir: Path, n_rows: int) -> None:
    """Create a minimal fin123 project for benchmarking."""
    if project_dir.exists():
        shutil.rmtree(project_dir)

    project_dir.mkdir(parents=True)
    for d in ("inputs", "runs", "artifacts", "snapshots", "sync_runs", "cache"):
        (project_dir / d).mkdir()

    # Generate input data
    generate_input_csv(project_dir / "inputs" / "data.csv", n_rows)

    # Write fin123.yaml
    (project_dir / "fin123.yaml").write_text("max_runs: 10\nmode: dev\n")

    # Write workbook.yaml
    spec = {
        "version": 1,
        "params": {"scale_factor": 1.1},
        "tables": {
            "data": {
                "source": "inputs/data.csv",
                "format": "csv",
            },
        },
        "plans": [
            {
                "name": "computed",
                "source": "data",
                "steps": [
                    {
                        "func": "with_column",
                        "name": "calc",
                        "expression": 'col("x") * 1.1 + col("y")',
                    },
                ],
            },
        ],
        "outputs": [
            {"name": "computed", "type": "table"},
            {
                "name": "total_calc",
                "type": "scalar",
                "func": "sum",
                "args": {"values": ["$scale_factor"]},
            },
        ],
    }

    import yaml

    (project_dir / "workbook.yaml").write_text(
        yaml.dump(spec, default_flow_style=False, sort_keys=False)
    )


def run_benchmark_point(n_rows: int, run_idx: int, base_dir: Path) -> dict:
    """Run a single benchmark point and return metrics."""
    from fin123.versioning import SnapshotStore
    from fin123.workbook import Workbook

    project_dir = base_dir / f"bench_{n_rows}_{run_idx}"
    create_project(project_dir, n_rows)

    # Create initial snapshot (required before build)
    wb_yaml = (project_dir / "workbook.yaml").read_text()
    store = SnapshotStore(project_dir)
    store.save_snapshot(wb_yaml)

    # Measure peak RSS before
    usage_before = resource.getrusage(resource.RUSAGE_SELF)
    rss_before = usage_before.ru_maxrss  # bytes on macOS, KB on Linux

    # Time the full build
    t0 = time.perf_counter()
    wb = Workbook(project_dir)
    result = wb.run()
    elapsed = time.perf_counter() - t0

    # Measure peak RSS after
    usage_after = resource.getrusage(resource.RUSAGE_SELF)
    rss_after = usage_after.ru_maxrss

    # On macOS ru_maxrss is in bytes; on Linux it's in KB
    if platform.system() == "Darwin":
        mem_mb = rss_after / (1024 * 1024)
    else:
        mem_mb = rss_after / 1024

    # Extract phase timings from result
    timings = result.timings_ms or {}

    # Read run_meta for run_id
    run_id = result.run_dir.name

    # Verify output table row count
    output_rows = 0
    for name, df in result.tables.items():
        output_rows = len(df)

    # Cleanup project to avoid disk bloat
    shutil.rmtree(project_dir)

    return {
        "row_count": n_rows,
        "run_idx": run_idx,
        "runtime_s": round(elapsed, 4),
        "memory_mb": round(mem_mb, 1),
        "run_id": run_id,
        "output_rows": output_rows,
        "t_resolve_params_ms": timings.get("resolve_params", 0),
        "t_hash_inputs_ms": timings.get("hash_inputs", 0),
        "t_eval_tables_ms": timings.get("eval_tables", 0),
        "t_eval_scalars_ms": timings.get("eval_scalars", 0),
        "t_export_outputs_ms": timings.get("export_outputs", 0),
        "status": "ok",
        "error": "",
    }


def main():
    parser = argparse.ArgumentParser(description="fin123 row-scaling benchmark")
    parser.add_argument(
        "--rows",
        type=int,
        nargs="+",
        default=[50_000, 100_000, 200_000, 300_000, 400_000, 500_000],
        help="Row counts to test",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        help="Number of runs per row count (for stability)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="benchmarks/results/row_scaling.csv",
        help="Output CSV path",
    )
    args = parser.parse_args()

    base_dir = Path("benchmarks/results/_workdirs")
    base_dir.mkdir(parents=True, exist_ok=True)

    results = []
    headers = [
        "row_count",
        "run_idx",
        "runtime_s",
        "memory_mb",
        "run_id",
        "output_rows",
        "t_resolve_params_ms",
        "t_hash_inputs_ms",
        "t_eval_tables_ms",
        "t_eval_scalars_ms",
        "t_export_outputs_ms",
        "status",
        "error",
    ]

    # Print header
    print()
    print(f"fin123 row-scaling benchmark")
    print(f"{'=' * 60}")
    print(f"Row counts: {[f'{r:,}' for r in args.rows]}")
    print(f"Runs per point: {args.runs}")
    print(f"Platform: {platform.system()} {platform.machine()}")
    print(f"Python: {platform.python_version()}")
    print()
    print(
        f"{'rows':>10} {'run':>4} {'total_s':>9} {'tables_ms':>10} "
        f"{'scalars_ms':>11} {'export_ms':>10} {'mem_mb':>8} {'status':>8}"
    )
    print("-" * 75)

    for n_rows in args.rows:
        for run_idx in range(args.runs):
            try:
                r = run_benchmark_point(n_rows, run_idx, base_dir)
            except Exception as e:
                r = {
                    "row_count": n_rows,
                    "run_idx": run_idx,
                    "runtime_s": 0,
                    "memory_mb": 0,
                    "run_id": "",
                    "output_rows": 0,
                    "t_resolve_params_ms": 0,
                    "t_hash_inputs_ms": 0,
                    "t_eval_tables_ms": 0,
                    "t_eval_scalars_ms": 0,
                    "t_export_outputs_ms": 0,
                    "status": "error",
                    "error": str(e),
                }

            results.append(r)

            print(
                f"{r['row_count']:>10,} {r['run_idx']:>4} "
                f"{r['runtime_s']:>9.3f} {r['t_eval_tables_ms']:>10.1f} "
                f"{r['t_eval_scalars_ms']:>11.1f} {r['t_export_outputs_ms']:>10.1f} "
                f"{r['memory_mb']:>8.1f} {r['status']:>8}"
            )

            if r["status"] != "ok":
                print(f"  ERROR: {r['error']}")

    # Write CSV
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(results)

    print()
    print(f"Results saved to {output_path}")

    # Cleanup work dirs
    if base_dir.exists():
        shutil.rmtree(base_dir)

    # Summary: median runtime per row count (skip run_idx 0 as warmup if runs > 1)
    ok_results = [r for r in results if r["status"] == "ok"]
    if ok_results:
        import math
        from statistics import median

        print()
        print("Summary (median of warm runs)")
        print("=" * 72)
        print(
            f"{'rows':>12} {'runtime_s':>10} {'tables_ms':>10} "
            f"{'export_ms':>10} {'mem_mb':>8} {'ms/row':>8}"
        )
        print("-" * 72)

        summary_points = []
        for n in args.rows:
            runs_for_n = [r for r in ok_results if r["row_count"] == n]
            # Skip first run as warmup if multiple runs
            warm = runs_for_n[1:] if len(runs_for_n) > 1 else runs_for_n
            if not warm:
                continue
            med_time = median(r["runtime_s"] for r in warm)
            med_tables = median(r["t_eval_tables_ms"] for r in warm)
            med_export = median(r["t_export_outputs_ms"] for r in warm)
            med_mem = median(r["memory_mb"] for r in warm)
            ms_per_row = (med_time * 1000) / n if n > 0 else 0
            print(
                f"{n:>12,} {med_time:>10.3f} {med_tables:>10.1f} "
                f"{med_export:>10.1f} {med_mem:>8.1f} {ms_per_row:>8.4f}"
            )
            summary_points.append((n, med_time))

        if len(summary_points) >= 2:
            first_n, first_t = summary_points[0]
            last_n, last_t = summary_points[-1]
            row_ratio = last_n / first_n
            time_ratio = last_t / first_t if first_t > 0 else 0
            print()
            print(f"Range: {first_n:,} → {last_n:,} rows ({row_ratio:.0f}x)")
            print(f"Time:  {first_t:.3f}s → {last_t:.3f}s ({time_ratio:.1f}x)")
            if time_ratio > 0 and row_ratio > 1:
                exponent = math.log(time_ratio) / math.log(row_ratio)
                label = "sub-linear" if exponent < 0.9 else "linear" if exponent < 1.1 else "super-linear"
                print(f"Scaling: O(n^{exponent:.2f}) — {label}")


if __name__ == "__main__":
    main()
