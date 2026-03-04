#!/usr/bin/env python3
"""Compact all parquet files into one per table under output-data/."""

import sys
from pathlib import Path

import pyarrow.parquet as pq


def compact_table(table_dir: Path) -> None:
    parquet_files = sorted(table_dir.glob("*.parquet"))
    if not parquet_files:
        print(f"  Skipping {table_dir.name}: no parquet files found")
        return

    print(f"  {table_dir.name}: reading {len(parquet_files)} files ...")
    table = pq.read_table(table_dir, schema=pq.read_schema(parquet_files[0]))

    output_path = table_dir / "compacted.parquet"
    pq.write_table(table, output_path)
    print(f"  {table_dir.name}: wrote {output_path} ({table.num_rows} rows)")

    for f in parquet_files:
        if f.name != "compacted.parquet":
            f.unlink()
    print(f"  {table_dir.name}: removed {len(parquet_files)} original files")


def main() -> None:
    base = Path(__file__).parent / "output-data"
    if not base.is_dir():
        print(f"Error: {base} does not exist", file=sys.stderr)
        sys.exit(1)

    table_dirs = sorted(p for p in base.iterdir() if p.is_dir())
    if not table_dirs:
        print("No table directories found.")
        return

    print(f"Found {len(table_dirs)} tables: {[d.name for d in table_dirs]}")
    for table_dir in table_dirs:
        compact_table(table_dir)

    print("Done.")


if __name__ == "__main__":
    main()
