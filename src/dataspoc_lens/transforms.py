"""Transforms — numbered .sql files, sequential execution."""

from __future__ import annotations

import re
import time
from pathlib import Path

import duckdb

from dataspoc_lens.config import TRANSFORMS_DIR


def discover_transforms(transforms_dir: Path | None = None) -> list[Path]:
    """Discover .sql files in transforms directory, sorted by numeric prefix."""
    d = transforms_dir or TRANSFORMS_DIR
    if not d.exists():
        return []

    sql_files = list(d.glob("*.sql"))

    def sort_key(p: Path) -> tuple[int, str]:
        match = re.match(r"^(\d+)", p.name)
        num = int(match.group(1)) if match else 999999
        return (num, p.name)

    return sorted(sql_files, key=sort_key)


def run_transform(
    conn: duckdb.DuckDBPyConnection, sql_path: Path
) -> tuple[float, str]:
    """Execute a SQL file against DuckDB connection.

    Returns (duration_seconds, status) where status is 'OK' or error message.
    """
    sql = sql_path.read_text(encoding="utf-8").strip()
    if not sql:
        return (0.0, "OK (empty)")

    start = time.time()
    try:
        conn.execute(sql)
        duration = time.time() - start
        return (duration, "OK")
    except Exception as e:
        duration = time.time() - start
        return (duration, f"ERROR: {e}")


def run_all_transforms(
    conn: duckdb.DuckDBPyConnection, transforms_dir: Path | None = None
) -> list[tuple[str, float, str]]:
    """Discover and run all transforms sequentially. Stops on error.

    Returns list of (filename, duration, status).
    """
    sql_files = discover_transforms(transforms_dir)
    results: list[tuple[str, float, str]] = []

    for sql_path in sql_files:
        duration, status = run_transform(conn, sql_path)
        results.append((sql_path.name, duration, status))
        if status.startswith("ERROR"):
            break

    return results
