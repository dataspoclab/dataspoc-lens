"""Export query results to CSV, JSON, or Parquet."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb


def export_csv(conn: duckdb.DuckDBPyConnection, sql: str, output_path: str) -> int:
    """Export query results to CSV. Returns row count."""
    result = conn.execute(sql)
    rows = result.fetchall()
    columns = [desc[0] for desc in result.description]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

    return len(rows)


def export_json(conn: duckdb.DuckDBPyConnection, sql: str, output_path: str) -> int:
    """Export query results to JSON (array of objects). Returns row count."""
    result = conn.execute(sql)
    rows = result.fetchall()
    columns = [desc[0] for desc in result.description]

    data = []
    for row in rows:
        obj = {}
        for col, val in zip(columns, row):
            # Convert non-serializable types to string
            try:
                json.dumps(val)
                obj[col] = val
            except (TypeError, ValueError):
                obj[col] = str(val)
        data.append(obj)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return len(rows)


def export_parquet(conn: duckdb.DuckDBPyConnection, sql: str, output_path: str) -> int:
    """Export query results to Parquet via DuckDB COPY TO. Returns row count."""
    # Count rows first
    count_result = conn.execute(f"SELECT COUNT(*) FROM ({sql}) _sub").fetchone()
    row_count = count_result[0] if count_result else 0

    conn.execute(f"COPY ({sql}) TO '{output_path}' (FORMAT PARQUET)")

    return row_count


def export_from_result(
    columns: list[str], rows: list[tuple], fmt: str, output_path: str
) -> int:
    """Export pre-fetched results (used by .export dot command)."""
    fmt = fmt.lower().strip()

    if fmt == "csv":
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)
        return len(rows)

    if fmt == "json":
        data = []
        for row in rows:
            obj = {}
            for col, val in zip(columns, row):
                try:
                    json.dumps(val)
                    obj[col] = val
                except (TypeError, ValueError):
                    obj[col] = str(val)
            data.append(obj)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return len(rows)

    if fmt == "parquet":
        conn = duckdb.connect()
        # Create a table from the data, then COPY TO parquet
        col_defs = ", ".join(f'"{c}" VARCHAR' for c in columns)
        conn.execute(f"CREATE TABLE _export_tmp ({col_defs})")
        for row in rows:
            placeholders = ", ".join("?" for _ in row)
            conn.execute(f"INSERT INTO _export_tmp VALUES ({placeholders})", list(row))
        conn.execute(f"COPY _export_tmp TO '{output_path}' (FORMAT PARQUET)")
        conn.close()
        return len(rows)

    raise ValueError(f"Unsupported format: {fmt}. Use csv, json, or parquet.")
