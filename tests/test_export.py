"""Tests for export — CSV, JSON, Parquet."""

import csv
import json
from pathlib import Path

import duckdb
import pytest

from dataspoc_lens.export import export_csv, export_from_result, export_json, export_parquet


@pytest.fixture
def conn_with_data():
    """DuckDB connection with sample data."""
    conn = duckdb.connect()
    conn.execute(
        "CREATE TABLE sample AS SELECT * FROM "
        "(VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.3), (3, 'Carol', 30.1)) "
        "AS t(id, name, score)"
    )
    yield conn
    conn.close()


SQL = "SELECT * FROM sample"


def test_export_csv(conn_with_data, tmp_path):
    """export_csv produces a valid CSV file."""
    out = str(tmp_path / "output.csv")
    count = export_csv(conn_with_data, SQL, out)
    assert count == 3

    with open(out, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    assert len(rows) == 3
    assert rows[0]["name"] == "Alice"


def test_export_json(conn_with_data, tmp_path):
    """export_json produces a valid JSON file."""
    out = str(tmp_path / "output.json")
    count = export_json(conn_with_data, SQL, out)
    assert count == 3

    with open(out, encoding="utf-8") as f:
        data = json.load(f)
    assert isinstance(data, list)
    assert len(data) == 3
    assert data[1]["name"] == "Bob"


def test_export_parquet(conn_with_data, tmp_path):
    """export_parquet produces a valid Parquet file."""
    out = str(tmp_path / "output.parquet")
    count = export_parquet(conn_with_data, SQL, out)
    assert count == 3

    # Verify by reading back with DuckDB
    verify_conn = duckdb.connect()
    result = verify_conn.execute(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()
    assert result[0] == 3
    verify_conn.close()


def test_export_from_result_csv(tmp_path):
    """export_from_result works for CSV."""
    columns = ["a", "b"]
    rows = [(1, "x"), (2, "y")]
    out = str(tmp_path / "result.csv")
    count = export_from_result(columns, rows, "csv", out)
    assert count == 2
    assert Path(out).exists()


def test_export_from_result_json(tmp_path):
    """export_from_result works for JSON."""
    columns = ["a", "b"]
    rows = [(1, "x"), (2, "y")]
    out = str(tmp_path / "result.json")
    count = export_from_result(columns, rows, "json", out)
    assert count == 2

    with open(out) as f:
        data = json.load(f)
    assert len(data) == 2


def test_export_from_result_parquet(tmp_path):
    """export_from_result works for Parquet."""
    columns = ["a", "b"]
    rows = [(1, "x"), (2, "y")]
    out = str(tmp_path / "result.parquet")
    count = export_from_result(columns, rows, "parquet", out)
    assert count == 2
    assert Path(out).exists()


def test_export_unsupported_format(tmp_path):
    """export_from_result raises for unsupported format."""
    with pytest.raises(ValueError, match="Unsupported format"):
        export_from_result(["a"], [(1,)], "xlsx", str(tmp_path / "out.xlsx"))
