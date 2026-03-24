"""Tests for catalog — discovery, manifest, view mounting."""

import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dataspoc_lens.catalog import (
    TableInfo,
    discover_tables,
    get_catalog_tables,
    mount_views,
)


def _create_parquet(path: Path, data: dict) -> None:
    """Helper: write a Parquet file from a dict of columns."""
    table = pa.table(data)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(path))


@pytest.fixture
def sample_bucket(tmp_path):
    """Create a local bucket with Parquet files."""
    # Table: users
    users_dir = tmp_path / "raw" / "users"
    _create_parquet(
        users_dir / "part-0001.parquet",
        {"id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"]},
    )

    # Table: orders
    orders_dir = tmp_path / "raw" / "orders"
    _create_parquet(
        orders_dir / "part-0001.parquet",
        {"order_id": [10, 20], "user_id": [1, 2], "total": [99.9, 149.5]},
    )

    return tmp_path


@pytest.fixture
def manifest_bucket(tmp_path):
    """Create a local bucket with a manifest."""
    # Create parquet data
    data_dir = tmp_path / "data" / "customers"
    _create_parquet(
        data_dir / "file.parquet",
        {"customer_id": [1, 2], "email": ["a@b.com", "c@d.com"]},
    )

    # Create manifest
    manifest_dir = tmp_path / ".dataspoc"
    manifest_dir.mkdir(parents=True)
    manifest = {
        "tables": [
            {
                "source": "pipe",
                "table": "customers",
                "location": "data/customers",
                "columns": ["customer_id", "email"],
                "row_count": 2,
            }
        ]
    }
    (manifest_dir / "manifest.json").write_text(json.dumps(manifest))

    return tmp_path


def test_discover_from_scan(sample_bucket):
    """Scan-based discovery finds Parquet tables."""
    uri = f"file://{sample_bucket}"
    tables = discover_tables(uri)
    assert len(tables) >= 1
    names = [t.table for t in tables]
    # Should find tables derived from directory names
    assert any("users" in n for n in names) or any("orders" in n for n in names)


def test_discover_from_manifest(manifest_bucket):
    """Manifest-first discovery reads manifest.json."""
    uri = f"file://{manifest_bucket}"
    tables = discover_tables(uri)
    assert len(tables) == 1
    assert tables[0].table == "customers"
    assert tables[0].source == "pipe"
    assert "customer_id" in tables[0].columns


def test_mount_views(sample_bucket):
    """Mount views creates queryable DuckDB views."""
    conn = duckdb.connect()

    # Create a simple table info pointing at the parquet files
    users_dir = sample_bucket / "raw" / "users"
    tables = [
        TableInfo(
            source="test",
            table="test_users",
            location=str(users_dir),
            columns=["id", "name"],
            row_count=3,
        )
    ]

    mount_views(conn, tables)

    # Verify the view exists and is queryable
    catalog = get_catalog_tables(conn)
    view_names = [t["table_name"] for t in catalog]
    assert "test_users" in view_names

    result = conn.execute("SELECT COUNT(*) FROM test_users").fetchone()
    assert result[0] == 3

    conn.close()


def test_mount_views_with_single_parquet(tmp_path):
    """Mount views handles single parquet file locations."""
    pq_file = tmp_path / "data.parquet"
    _create_parquet(pq_file, {"x": [1, 2, 3], "y": [4, 5, 6]})

    conn = duckdb.connect()
    tables = [
        TableInfo(
            source="test",
            table="single_table",
            location=str(pq_file),
            columns=["x", "y"],
            row_count=3,
        )
    ]

    mount_views(conn, tables)

    result = conn.execute("SELECT COUNT(*) FROM single_table").fetchone()
    assert result[0] == 3

    conn.close()


def test_get_catalog_tables():
    """get_catalog_tables returns table info from information_schema."""
    conn = duckdb.connect()
    conn.execute("CREATE VIEW my_view AS SELECT 1 AS col")

    tables = get_catalog_tables(conn)
    names = [t["table_name"] for t in tables]
    assert "my_view" in names

    conn.close()
