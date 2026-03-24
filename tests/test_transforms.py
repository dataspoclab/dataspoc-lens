"""Tests for transforms — discovery, execution, ordering, error handling."""

from pathlib import Path

import duckdb
import pytest

from dataspoc_lens.transforms import discover_transforms, run_all_transforms, run_transform


@pytest.fixture
def transforms_dir(tmp_path):
    """Create a transforms directory with numbered SQL files."""
    d = tmp_path / "transforms"
    d.mkdir()

    (d / "001_create_table.sql").write_text(
        "CREATE TABLE clean_users AS SELECT 1 AS id, 'Alice' AS name"
    )
    (d / "002_add_data.sql").write_text(
        "INSERT INTO clean_users VALUES (2, 'Bob')"
    )
    (d / "003_summary.sql").write_text(
        "CREATE TABLE user_count AS SELECT COUNT(*) AS total FROM clean_users"
    )

    return d


@pytest.fixture
def transforms_dir_with_error(tmp_path):
    """Create transforms where the second one has bad SQL."""
    d = tmp_path / "transforms"
    d.mkdir()

    (d / "001_ok.sql").write_text("SELECT 1")
    (d / "002_bad.sql").write_text("SELECT * FROM nonexistent_table_xyz")
    (d / "003_never.sql").write_text("SELECT 2")

    return d


def test_discover_transforms_ordering(transforms_dir):
    """Transforms are discovered and sorted by numeric prefix."""
    files = discover_transforms(transforms_dir)
    assert len(files) == 3
    assert files[0].name == "001_create_table.sql"
    assert files[1].name == "002_add_data.sql"
    assert files[2].name == "003_summary.sql"


def test_discover_transforms_empty(tmp_path):
    """Empty directory returns empty list."""
    d = tmp_path / "empty"
    d.mkdir()
    assert discover_transforms(d) == []


def test_discover_transforms_nonexistent(tmp_path):
    """Nonexistent directory returns empty list."""
    assert discover_transforms(tmp_path / "nope") == []


def test_run_transform_success(transforms_dir):
    """run_transform executes SQL and returns OK."""
    conn = duckdb.connect()
    sql_path = transforms_dir / "001_create_table.sql"
    duration, status = run_transform(conn, sql_path)
    assert status == "OK"
    assert duration >= 0

    result = conn.execute("SELECT COUNT(*) FROM clean_users").fetchone()
    assert result[0] == 1
    conn.close()


def test_run_transform_error(tmp_path):
    """run_transform returns error status for bad SQL."""
    d = tmp_path / "t"
    d.mkdir()
    bad_sql = d / "bad.sql"
    bad_sql.write_text("SELECT * FROM this_table_does_not_exist")

    conn = duckdb.connect()
    duration, status = run_transform(conn, bad_sql)
    assert status.startswith("ERRO")
    assert duration >= 0
    conn.close()


def test_run_all_transforms_success(transforms_dir):
    """run_all_transforms executes all files in order."""
    conn = duckdb.connect()
    results = run_all_transforms(conn, transforms_dir)
    assert len(results) == 3
    assert all(s == "OK" for _, _, s in results)

    # Verify final state
    result = conn.execute("SELECT total FROM user_count").fetchone()
    assert result[0] == 2  # Alice + Bob
    conn.close()


def test_run_all_transforms_stops_on_error(transforms_dir_with_error):
    """run_all_transforms stops at first error, does not run subsequent files."""
    conn = duckdb.connect()
    results = run_all_transforms(conn, transforms_dir_with_error)

    assert len(results) == 2  # 001 OK, 002 ERRO — 003 never runs
    assert results[0][2] == "OK"
    assert results[1][2].startswith("ERRO")
    conn.close()


def test_run_transform_empty_file(tmp_path):
    """Empty SQL file returns OK."""
    d = tmp_path / "t"
    d.mkdir()
    empty = d / "001_empty.sql"
    empty.write_text("")

    conn = duckdb.connect()
    duration, status = run_transform(conn, empty)
    assert "OK" in status
    conn.close()
