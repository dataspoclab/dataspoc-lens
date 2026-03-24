"""Tests for the SQL shell — query execution, formatting, dot commands."""

import duckdb

from dataspoc_lens.shell import format_results, handle_dot_command, run_query


def test_run_query_simple():
    """run_query executes SQL and returns columns, rows, duration."""
    conn = duckdb.connect()
    columns, rows, duration = run_query(conn, "SELECT 1 AS x, 'hello' AS y")
    assert columns == ["x", "y"]
    assert len(rows) == 1
    assert rows[0] == (1, "hello")
    assert duration >= 0
    conn.close()


def test_run_query_multiple_rows():
    """run_query handles multiple rows."""
    conn = duckdb.connect()
    columns, rows, duration = run_query(
        conn, "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, letter)"
    )
    assert columns == ["id", "letter"]
    assert len(rows) == 3
    conn.close()


def test_format_results_basic():
    """format_results produces a table string."""
    columns = ["id", "name"]
    rows = [(1, "Alice"), (2, "Bob")]
    output = format_results(columns, rows)
    assert "id" in output
    assert "name" in output
    assert "Alice" in output
    assert "Bob" in output


def test_format_results_empty():
    """format_results handles empty columns."""
    output = format_results([], [])
    assert "no results" in output.lower()


def test_format_results_truncation():
    """format_results truncates long values."""
    columns = ["data"]
    rows = [("x" * 100,)]
    output = format_results(columns, rows, max_col_width=20)
    assert "..." in output


def test_dot_tables():
    """dot command .tables lists tables."""
    conn = duckdb.connect()
    conn.execute("CREATE VIEW test_view AS SELECT 1 AS col")
    output = handle_dot_command(".tables", conn, [])
    assert "test_view" in output
    conn.close()


def test_dot_schema():
    """dot command .schema shows table schema."""
    conn = duckdb.connect()
    conn.execute("CREATE VIEW users AS SELECT 1 AS id, 'test' AS name")
    output = handle_dot_command(".schema users", conn, [])
    assert "id" in output
    assert "name" in output
    conn.close()


def test_dot_schema_no_arg():
    """dot command .schema without arg shows usage."""
    conn = duckdb.connect()
    output = handle_dot_command(".schema", conn, [])
    assert "Usage" in output
    conn.close()


def test_dot_buckets():
    """dot command .buckets lists buckets."""
    conn = duckdb.connect()
    output = handle_dot_command(".buckets", conn, ["s3://my-bucket", "gs://other"])
    assert "s3://my-bucket" in output
    assert "gs://other" in output
    conn.close()


def test_dot_buckets_empty():
    """dot command .buckets with no buckets."""
    conn = duckdb.connect()
    output = handle_dot_command(".buckets", conn, [])
    assert "No buckets" in output
    conn.close()


def test_dot_help():
    """dot command .help shows available commands."""
    conn = duckdb.connect()
    output = handle_dot_command(".help", conn, [])
    assert ".tables" in output
    assert ".schema" in output
    assert ".quit" in output
    conn.close()


def test_dot_quit():
    """dot command .quit returns None to signal exit."""
    conn = duckdb.connect()
    output = handle_dot_command(".quit", conn, [])
    assert output is None
    conn.close()


def test_dot_exit():
    """dot command .exit returns None to signal exit."""
    conn = duckdb.connect()
    output = handle_dot_command(".exit", conn, [])
    assert output is None
    conn.close()


def test_dot_unknown():
    """Unknown dot command shows error."""
    conn = duckdb.connect()
    output = handle_dot_command(".foobar", conn, [])
    assert "Unknown" in output
    conn.close()
