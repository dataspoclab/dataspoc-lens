"""Quick connection helper — one-liner to get DuckDB with tables mounted.

Usage in any notebook (Jupyter, Marimo, script):
    from dataspoc_lens.connect import connect
    conn, tables = connect()
    conn.sql("SELECT * FROM orders LIMIT 10").show()
"""

from __future__ import annotations


def connect() -> tuple:
    """Create DuckDB connection with all registered bucket tables mounted.

    Returns:
        (conn, table_names) — DuckDB connection and list of table name strings
    """
    from dataspoc_lens.config import load_config
    from dataspoc_lens.catalog import discover_tables, mount_views
    from dataspoc_lens.shell import get_connection

    config = load_config()
    conn = get_connection()

    all_tables = []
    for bucket in config.buckets:
        try:
            tables = discover_tables(bucket)
            all_tables.extend(tables)
        except Exception:
            pass

    mount_views(conn, all_tables)
    table_names = [t.table for t in all_tables]

    return conn, table_names
