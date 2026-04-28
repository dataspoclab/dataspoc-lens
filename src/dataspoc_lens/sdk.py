"""LensClient — Python SDK for programmatic access to DataSpoc Lens.

Usage:
    from dataspoc_lens import LensClient

    with LensClient() as client:
        print(client.tables())
        result = client.query("SELECT * FROM orders LIMIT 10")
        print(result["rows"])
"""

from __future__ import annotations

from typing import Any


class LensClient:
    """High-level SDK client for DataSpoc Lens.

    Connects to registered buckets, mounts DuckDB views, and exposes
    query, AI, and cache operations as simple method calls.

    All public methods return JSON-serializable data (dicts, lists,
    strings, numbers).
    """

    def __init__(self) -> None:
        from dataspoc_lens.connect import connect

        self._conn, self._table_names = connect()

    # -- Context manager protocol ------------------------------------------

    def __enter__(self) -> LensClient:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    # -- Public API --------------------------------------------------------

    def tables(self) -> list[str]:
        """Return the list of mounted table names."""
        return list(self._table_names)

    def schema(self, table: str) -> list[dict]:
        """Return column metadata for *table*.

        Each element is ``{"column_name": ..., "data_type": ...}``.
        """
        from dataspoc_lens.catalog import get_table_columns

        return get_table_columns(self._conn, table)

    def query(self, sql: str) -> dict:
        """Execute *sql* and return results.

        Returns::

            {
                "columns": ["col1", "col2", ...],
                "rows": [[val1, val2, ...], ...],
                "row_count": int,
                "duration": float,  # seconds
            }
        """
        from dataspoc_lens.shell import run_query

        columns, rows, duration = run_query(self._conn, sql)
        return {
            "columns": columns,
            "rows": [list(row) for row in rows],
            "row_count": len(rows),
            "duration": duration,
        }

    def ask(self, question: str, **kwargs: Any) -> dict:
        """Translate a natural-language *question* to SQL, execute it, and
        return the result.

        Keyword arguments are forwarded to :func:`dataspoc_lens.ai.ask`
        (e.g. ``provider``, ``api_key``, ``model``, ``debug``).

        Returns::

            {
                "sql": str,
                "columns": [...],
                "rows": [[...], ...],
                "duration": float,
                "error": str | None,
            }
        """
        from dataspoc_lens.ai import ask as ai_ask

        result = ai_ask(self._conn, question, **kwargs)
        return {
            "sql": result.get("sql", ""),
            "columns": result.get("columns", []),
            "rows": [list(row) for row in result.get("rows", [])],
            "duration": result.get("duration", 0.0),
            "error": result.get("error"),
        }

    # -- Cache operations --------------------------------------------------

    def cache_status(self) -> list[dict]:
        """Return metadata for all cached tables.

        Each element is::

            {
                "table": str,
                "status": "fresh" | "stale",
                "cached_at": str,
                "size_bytes": int,
            }
        """
        from dataspoc_lens.cache import list_cached_tables

        cached = list_cached_tables()
        return [
            {
                "table": c["table"],
                "status": c["status"],
                "cached_at": c["cached_at"],
                "size_bytes": c["size_bytes"],
            }
            for c in cached
        ]

    def cache_refresh(self, table: str) -> dict:
        """Force-refresh the local cache for *table*.

        Returns ``{"cached_at": ..., "size_bytes": ..., "file_count": ...}``.

        Raises ``ValueError`` if *table* is not found in any registered bucket.
        """
        from dataspoc_lens.cache import cache_table
        from dataspoc_lens.catalog import discover_tables
        from dataspoc_lens.config import load_config

        config = load_config()
        source_uri = self._find_table_uri(table, config.buckets)
        if source_uri is None:
            raise ValueError(
                f"Table '{table}' not found in registered buckets."
            )

        info = cache_table(table, source_uri, force=True)
        return {
            "cached_at": info["cached_at"],
            "size_bytes": info["size_bytes"],
            "file_count": info["file_count"],
        }

    def cache_refresh_stale(self) -> list[dict]:
        """Refresh all stale cached tables.

        Returns a list of dicts (same shape as :meth:`cache_refresh`) for
        each table that was refreshed.
        """
        from dataspoc_lens.cache import list_cached_tables

        refreshed: list[dict] = []
        for entry in list_cached_tables():
            if entry["status"] == "stale":
                info = self.cache_refresh(entry["table"])
                refreshed.append({"table": entry["table"], **info})
        return refreshed

    def cache_clear(self, table: str | None = None) -> list[str]:
        """Clear cached data.

        If *table* is given, only that table's cache is cleared.  Otherwise
        all cached tables are removed.

        Returns the list of cleared table names.
        """
        from dataspoc_lens.cache import clear_cache

        return clear_cache(table)

    # -- Lifecycle ---------------------------------------------------------

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None  # type: ignore[assignment]

    # -- Internal helpers --------------------------------------------------

    def _find_table_uri(self, table: str, buckets: list[str]) -> str | None:
        """Look up the remote URI for *table* across all registered buckets."""
        from dataspoc_lens.catalog import discover_tables

        for bucket in buckets:
            try:
                for t in discover_tables(bucket):
                    if t.table == table:
                        return t.location
            except Exception:
                continue
        return None
