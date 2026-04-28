"""MCP Server for DataSpoc Lens.

Exposes Lens capabilities (query, catalog, cache, AI) as MCP tools
so that AI agents can interact with the data lake programmatically.

Start with:  dataspoc-lens mcp
"""

from __future__ import annotations

import json
import re

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("dataspoc-lens")

_client = None

_WRITE_PATTERN = re.compile(
    r"^\s*(DROP|DELETE|INSERT|UPDATE|ALTER|TRUNCATE|CREATE|REPLACE)\b",
    re.IGNORECASE,
)


def _get_client():
    """Return a lazily-initialized LensClient (connect once, reuse)."""
    global _client
    if _client is None:
        from dataspoc_lens.sdk import LensClient

        _client = LensClient()
    return _client


# ── Tools ─────────────────────────────────────────────────────────────


@mcp.tool()
def list_tables() -> str:
    """List all available tables in the data lake.

    Returns one table name per line. Use describe_table() to see columns.
    """
    client = _get_client()
    tables = client.tables()
    return "\n".join(tables) if tables else "(no tables found)"


@mcp.tool()
def describe_table(table: str) -> str:
    """Describe the schema of a table.

    Returns JSON array of objects with 'column_name' and 'data_type' keys.

    Args:
        table: Name of the table to describe.
    """
    client = _get_client()
    columns = client.schema(table)
    return json.dumps(columns, default=str)


@mcp.tool()
def query(sql: str) -> str:
    """Execute a read-only SQL query against the data lake.

    Returns JSON with 'columns', 'rows', 'row_count', and 'duration'.
    Write operations (DROP, DELETE, INSERT, UPDATE, ALTER, etc.) are rejected.

    Args:
        sql: SQL query to execute. Must be a read-only statement (SELECT, SHOW, etc.).
    """
    if _WRITE_PATTERN.match(sql):
        return json.dumps(
            {"error": "Write operations are not allowed. Only SELECT queries are permitted."}
        )
    client = _get_client()
    result = client.query(sql)
    return json.dumps(result, default=str)


@mcp.tool()
def ask(question: str) -> str:
    """Translate a natural-language question to SQL and execute it.

    The question is sent to the configured LLM, which generates SQL based on
    the available table schemas. The SQL is then executed and results returned.

    Returns JSON with 'sql', 'columns', 'rows', 'duration', and 'error'.

    Args:
        question: Natural language question about the data (e.g. "What are the top 10 customers by revenue?").
    """
    client = _get_client()
    result = client.ask(question)
    return json.dumps(result, default=str)


@mcp.tool()
def cache_status() -> str:
    """Show the status of all locally cached tables.

    Returns JSON array of objects with 'table', 'status' (fresh/stale),
    'cached_at', and 'size_bytes'.
    """
    client = _get_client()
    status = client.cache_status()
    return json.dumps(status, default=str)


@mcp.tool()
def cache_refresh(table: str) -> str:
    """Force-refresh the local cache for a specific table.

    Downloads the latest Parquet files from the remote bucket to local cache.
    Returns JSON with 'cached_at', 'size_bytes', and 'file_count'.

    Args:
        table: Name of the table to refresh.
    """
    client = _get_client()
    try:
        result = client.cache_refresh(table)
        return json.dumps(result, default=str)
    except ValueError as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def cache_refresh_stale() -> str:
    """Refresh all stale cached tables.

    Finds tables whose cache is older than the latest extraction and
    re-downloads them. Returns JSON array of refreshed tables.
    """
    client = _get_client()
    refreshed = client.cache_refresh_stale()
    return json.dumps(refreshed, default=str)


# ── Resources ─────────────────────────────────────────────────────────


@mcp.resource("lens://tables")
def tables_catalog() -> str:
    """Full catalog of all tables with their column schemas.

    Returns JSON array of objects with 'table' and 'columns' keys,
    where 'columns' is an array of {column_name, data_type}.
    """
    client = _get_client()
    catalog = []
    for table_name in client.tables():
        columns = client.schema(table_name)
        catalog.append({"table": table_name, "columns": columns})
    return json.dumps(catalog, default=str)


# ── Entry point ───────────────────────────────────────────────────────


def run_server() -> None:
    """Start the MCP server (stdio transport)."""
    mcp.run()
