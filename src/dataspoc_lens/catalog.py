"""Catalog — bucket discovery, manifest parsing, DuckDB view mounting."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

import duckdb
import fsspec


@dataclass
class TableInfo:
    """Metadata for a discovered table."""

    source: str
    table: str
    location: str
    columns: list[str] = field(default_factory=list)
    row_count: int = 0


def _get_fs(uri: str) -> tuple[fsspec.AbstractFileSystem, str]:
    """Resolve filesystem and path from URI."""
    if uri.startswith("file://"):
        path = uri[7:]
        fs = fsspec.filesystem("file")
        return fs, path

    fs, _, paths = fsspec.get_fs_token_paths(uri)
    path = paths[0] if paths else ""
    return fs, path


def _discover_from_manifest(bucket_uri: str) -> list[TableInfo] | None:
    """Try manifest-first discovery: read <bucket>/.dataspoc/manifest.json."""
    manifest_uri = f"{bucket_uri.rstrip('/')}/.dataspoc/manifest.json"
    fs, path = _get_fs(manifest_uri)

    if not fs.exists(path):
        return None

    with fs.open(path, "rb") as f:
        manifest = json.load(f)

    tables: list[TableInfo] = []
    raw_tables = manifest.get("tables", {})

    # Support both dict format (from Pipe: {"source/table": {...}}) and list format
    if isinstance(raw_tables, dict):
        items = raw_tables.values()
    else:
        items = raw_tables

    for tbl in items:
        # Build location from bucket + source + table if not provided
        location = tbl.get("location", "")
        if not location:
            src = tbl.get("source", "")
            tbl_name = tbl.get("table", "")
            if src and tbl_name:
                location = f"{bucket_uri.rstrip('/')}/raw/{src}/{tbl_name}"
        if not location.startswith(("s3://", "gs://", "az://", "file://", "/")):
            location = f"{bucket_uri.rstrip('/')}/{location}"

        # Extract row count from stats (Pipe format) or direct field
        row_count = tbl.get("row_count", 0)
        if not row_count and "stats" in tbl:
            row_count = tbl["stats"].get("total_rows", 0)

        tables.append(
            TableInfo(
                source=tbl.get("source", "manifest"),
                table=tbl.get("table", tbl.get("name", "")),
                location=location,
                columns=tbl.get("columns", []),
                row_count=row_count,
            )
        )
    return tables if tables else None


def _discover_from_scan(bucket_uri: str) -> list[TableInfo]:
    """Scan-based fallback: glob for *.parquet, group by parent directory."""
    fs, base_path = _get_fs(bucket_uri)

    try:
        all_files = fs.glob(f"{base_path}/**/*.parquet")
    except Exception:
        all_files = []

    if not all_files:
        return []

    # Group parquet files by their parent directory
    dir_files: dict[str, list[str]] = {}
    for fpath in all_files:
        parent = str(PurePosixPath(fpath).parent)
        dir_files.setdefault(parent, []).append(fpath)

    # Find leaf directories (directories that contain parquet files directly)
    # and group into table-level directories
    table_dirs: dict[str, list[str]] = {}
    for d, files in dir_files.items():
        # Walk up to find the "table root" — the first directory after base_path
        rel = PurePosixPath(d).relative_to(base_path) if d.startswith(base_path) else PurePosixPath(d)
        parts = rel.parts
        if not parts:
            table_key = base_path
        else:
            # Use up to 2 levels as table identifier (source/table)
            table_key = str(PurePosixPath(base_path) / parts[0])
            if len(parts) >= 2:
                table_key = str(PurePosixPath(base_path) / parts[0] / parts[1])
        table_dirs.setdefault(table_key, []).extend(files)

    tables: list[TableInfo] = []
    conn = duckdb.connect()
    try:
        for table_path, files in table_dirs.items():
            first_file = files[0]
            # Build a URI for the first file
            if bucket_uri.startswith("file://"):
                file_uri = f"file://{first_file}"
            elif bucket_uri.startswith("/"):
                file_uri = first_file
            else:
                protocol = bucket_uri.split("://")[0]
                file_uri = f"{protocol}://{first_file}"

            try:
                schema_result = conn.execute(
                    f"SELECT * FROM read_parquet('{file_uri}') LIMIT 0"
                )
                columns = [desc[0] for desc in schema_result.description]
            except Exception:
                columns = []

            try:
                count_result = conn.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{file_uri}')"
                ).fetchone()
                row_count = count_result[0] if count_result else 0
            except Exception:
                row_count = 0

            # Derive table name from path
            rel_path = PurePosixPath(table_path).relative_to(base_path) if table_path.startswith(base_path) else PurePosixPath(table_path)
            table_name = str(rel_path).replace("/", "_").replace("-", "_").replace(".", "_")
            if not table_name or table_name == ".":
                table_name = PurePosixPath(base_path).name.replace("-", "_")

            # Build location URI
            if bucket_uri.startswith("file://"):
                location = f"file://{table_path}"
            elif bucket_uri.startswith("/"):
                location = table_path
            else:
                protocol = bucket_uri.split("://")[0]
                location = f"{protocol}://{table_path}"

            tables.append(
                TableInfo(
                    source="scan",
                    table=table_name,
                    location=location,
                    columns=columns,
                    row_count=row_count,
                )
            )
    finally:
        conn.close()

    return tables


def discover_tables(bucket_uri: str) -> list[TableInfo]:
    """Discover tables in a bucket. Manifest-first, scan-based fallback."""
    tables = _discover_from_manifest(bucket_uri)
    if tables is not None:
        return tables
    return _discover_from_scan(bucket_uri)


def mount_views(conn: duckdb.DuckDBPyConnection, tables: list[TableInfo]) -> None:
    """Create DuckDB views for each discovered table.

    If a table has a fresh local cache, uses local path instead of remote URI.
    """
    from dataspoc_lens.cache import get_cache_meta, get_local_cache_path, is_cache_fresh

    cache_meta = get_cache_meta()

    for tbl in tables:
        location = tbl.location.rstrip("/")
        safe_name = tbl.table.replace("-", "_").replace(".", "_")

        # Check for fresh local cache
        local_path = get_local_cache_path(safe_name)
        if local_path is None:
            local_path = get_local_cache_path(tbl.table)
        if local_path and is_cache_fresh(tbl.table, cache_meta):
            location = str(local_path)
        sql = (
            f"CREATE OR REPLACE VIEW {safe_name} AS "
            f"SELECT * FROM read_parquet('{location}/**/*.parquet', "
            f"hive_partitioning=true, union_by_name=true)"
        )
        try:
            conn.execute(sql)
        except Exception:
            # Fallback: try single level glob
            try:
                sql_flat = (
                    f"CREATE OR REPLACE VIEW {safe_name} AS "
                    f"SELECT * FROM read_parquet('{location}/*.parquet', "
                    f"hive_partitioning=true, union_by_name=true)"
                )
                conn.execute(sql_flat)
            except Exception:
                pass
            sql_single = (
                f"CREATE OR REPLACE VIEW {safe_name} AS "
                f"SELECT * FROM read_parquet('{location}', "
                f"hive_partitioning=true, union_by_name=true)"
            )
            try:
                conn.execute(sql_single)
            except Exception:
                pass


def get_catalog_tables(conn: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """List tables from DuckDB information_schema."""
    result = conn.execute(
        "SELECT table_name, table_type FROM information_schema.tables "
        "WHERE table_schema = 'main' ORDER BY table_name"
    ).fetchall()
    return [{"table_name": row[0], "table_type": row[1]} for row in result]


def get_table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> list[dict[str, str]]:
    """Get column info for a table."""
    result = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_schema = 'main' AND table_name = '{table_name}' "
        "ORDER BY ordinal_position"
    ).fetchall()
    return [{"column_name": row[0], "data_type": row[1]} for row in result]
