"""Cache — local caching of remote Parquet data for offline/low-cost access."""

from __future__ import annotations

import json
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import fsspec

from dataspoc_lens.config import DATASPOC_LENS_HOME

CACHE_DIR = DATASPOC_LENS_HOME / "cache"
CACHE_META_FILE = CACHE_DIR / "cache_meta.json"


def _ensure_cache_dir() -> Path:
    """Ensure the cache directory exists."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR


def get_cache_meta(cache_dir: Path | None = None) -> dict[str, Any]:
    """Load cache_meta.json. Returns empty dict if not found."""
    meta_file = (cache_dir or CACHE_DIR) / "cache_meta.json"
    if not meta_file.exists():
        return {}
    with open(meta_file) as f:
        return json.load(f)


def update_cache_meta(
    table: str,
    info: dict[str, Any],
    cache_dir: Path | None = None,
) -> None:
    """Update metadata for a cached table."""
    cdir = cache_dir or CACHE_DIR
    cdir.mkdir(parents=True, exist_ok=True)
    meta_file = cdir / "cache_meta.json"

    meta = get_cache_meta(cdir)
    meta[table] = info

    with open(meta_file, "w") as f:
        json.dump(meta, f, indent=2, default=str)


def cache_table(
    table_name: str,
    source_uri: str,
    cache_dir: Path | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Download Parquet files from source_uri to local cache.

    Returns metadata dict with cached_at, source_uri, size_bytes, file_count.
    """
    cdir = cache_dir or CACHE_DIR
    table_cache_dir = cdir / table_name

    if table_cache_dir.exists() and not force:
        # Already cached
        meta = get_cache_meta(cdir)
        if table_name in meta:
            return meta[table_name]

    # Clean if forcing refresh
    if table_cache_dir.exists():
        shutil.rmtree(table_cache_dir)

    table_cache_dir.mkdir(parents=True, exist_ok=True)

    # Resolve remote filesystem
    source = source_uri.rstrip("/")
    if source.startswith("file://"):
        fs = fsspec.filesystem("file")
        remote_path = source[7:]
    else:
        fs, _, paths = fsspec.get_fs_token_paths(source)
        remote_path = paths[0] if paths else ""

    # Find parquet files
    if fs.isfile(remote_path):
        parquet_files = [remote_path]
    else:
        parquet_files = fs.glob(f"{remote_path}/**/*.parquet")
        if not parquet_files:
            parquet_files = fs.glob(f"{remote_path}/*.parquet")

    total_size = 0
    file_count = 0

    for remote_file in parquet_files:
        filename = Path(remote_file).name
        local_file = table_cache_dir / filename

        fs.get(remote_file, str(local_file))
        total_size += local_file.stat().st_size
        file_count += 1

    info = {
        "cached_at": datetime.now(timezone.utc).isoformat(),
        "source_uri": source_uri,
        "size_bytes": total_size,
        "file_count": file_count,
    }

    update_cache_meta(table_name, info, cdir)
    return info


def is_cache_fresh(
    table: str,
    cache_meta: dict[str, Any],
    manifest_tables: dict[str, Any] | None = None,
) -> bool:
    """Check if cached data is fresh by comparing cached_at vs manifest last_extraction.

    If no manifest info is available, considers cache as fresh.
    """
    if table not in cache_meta:
        return False

    cached_at_str = cache_meta[table].get("cached_at")
    if not cached_at_str:
        return False

    if not manifest_tables:
        return True

    # Look for the table in manifest
    last_extraction = None
    if isinstance(manifest_tables, dict):
        for key, tbl in manifest_tables.items():
            tbl_name = tbl.get("table", key)
            if tbl_name == table:
                last_extraction = tbl.get("last_extraction")
                break
    elif isinstance(manifest_tables, list):
        for tbl in manifest_tables:
            if tbl.get("table") == table or tbl.get("name") == table:
                last_extraction = tbl.get("last_extraction")
                break

    if not last_extraction:
        # No extraction timestamp in manifest -- consider cache as fresh
        return True

    cached_at = datetime.fromisoformat(cached_at_str)
    extraction_dt = datetime.fromisoformat(last_extraction)

    # Ensure both are tz-aware for comparison
    if cached_at.tzinfo is None:
        cached_at = cached_at.replace(tzinfo=timezone.utc)
    if extraction_dt.tzinfo is None:
        extraction_dt = extraction_dt.replace(tzinfo=timezone.utc)

    return cached_at >= extraction_dt


def clear_cache(
    table: str | None = None,
    cache_dir: Path | None = None,
) -> list[str]:
    """Remove cached data. If table is None, clears all. Returns list of cleared tables."""
    cdir = cache_dir or CACHE_DIR
    meta = get_cache_meta(cdir)
    cleared: list[str] = []

    if table:
        table_dir = cdir / table
        if table_dir.exists():
            shutil.rmtree(table_dir)
        if table in meta:
            del meta[table]
        cleared.append(table)
    else:
        # Clear all
        for tbl_name in list(meta.keys()):
            table_dir = cdir / tbl_name
            if table_dir.exists():
                shutil.rmtree(table_dir)
            cleared.append(tbl_name)
        meta = {}

    # Write updated meta
    meta_file = cdir / "cache_meta.json"
    if meta:
        with open(meta_file, "w") as f:
            json.dump(meta, f, indent=2, default=str)
    elif meta_file.exists():
        meta_file.unlink()

    return cleared


def list_cached_tables(
    cache_dir: Path | None = None,
    manifest_tables: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Return list of cached tables with metadata and freshness status."""
    cdir = cache_dir or CACHE_DIR
    meta = get_cache_meta(cdir)

    result = []
    for table_name, info in meta.items():
        fresh = is_cache_fresh(table_name, meta, manifest_tables)
        result.append(
            {
                "table": table_name,
                "cached_at": info.get("cached_at", ""),
                "size_bytes": info.get("size_bytes", 0),
                "file_count": info.get("file_count", 0),
                "source_uri": info.get("source_uri", ""),
                "status": "fresh" if fresh else "stale",
            }
        )

    return result


def get_local_cache_path(table_name: str, cache_dir: Path | None = None) -> Path | None:
    """Return the local cache directory for a table if it exists and has files."""
    cdir = cache_dir or CACHE_DIR
    table_dir = cdir / table_name
    if table_dir.exists() and any(table_dir.glob("*.parquet")):
        return table_dir
    return None
