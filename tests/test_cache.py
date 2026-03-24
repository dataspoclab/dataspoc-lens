"""Tests for cache — local caching of remote Parquet data."""

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dataspoc_lens.cache import (
    cache_table,
    clear_cache,
    get_cache_meta,
    get_local_cache_path,
    is_cache_fresh,
    list_cached_tables,
    update_cache_meta,
)


def _create_parquet(path: Path, data: dict) -> None:
    """Helper: write a Parquet file from a dict of columns."""
    table = pa.table(data)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(path))


@pytest.fixture
def source_dir(tmp_path):
    """Create a local directory with Parquet files to act as source."""
    table_dir = tmp_path / "source" / "orders"
    _create_parquet(
        table_dir / "part-0001.parquet",
        {"order_id": [1, 2, 3], "amount": [10.0, 20.0, 30.0]},
    )
    _create_parquet(
        table_dir / "part-0002.parquet",
        {"order_id": [4, 5], "amount": [40.0, 50.0]},
    )
    return table_dir


@pytest.fixture
def cache_dir(tmp_path):
    """Provide a temporary cache directory."""
    cd = tmp_path / "cache"
    cd.mkdir()
    return cd


class TestCacheTable:
    def test_copies_files_locally(self, source_dir, cache_dir):
        """cache_table downloads parquet files to local cache dir."""
        info = cache_table(
            "orders",
            f"file://{source_dir}",
            cache_dir=cache_dir,
        )

        cached_files = list((cache_dir / "orders").glob("*.parquet"))
        assert len(cached_files) == 2
        assert info["file_count"] == 2
        assert info["size_bytes"] > 0
        assert "cached_at" in info

    def test_skips_if_already_cached(self, source_dir, cache_dir):
        """cache_table skips download if cache already exists."""
        info1 = cache_table("orders", f"file://{source_dir}", cache_dir=cache_dir)
        info2 = cache_table("orders", f"file://{source_dir}", cache_dir=cache_dir)
        assert info1["cached_at"] == info2["cached_at"]

    def test_force_refresh(self, source_dir, cache_dir):
        """cache_table with force=True re-downloads data."""
        info1 = cache_table("orders", f"file://{source_dir}", cache_dir=cache_dir)
        info2 = cache_table(
            "orders", f"file://{source_dir}", cache_dir=cache_dir, force=True
        )
        # The cached_at should be different (re-downloaded)
        assert info2["file_count"] == 2


class TestCacheMeta:
    def test_metadata_persisted(self, cache_dir):
        """Cache metadata is written to cache_meta.json."""
        update_cache_meta(
            "test_table",
            {"cached_at": "2025-01-01T00:00:00+00:00", "size_bytes": 1234},
            cache_dir=cache_dir,
        )

        meta = get_cache_meta(cache_dir)
        assert "test_table" in meta
        assert meta["test_table"]["size_bytes"] == 1234

    def test_metadata_updated(self, cache_dir):
        """Updating metadata preserves other tables."""
        update_cache_meta(
            "table_a", {"cached_at": "2025-01-01T00:00:00+00:00"}, cache_dir=cache_dir
        )
        update_cache_meta(
            "table_b", {"cached_at": "2025-02-01T00:00:00+00:00"}, cache_dir=cache_dir
        )

        meta = get_cache_meta(cache_dir)
        assert "table_a" in meta
        assert "table_b" in meta

    def test_get_cache_meta_empty(self, cache_dir):
        """get_cache_meta returns empty dict when no meta file."""
        meta = get_cache_meta(cache_dir)
        assert meta == {}


class TestIsCacheFresh:
    def test_fresh_when_no_manifest(self):
        """Cache is considered fresh when no manifest is available."""
        meta = {
            "orders": {"cached_at": "2025-06-01T00:00:00+00:00", "size_bytes": 100}
        }
        assert is_cache_fresh("orders", meta) is True

    def test_fresh_when_cached_after_extraction(self):
        """Cache is fresh when cached_at >= last_extraction."""
        meta = {
            "orders": {"cached_at": "2025-06-15T00:00:00+00:00", "size_bytes": 100}
        }
        manifest = [
            {"table": "orders", "last_extraction": "2025-06-01T00:00:00+00:00"}
        ]
        assert is_cache_fresh("orders", meta, manifest) is True

    def test_stale_when_cached_before_extraction(self):
        """Cache is stale when cached_at < last_extraction."""
        meta = {
            "orders": {"cached_at": "2025-06-01T00:00:00+00:00", "size_bytes": 100}
        }
        manifest = [
            {"table": "orders", "last_extraction": "2025-06-15T00:00:00+00:00"}
        ]
        assert is_cache_fresh("orders", meta, manifest) is False

    def test_not_cached(self):
        """Returns False when table is not in cache meta."""
        assert is_cache_fresh("orders", {}) is False

    def test_fresh_dict_manifest(self):
        """Works with dict-format manifest tables."""
        meta = {
            "orders": {"cached_at": "2025-06-15T00:00:00+00:00", "size_bytes": 100}
        }
        manifest = {
            "src/orders": {
                "table": "orders",
                "last_extraction": "2025-06-01T00:00:00+00:00",
            }
        }
        assert is_cache_fresh("orders", meta, manifest) is True


class TestClearCache:
    def test_clear_specific_table(self, source_dir, cache_dir):
        """clear_cache removes a specific table."""
        cache_table("orders", f"file://{source_dir}", cache_dir=cache_dir)
        assert (cache_dir / "orders").exists()

        cleared = clear_cache("orders", cache_dir=cache_dir)
        assert cleared == ["orders"]
        assert not (cache_dir / "orders").exists()

        meta = get_cache_meta(cache_dir)
        assert "orders" not in meta

    def test_clear_all(self, source_dir, cache_dir):
        """clear_cache with table=None removes all cached tables."""
        cache_table("orders", f"file://{source_dir}", cache_dir=cache_dir)

        cleared = clear_cache(cache_dir=cache_dir)
        assert "orders" in cleared
        assert not (cache_dir / "orders").exists()

    def test_clear_empty_cache(self, cache_dir):
        """clear_cache on empty cache returns empty list."""
        cleared = clear_cache(cache_dir=cache_dir)
        assert cleared == []


class TestListCachedTables:
    def test_lists_cached_tables(self, source_dir, cache_dir):
        """list_cached_tables returns metadata for each cached table."""
        cache_table("orders", f"file://{source_dir}", cache_dir=cache_dir)

        result = list_cached_tables(cache_dir=cache_dir)
        assert len(result) == 1
        assert result[0]["table"] == "orders"
        assert result[0]["size_bytes"] > 0
        assert result[0]["status"] == "fresh"

    def test_empty_cache(self, cache_dir):
        """list_cached_tables returns empty list when no cache."""
        result = list_cached_tables(cache_dir=cache_dir)
        assert result == []


class TestGetLocalCachePath:
    def test_returns_path_when_cached(self, source_dir, cache_dir):
        """Returns path when cache exists with parquet files."""
        cache_table("orders", f"file://{source_dir}", cache_dir=cache_dir)
        path = get_local_cache_path("orders", cache_dir=cache_dir)
        assert path is not None
        assert path == cache_dir / "orders"

    def test_returns_none_when_not_cached(self, cache_dir):
        """Returns None when table is not cached."""
        path = get_local_cache_path("nonexistent", cache_dir=cache_dir)
        assert path is None
