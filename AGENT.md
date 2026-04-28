# DataSpoc Lens — Agent Guide

You are interacting with DataSpoc Lens, a virtual warehouse over cloud Parquet files powered by DuckDB.

## What you can do

| Capability | How | Returns |
|------------|-----|---------|
| Discover tables | `dataspoc-lens catalog` | Table names, column count, row count |
| Inspect schema | `dataspoc-lens catalog --detail <table>` | Column names and types |
| Run SQL | `dataspoc-lens query "<sql>"` | Rows, columns, duration |
| Ask in natural language | `dataspoc-lens ask "<question>"` | Generated SQL + results |
| Export results | `dataspoc-lens query "<sql>" --export file.csv` | CSV, JSON, or Parquet file |
| Check cache freshness | `dataspoc-lens cache --list` | Cached tables with fresh/stale status |
| Refresh stale cache | `dataspoc-lens cache <table> --refresh` | Re-downloads from remote |
| Clear cache | `dataspoc-lens cache --clear` | Removes local cached data |
| List transforms | `dataspoc-lens transform list` | Numbered .sql files |
| Run transforms | `dataspoc-lens transform run` | Executes SQL transforms in order |
| Launch notebook | `dataspoc-lens notebook` | Opens JupyterLab with tables mounted |

## Python SDK

```python
from dataspoc_lens.connect import connect

# Connect and mount all registered bucket tables
conn, tables = connect()

# Query
result = conn.execute("SELECT * FROM orders LIMIT 10").fetchdf()

# AI ask
from dataspoc_lens.ai import ask
result = ask(conn, "top 10 customers by revenue")
# Returns: {"sql": "...", "columns": [...], "rows": [...], "duration": 0.03}

# Discovery
from dataspoc_lens.catalog import discover_tables, get_table_columns
tables = discover_tables("s3://my-bucket")  # returns list[TableInfo]
columns = get_table_columns(conn, "orders")  # returns list[dict]

# Cache
from dataspoc_lens.cache import cache_table, list_cached_tables, clear_cache
cache_table("orders", "s3://bucket/raw/orders/", force=True)  # refresh
status = list_cached_tables()  # [{table, status: fresh/stale, ...}]
clear_cache(table="orders")  # or clear_cache() for all
```

## Key functions

| Module | Function | Signature | Returns |
|--------|----------|-----------|---------|
| `connect` | `connect()` | `() → (DuckDBPyConnection, list[str])` | Connection + table names |
| `catalog` | `discover_tables(bucket)` | `(str) → list[TableInfo]` | Tables with location, columns, row_count |
| `catalog` | `get_table_columns(conn, table)` | `(conn, str) → list[dict]` | `[{column_name, data_type}]` |
| `catalog` | `mount_views(conn, tables)` | `(conn, list[TableInfo]) → None` | Creates DuckDB views |
| `shell` | `run_query(conn, sql)` | `(conn, str) → (list, list, float)` | `(columns, rows, duration)` |
| `ai` | `ask(conn, question, ...)` | `(conn, str, ...) → dict` | `{sql, columns, rows, duration, error}` |
| `ai` | `build_schema_context(conn)` | `(conn) → str` | Schema as JSON string for LLM |
| `export` | `export_csv(conn, sql, path)` | `(conn, str, str) → int` | Row count |
| `export` | `export_json(conn, sql, path)` | `(conn, str, str) → int` | Row count |
| `export` | `export_parquet(conn, sql, path)` | `(conn, str, str) → int` | Row count |
| `cache` | `cache_table(name, uri, force)` | `(str, str, bool) → dict` | `{cached_at, size_bytes, file_count}` |
| `cache` | `list_cached_tables()` | `() → list[dict]` | `[{table, status, cached_at, size_bytes}]` |
| `cache` | `is_cache_fresh(table, meta, manifest)` | `(str, dict, dict) → bool` | True if cache is up to date |
| `cache` | `clear_cache(table?)` | `(str?) → list[str]` | Cleared table names |
| `config` | `load_config()` | `() → LensConfig` | Registered buckets, LLM settings |

## Configuration

```
~/.dataspoc-lens/
  config.yaml       # Registered buckets + LLM provider settings
  transforms/       # Numbered .sql files (001_clean.sql, 002_agg.sql)
  cache/            # Local Parquet cache
  history           # Shell command history
```

### config.yaml

```yaml
buckets:
  - s3://my-data-lake
  - file:///local/data

llm:
  provider: ollama       # ollama (free, local), anthropic, openai
  model: duckdb-nsql:7b
  api_key: ""            # or set DATASPOC_LLM_API_KEY env var
```

## Data flow

```
Cloud Bucket (S3/GCS/Azure)
  └── .dataspoc/manifest.json    ← written by Pipe, read by Lens
  └── raw/<source>/<table>/*.parquet

Lens reads bucket → mounts DuckDB views → query/ask/export
Lens cache → ~/.dataspoc-lens/cache/<table>/*.parquet (local copy)
```

## Patterns for agents

### Pattern: Explore then query
```
1. dataspoc-lens catalog                    → see what tables exist
2. dataspoc-lens catalog --detail orders    → see columns
3. dataspoc-lens query "SELECT ..."         → run SQL
```

### Pattern: Ensure fresh data then query
```
1. dataspoc-lens cache --list               → check freshness
2. dataspoc-lens cache orders --refresh     → refresh if stale
3. dataspoc-lens query "SELECT ..."         → query on fresh data
```

### Pattern: Natural language exploration
```
1. dataspoc-lens catalog                    → get table context
2. dataspoc-lens ask "top customers by revenue"  → AI generates SQL
3. dataspoc-lens ask "break that down by month"  → follow-up
```

## Constraints

- Read-only on raw data. Lens never writes to raw/ in the bucket.
- DuckDB runs in-process. No server, no daemon.
- AI ask requires LLM config (Ollama local or API key for Anthropic/OpenAI).
- Cache freshness compares against Pipe's manifest.json `last_extraction` timestamp.
- All access control is cloud IAM. Lens needs READ access to buckets.
