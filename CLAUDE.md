# DataSpoc Lens

## What is this project

DataSpoc Lens is the **virtual warehouse** of the DataSpoc platform. It connects to cloud buckets (written by DataSpoc Pipe or any tool that writes Parquet), mounts tables as DuckDB views, and lets users query with SQL, an interactive shell, Jupyter notebooks, and natural language (AI). It is the analysis layer: organized data in → insights out.

## Core principle

**DuckDB over Parquet in cloud. SQL shell. Transforms. AI. Simple.** No Databricks, no Snowflake, no data warehouse infrastructure. Just a CLI that mounts your bucket and lets you query.

## Architecture

```
[Cloud Bucket] → [Catalog Discovery] → [DuckDB Views] → [Query/Shell/Notebook/AI]
     │                  │
     │            manifest.json (from Pipe)
     │            or scan-based (glob *.parquet)
     │
     └── read via DuckDB httpfs (remote Parquet, no download needed)
         └── or local cache (~/.dataspoc-lens/cache/) for offline work
```

- **Discovery**: manifest-first (reads Pipe's `/.dataspoc/manifest.json`), scan-based fallback (globs for `*.parquet`)
- **DuckDB views**: each table becomes `CREATE VIEW <name> AS SELECT * FROM read_parquet('<path>/*.parquet', hive_partitioning=true, union_by_name=true)`
- **Cache**: copies Parquet from remote to local (`~/.dataspoc-lens/cache/<table>/`), views auto-switch to local path when cache is fresh

## Permissions & Access Control

**Design decision: DataSpoc does NOT implement RBAC or access control.** All permissions are delegated to the cloud provider's IAM (AWS IAM, GCP IAM, Azure AD).

- Lens needs only **READ** access to buckets
- Each analyst has their own cloud credentials that limit which buckets they can see
- If an analyst lacks IAM permission, `add-bucket` fails with "Access Denied" — they see nothing from that bucket
- Best practice: separate buckets per permission level (public, finance, hr, executive)
- Never implement authentication, authorization, or user management in Lens code

## Key design decisions

- **CLI-first**: all operations via `dataspoc-lens` command
- **Read-only on raw**: Lens never writes to raw/. Lens transforms write to gold/ (analyst aggregations). Pipe transforms write to curated/ (DE cleaning)
- **DuckDB in-process**: no server, no daemon. DuckDB runs embedded in the CLI process
- **User provides LLM key**: AI features use the user's own Anthropic/OpenAI API key
- **Manifest as contract**: reads the manifest written by Pipe for table discovery
- **Cache for offline**: local cache allows working without cloud access after initial download

## Directory structure

```
src/dataspoc_lens/
  __init__.py           # Version
  cli.py                # Typer CLI — all commands
  config.py             # LensConfig, buckets list, paths
  catalog.py            # Discovery (manifest + scan), mount_views, TableInfo
  shell.py              # Interactive REPL (prompt_toolkit, autocomplete, dot commands)
  export.py             # Export to CSV, JSON, Parquet
  transforms.py         # Numbered .sql file execution (CTAS pattern)
  notebook.py           # JupyterLab launch with pre-mounted tables
  ai.py                 # NL-to-SQL via LLM (Anthropic/OpenAI)
  cache.py              # Local cache management (download, freshness, clear)

tests/                  # pytest tests
docs/                   # Pre-dev docs + usage guides
```

## Conventions

- **Language**: Python 3.10+
- **CLI framework**: Typer
- **Query engine**: DuckDB (in-process, embedded)
- **Shell**: prompt_toolkit with PygmentsLexer(SqlLexer), WordCompleter, FileHistory
- **Config**: YAML at `~/.dataspoc-lens/config.yaml`
- **Package manager**: uv (preferred), pip compatible
- **Tests**: pytest, run with `pytest tests/ -v`
- **Optional extras**: `[s3]`, `[gcs]`, `[azure]`, `[jupyter]`, `[ai]`, `[all]`
- **CLI messages**: English (standardizing)

## Config structure

```
~/.dataspoc-lens/
  config.yaml           # Registered buckets
  transforms/           # Numbered .sql files for data transformation
    001_clean_users.sql
    002_aggregate_sales.sql
  cache/                # Local cached Parquet files
    <table>/
      *.parquet
    cache_meta.json     # Cache metadata (cached_at, size, freshness)
  history               # Shell command history
```

## How discovery works

1. Read `<bucket>/.dataspoc/manifest.json` (written by Pipe)
2. Parse tables dict (supports both `{"key": {...}}` dict and `[{...}]` list formats)
3. Build location from source + table name: `<bucket>/raw/<source>/<table>/`
4. If no manifest: scan bucket for `*.parquet` files, group by parent directory
5. Create DuckDB view for each table

## Shell dot commands

| Command | Action |
|---------|--------|
| `.tables` | List mounted tables |
| `.schema <table>` | DESCRIBE table |
| `.buckets` | List registered buckets |
| `.cache [table]` | Cache a table locally |
| `.export <format> <path>` | Export last result |
| `.help` | Show commands |
| `.quit` | Exit |

## Cache freshness

- `cached_at` compared to manifest's `last_extraction` timestamp
- If Pipe ran after cache was created → "stale"
- `mount_views()` auto-uses local cache when fresh
- `--refresh` forces re-download
- `--clear` removes cached files

## What NOT to do

- Don't write to the raw bucket — Lens is read-only on `raw/`
- Don't implement data ingestion — that's Pipe's job
- Don't add ML features — that's DataSpoc ML
- Don't add multi-tenancy — IAM handles access
- Don't add streaming — this works on static Parquet
- Don't break compatibility with Pipe's manifest format

## Related projects

- **DataSpoc Pipe** (separate repo): writes the data that Lens reads
- **DataSpoc ML** (separate repo, private): accessed via `dataspoc-lens ml` commands
- **DataSpoc Platform** (separate repo, private): backend for licensing

## License

Apache 2.0 — open source, free to use and modify.
