# DataSpoc Lens Usage Guide

## Table of Contents

1. [Introduction](#1-introduction)
2. [Installation](#2-installation)
3. [Quickstart](#3-quickstart)
4. [Configuration](#4-configuration)
5. [Commands Reference](#5-commands-reference)
6. [Interactive Shell](#6-interactive-shell)
7. [SQL Transforms](#7-sql-transforms)
8. [AI / Natural Language Queries](#8-ai--natural-language-queries)
9. [Notebook (Jupyter)](#9-notebook-jupyter)
   - [Marimo (Reactive Notebook)](#marimo-reactive-notebook)
10. [Multi-cloud Support](#10-multi-cloud-support)
11. [Integration with DataSpoc Pipe](#11-integration-with-dataspoc-pipe)
12. [Export](#12-export)
13. [Cache](#13-cache)

14. [Troubleshooting](#14-troubleshooting)
15. [Practical Examples](#15-practical-examples)
16. [Part of the DataSpoc Platform](#16-part-of-the-dataspoc-platform)

---

## 1. Introduction

DataSpoc Lens is a **virtual warehouse** over cloud Parquet files, powered by DuckDB. It lets you mount cloud buckets (Amazon S3, Google Cloud Storage, Azure Blob Storage, or local directories) as queryable SQL tables -- without copying data or spinning up a database server.

### Who is it for?

- **Data engineers** who need to inspect and validate data produced by pipelines.
- **Data analysts** who want to run SQL directly on data lake files.
- **Data scientists** who want to explore datasets before building models.
- **Platform teams** who need a lightweight query layer over cloud storage.

### How does it connect with DataSpoc Pipe?

DataSpoc Pipe ingests data from APIs and stores it as Parquet files in cloud storage. Lens reads those files (either via a manifest produced by Pipe or by scanning directories) and exposes them as SQL views. This creates a seamless **ingest-then-query** workflow with no ETL server in between.

---

## 2. Installation

### Base installation

```bash
pip install dataspoc-lens
```

### Optional extras

Install only the extras you need:

```bash
# Cloud providers
pip install dataspoc-lens[s3]       # Amazon S3 support
pip install dataspoc-lens[gcs]      # Google Cloud Storage support
pip install dataspoc-lens[azure]    # Azure Blob Storage support

# JupyterLab integration
pip install dataspoc-lens[jupyter]

# Marimo reactive notebook
pip install dataspoc-lens[marimo]

# AI natural language queries (local Ollama or cloud Anthropic/OpenAI)
pip install dataspoc-lens[ai]

# Everything at once
pip install dataspoc-lens[all]
```

### Requirements

- Python 3.9 or later
- Core dependencies (installed automatically): `duckdb`, `fsspec`, `typer`, `rich`, `tabulate`, `pyyaml`, `pydantic`, `prompt_toolkit`, `pygments`
- For cloud access: valid credentials for your cloud provider (see [Multi-cloud Support](#10-multi-cloud-support))

---

## 3. Quickstart

This walkthrough takes you from zero to querying data in five steps.

### Step 1: Initialize configuration

```bash
dataspoc-lens init
```

This creates the directory `~/.dataspoc-lens/` with a default `config.yaml` and a `transforms/` folder.

### Step 2: Register a bucket

Point Lens at a bucket containing Parquet files. This can be an S3 bucket where DataSpoc Pipe has deposited data:

```bash
dataspoc-lens add-bucket s3://my-data-lake
```

Lens will automatically discover tables -- first by looking for a `.dataspoc/manifest.json` (produced by Pipe), then by scanning for `*.parquet` files if no manifest is found. It prints a summary of discovered tables, including column count, row count, and source type.

### Step 3: View the catalog

```bash
dataspoc-lens catalog
```

This lists all tables across all registered buckets. To see a specific table's schema:

```bash
dataspoc-lens catalog --detail orders
```

### Step 4: Launch the interactive shell

```bash
dataspoc-lens shell
```

You are now in the Lens REPL with SQL autocomplete and syntax highlighting:

```
lens> SELECT COUNT(*) FROM orders;
lens> SELECT customer_id, SUM(total) FROM orders GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
```

### Step 5: Ask a question in natural language

```bash
# With Ollama (default, local, no API key needed)
dataspoc-lens setup-ai
dataspoc-lens ask "how many orders did we have yesterday?"

# Or with a cloud provider
export DATASPOC_LLM_API_KEY=sk-...
dataspoc-lens ask "how many orders did we have yesterday?"
```

Lens sends your table schemas and sample data to the LLM, receives SQL, executes it, and prints the results.

---

## 4. Configuration

### Location

All configuration lives under `~/.dataspoc-lens/`:

```
~/.dataspoc-lens/
    config.yaml          # Registered buckets
    transforms/          # Numbered .sql transform files
    history              # Shell command history (auto-managed)
```

### config.yaml structure

```yaml
buckets:
  - s3://my-data-lake
  - gs://another-project-bucket
  - file:///home/user/local-data
```

The `buckets` key holds a list of bucket URIs. Each URI is a data source that Lens scans for Parquet files.

### LLM settings

LLM configuration lives in `~/.dataspoc-lens/config.yaml` and can be overridden by environment variables:

```yaml
llm:
  provider: ollama
  model: duckdb-nsql:7b    # or qwen2.5-coder:1.5b for lighter
```

| Variable | Description | Default |
|----------|-------------|---------|
| `DATASPOC_LLM_PROVIDER` | LLM provider to use | `ollama` |
| `DATASPOC_LLM_MODEL` | Model name | `duckdb-nsql:7b` |
| `DATASPOC_LLM_API_KEY` | API key (only needed for cloud providers) | (none) |

Environment variables override config.yaml values.

Supported values for `DATASPOC_LLM_PROVIDER`: `ollama` (local, free, default), `anthropic` (uses Claude Sonnet, requires API key), or `openai` (uses GPT-4o, requires API key).

---

## 5. Commands Reference

### `init`

Initialize DataSpoc Lens configuration directory.

```bash
dataspoc-lens init
```

Creates `~/.dataspoc-lens/`, `~/.dataspoc-lens/config.yaml`, and `~/.dataspoc-lens/transforms/`. Safe to run multiple times -- it will not overwrite existing files.

### `add-bucket`

Register a bucket and discover tables.

```bash
dataspoc-lens add-bucket <URI>
```

Supported URI schemes: `s3://`, `gs://`, `az://`, `file://`

Examples:

```bash
dataspoc-lens add-bucket s3://my-company-datalake
dataspoc-lens add-bucket gs://analytics-bucket
dataspoc-lens add-bucket az://storage-container
dataspoc-lens add-bucket file:///home/user/local-data
```

If the bucket is already registered, Lens will skip adding it but still re-run discovery. The command prints a table showing every discovered table with its column count, row count, and source (manifest or scan).

### `catalog`

List all discovered tables from all registered buckets.

```bash
# List all tables
dataspoc-lens catalog

# Show detailed schema (columns and types) for a specific table
dataspoc-lens catalog --detail users
```

### `query`

Execute a SQL query and print results to stdout.

```bash
dataspoc-lens query "SELECT * FROM orders LIMIT 10"
dataspoc-lens query "SELECT status, COUNT(*) FROM orders GROUP BY status"
```

Results are displayed as a formatted ASCII table with row count and execution time.

| Flag | Description |
|------|-------------|
| `--export`, `-e` | Export results to a file. Format is detected from the file extension (`.csv`, `.json`, `.parquet`) |

```bash
dataspoc-lens query "SELECT * FROM orders" --export resultado.csv
dataspoc-lens query "SELECT * FROM orders" -e dados.json
dataspoc-lens query "SELECT * FROM orders" -e dados.parquet
```

### `shell`

Launch the interactive SQL shell (REPL). See the dedicated section [Interactive Shell](#6-interactive-shell) for full details.

```bash
dataspoc-lens shell
```

### `setup-ai`

Set up the AI provider for natural language queries. Installs Ollama (if not present) and downloads the default model (`duckdb-nsql:7b`).

```bash
dataspoc-lens setup-ai
```

### `transform list`

List available SQL transform files.

```bash
dataspoc-lens transform list
```

Shows a numbered list of `.sql` files found in `~/.dataspoc-lens/transforms/`.

### `transform run`

Execute all SQL transforms in numeric order. Stops on the first error.

```bash
dataspoc-lens transform run
```

Each transform is executed against DuckDB with all views already mounted. Execution time and status (OK or error) are printed for each file.

### `ask`

Ask a natural language question and get results via AI-generated SQL.

```bash
dataspoc-lens ask "how many users signed up last month?"
dataspoc-lens ask --debug "what is the average order value?"
dataspoc-lens ask "pedidos por cidade" --export cidades.csv
```

| Flag | Description |
|------|-------------|
| `--debug` | Print the full prompt sent to the LLM before showing results |
| `--export`, `-e` | Export results to a file. Format is detected from the file extension (`.csv`, `.json`, `.parquet`) |

The `--debug` flag is useful for understanding what context the LLM receives and for diagnosing unexpected results.

### `notebook`

Launch JupyterLab with tables pre-mounted. Requires the `jupyter` extra.

```bash
dataspoc-lens notebook
```

Use `--marimo` to launch Marimo instead of JupyterLab (requires the `marimo` extra):

```bash
dataspoc-lens notebook --marimo
```

### `cache`

Manage local cache of remote Parquet data. Copies Parquet files from a cloud bucket to your machine, avoiding repeated bandwidth usage and egress costs.

```bash
# Cache a table locally
dataspoc-lens cache <table>

# Force re-download (refresh stale cache)
dataspoc-lens cache <table> --refresh

# List all cached tables with date, size, and freshness status
dataspoc-lens cache --list

# Clear all cached data
dataspoc-lens cache --clear

# Clear cache for a specific table
dataspoc-lens cache <table> --clear
```

| Flag | Description |
|------|-------------|
| `--list` | List cached tables with date, size, and status (fresh/stale) |
| `--refresh` | Force re-download even if cache already exists |
| `--clear` | Clear cached data (all tables, or a specific table if `<table>` is given) |

See the dedicated section [Cache](#13-cache) for full details on how caching works.

### `ml activate`

Display information about the DataSpoc ML commercial product.

```bash
dataspoc-lens ml activate
```

### `ml status`

Show ML gateway status.

```bash
dataspoc-lens ml status
```

### `--version`

Show the installed version of DataSpoc Lens.

```bash
dataspoc-lens --version
dataspoc-lens -v
```

---

## 6. Interactive Shell

The interactive shell is a full-featured SQL REPL built with `prompt_toolkit`.

### Launching

```bash
dataspoc-lens shell
```

### Features

- **SQL syntax highlighting** -- Powered by Pygments SQL lexer. Keywords, strings, and numbers are color-coded.
- **Autocomplete** -- Tab-completion for SQL keywords, table names, and column names. All mounted tables and their columns are available in the completer.
- **History** -- Commands are saved to `~/.dataspoc-lens/history` and persist across sessions. Press the up/down arrow keys to navigate history.
- **Auto-suggest** -- Ghost-text suggestions from history appear as you type.

### Dot commands

Inside the shell, commands starting with a dot (`.`) are meta-commands:

| Command | Description |
|---------|-------------|
| `.tables` | List all mounted tables and their types |
| `.schema <table>` | Show column names and data types for a table |
| `.buckets` | List all registered bucket URIs |
| `.export <format> <path>` | Export the last query result to a file (`csv`, `json`, or `parquet`) |
| `.cache <table>` | Cache a table locally for offline access and reduced latency |
| `.help` | Show the list of available dot commands |
| `.quit` / `.exit` | Exit the shell |

### Using `ask` inside the shell

You can use the `ask` command directly within the shell (without quotes):

```
lens> ask how many orders were placed this week?
SQL: SELECT COUNT(*) FROM orders WHERE order_date >= CURRENT_DATE - INTERVAL '7 days'

+----------+
| count(*) |
+----------|
|      342 |
+----------+
(1 row(s), 0.045s)
```

The AI-generated SQL is shown before the results, and the result is stored as the last result (available for `.export`).

### Example session

```
$ dataspoc-lens shell
DataSpoc Lens Shell
Type SQL or .help for commands. Ctrl+D or .quit to exit.

lens> .tables
  orders (VIEW)
  users (VIEW)
  products (VIEW)

lens> .schema orders
Table: orders
  order_id  BIGINT
  customer_id  BIGINT
  total  DOUBLE
  status  VARCHAR
  created_at  TIMESTAMP

lens> SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status ORDER BY cnt DESC;
+----------+------+
| status   | cnt  |
|----------+------|
| shipped  | 1245 |
| pending  |  342 |
| canceled |   78 |
+----------+------+
(3 row(s), 0.012s)

lens> .export csv /tmp/order_status.csv
Exported 3 rows to /tmp/order_status.csv (csv)

lens> .quit
```

---

## 7. SQL Transforms (Gold Layer)

Transforms are numbered `.sql` files that let analysts build **gold** datasets — aggregations, metrics, reports — from the curated data that Data Engineers prepared with Pipe.

```
Pipe (DE):    source → raw → transform(df) → curated    (Python, automated, cron)
Lens (Analyst): curated → transform SQL → gold           (SQL, on demand, exploratory)
```

Think of them as simple, file-based dbt models — but for analysts, not engineers.

### Directory

Place your transform files in `~/.dataspoc-lens/transforms/`:

```
~/.dataspoc-lens/transforms/
    001_clean_users.sql
    002_aggregate_orders.sql
    003_build_summary.sql
```

### Naming convention

Files are sorted by their numeric prefix and executed in ascending order. Use zero-padded numbers to ensure correct ordering (e.g., `001_`, `002_`, ..., `099_`).

### SQL pattern: CTAS

Use `CREATE TABLE AS SELECT` (CTAS) to materialize transformed data. Since DuckDB is the engine, you have access to its full SQL dialect:

```sql
-- 001_clean_users.sql
CREATE OR REPLACE TABLE curated_users AS
SELECT
    user_id,
    LOWER(TRIM(email)) AS email,
    COALESCE(name, 'Unknown') AS name,
    created_at
FROM users
WHERE email IS NOT NULL;
```

```sql
-- 002_aggregate_orders.sql
CREATE OR REPLACE TABLE order_summary AS
SELECT
    customer_id,
    COUNT(*) AS order_count,
    SUM(total) AS total_spent,
    MIN(created_at) AS first_order,
    MAX(created_at) AS last_order
FROM orders
GROUP BY customer_id;
```

```sql
-- 003_build_summary.sql
CREATE OR REPLACE TABLE customer_360 AS
SELECT
    u.user_id,
    u.name,
    u.email,
    COALESCE(o.order_count, 0) AS order_count,
    COALESCE(o.total_spent, 0) AS total_spent,
    o.first_order,
    o.last_order
FROM curated_users u
LEFT JOIN order_summary o ON u.user_id = o.customer_id;
```

### Running transforms

```bash
# List available transforms
dataspoc-lens transform list

# Execute all transforms in order
dataspoc-lens transform run
```

Execution stops on the first error. Each transform sees the results of the previous ones (tables created by earlier transforms are available to later transforms).

### Data lake layers

| Layer | Who | Tool | Path | Purpose |
|-------|-----|------|------|---------|
| **Raw** | DE | Pipe | `raw/` | Unmodified source data |
| **Curated** | DE | Pipe transform | `curated/` | Cleaned, typed, deduplicated |
| **Gold** | Analyst | Lens transform | `gold/` | Aggregations, metrics, reports |

When writing Lens transforms that export back to the bucket, use the `gold/` directory:

```sql
-- 001_revenue_by_month.sql
COPY (
    SELECT DATE_TRUNC('month', order_date) AS month,
           SUM(total) AS revenue
    FROM clean_orders
    GROUP BY 1
) TO 's3://my-bucket/gold/revenue/monthly.parquet' (FORMAT PARQUET);
```

```
s3://my-bucket/
    raw/              ← Pipe writes (source data)
    curated/          ← Pipe transforms (DE cleaned data)
    gold/             ← Lens transforms (analyst aggregations)
```

You can also export query results directly:

```bash
dataspoc-lens query "SELECT * FROM customer_360" --export customer_360.parquet
```

---

## 8. AI / Natural Language Queries

Lens can convert natural language questions into SQL using an LLM. The default provider is **Ollama** (local, free, no API key needed). Cloud providers (Anthropic Claude, OpenAI GPT) are also supported.

### Setup

#### Option A: Ollama (default -- local, free)

```bash
dataspoc-lens setup-ai
```

This installs Ollama (if not already installed) and downloads the `duckdb-nsql:7b` model. No API key is needed.

#### Option B: Cloud provider

```bash
pip install dataspoc-lens[ai]

# For Anthropic
export DATASPOC_LLM_PROVIDER=anthropic
export DATASPOC_LLM_API_KEY=sk-ant-...

# For OpenAI
export DATASPOC_LLM_PROVIDER=openai
export DATASPOC_LLM_API_KEY=sk-...
```

### Configuration

LLM settings are stored in `~/.dataspoc-lens/config.yaml`:

```yaml
llm:
  provider: ollama
  model: duckdb-nsql:7b    # or qwen2.5-coder:1.5b for lighter
```

Environment variables override config values:

| Variable | Description | Default |
|----------|-------------|---------|
| `DATASPOC_LLM_PROVIDER` | LLM provider | `ollama` |
| `DATASPOC_LLM_MODEL` | Model name | `duckdb-nsql:7b` |
| `DATASPOC_LLM_API_KEY` | API key (cloud providers only) | (none) |

### Providers

| Provider | Model used | API key required |
|----------|-----------|-----------------|
| `ollama` (default) | duckdb-nsql:7b | No (local) |
| `anthropic` | Claude Sonnet | Yes |
| `openai` | GPT-4o | Yes |

### Usage

From the command line:

```bash
dataspoc-lens ask "how many users signed up last month?"
dataspoc-lens ask "what are the top 10 products by revenue?"
dataspoc-lens ask "show me orders with status pending created in the last 7 days"
```

From the interactive shell (no quotes needed):

```
lens> ask what is the average order value by month?
```

### How it works

1. Lens reads the DDL (column names and types) of all mounted views.
2. It fetches 3 sample rows from each table to give the LLM context about the actual data.
3. It builds a prompt containing the schema, sample data, and the user's question.
4. The LLM returns raw SQL (no explanations, no markdown).
5. Lens extracts the SQL, executes it against DuckDB, and prints the results.

### Debugging with `--debug`

Use `--debug` to see the full prompt sent to the LLM:

```bash
dataspoc-lens ask --debug "how many orders per day this week?"
```

This prints the complete prompt (including schema DDL and sample data) before showing the SQL and results. It is useful for:

- Understanding what context the LLM is working with.
- Diagnosing wrong or unexpected SQL output.
- Verifying that table schemas are correct.

### Limitations

- The LLM does not have access to the full dataset, only 3 sample rows per table.
- Complex joins across many tables may produce incorrect SQL.
- The quality of results depends on the LLM model and the clarity of the question.
- With cloud providers, each `ask` call makes an API request, which incurs costs. Ollama runs locally at no cost.
- The `--debug` flag is only available from the CLI, not from within the shell.

---

## 9. Notebook (Jupyter)

Lens can launch JupyterLab with all your tables pre-mounted and ready to query.

### Setup

```bash
pip install dataspoc-lens[jupyter]
```

### Launching

```bash
dataspoc-lens notebook
```

This generates a startup script at `~/.ipython/profile_default/startup/00-dataspoc-lens.py`, then launches JupyterLab. When JupyterLab starts, the startup script:

1. Creates a DuckDB connection.
2. Loads the `httpfs` extension.
3. Discovers and mounts views from all registered buckets.
4. Loads the `jupysql` extension for `%%sql` magic support.

### Using `%%sql` magic

In any notebook cell, use the `%%sql` cell magic to run SQL directly:

```python
%%sql
SELECT * FROM orders LIMIT 10
```

```python
%%sql
SELECT status, COUNT(*) AS cnt
FROM orders
GROUP BY status
ORDER BY cnt DESC
```

### Using the DuckDB connection directly

A `conn` variable (DuckDB connection) is available in the notebook namespace:

```python
df = conn.execute("SELECT * FROM users LIMIT 100").fetchdf()
df.head()
```

### Pre-mounted tables

All tables from your registered buckets are available as views. You can list them:

```python
%%sql
SHOW TABLES
```

### Marimo (Reactive Notebook)

Marimo is a modern reactive notebook -- cells update automatically when dependencies change. More interactive than Jupyter for data exploration.

```bash
# Install
pip install dataspoc-lens[marimo]

# Launch
dataspoc-lens notebook --marimo
```

Opens in the browser with tables pre-mounted. Features:

- **Reactive cells**: change a query, results update automatically
- **App mode**: toggle between code view and clean app view
- **Interactive dataframes**: filter, sort, explore with mouse
- **Built-in charts**: click to generate visualizations
- **No "Run All" needed**: everything reacts to changes

The `conn` object is available in all cells:

```python
conn.sql("SELECT * FROM orders LIMIT 10").df()
```

Use `dataspoc-lens notebook` for Jupyter (default) or `--marimo` for Marimo.

---

## 10. Multi-cloud Support

Lens supports reading Parquet files from multiple cloud providers and local filesystems.

### Amazon S3

```bash
pip install dataspoc-lens[s3]
dataspoc-lens add-bucket s3://my-bucket
```

**Credentials:** DuckDB uses the `httpfs` extension, which reads AWS credentials from standard locations:

- Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
- AWS credentials file: `~/.aws/credentials`
- IAM instance roles (when running on EC2/ECS)

### Google Cloud Storage

```bash
pip install dataspoc-lens[gcs]
dataspoc-lens add-bucket gs://my-bucket
```

**Credentials:** Uses `GOOGLE_APPLICATION_CREDENTIALS` environment variable pointing to a service account JSON file, or Application Default Credentials (ADC).

### Azure Blob Storage

```bash
pip install dataspoc-lens[azure]
dataspoc-lens add-bucket az://my-container
```

**Credentials:** Uses `AZURE_STORAGE_ACCOUNT` and `AZURE_STORAGE_KEY` environment variables, or Azure CLI authentication.

### Local filesystem

```bash
dataspoc-lens add-bucket file:///path/to/local/data
```

No credentials needed. Useful for development and testing.

### Multiple buckets

You can register buckets from different providers simultaneously:

```bash
dataspoc-lens add-bucket s3://production-data
dataspoc-lens add-bucket gs://analytics-project
dataspoc-lens add-bucket file:///home/user/test-data
```

All tables from all buckets are merged into a single catalog and available for cross-source queries.

---

## 11. Integration with DataSpoc Pipe

DataSpoc Pipe ingests data from APIs and writes Parquet files to cloud storage. Lens is designed to read directly from Pipe's output.

### Manifest-first discovery

When Pipe writes data, it produces a `.dataspoc/manifest.json` file at the bucket root:

```
s3://my-bucket/
    .dataspoc/
        manifest.json
    raw/
        users/
            part-0001.parquet
        orders/
            part-0001.parquet
```

The manifest contains table definitions:

```json
{
  "tables": [
    {
      "table": "users",
      "source": "pipe",
      "location": "raw/users",
      "columns": ["user_id", "name", "email", "created_at"],
      "row_count": 15000
    },
    {
      "table": "orders",
      "source": "pipe",
      "location": "raw/orders",
      "columns": ["order_id", "customer_id", "total", "status", "created_at"],
      "row_count": 120000
    }
  ]
}
```

When `add-bucket` runs, it first checks for this manifest. If found, it uses the manifest for fast, accurate table discovery. If no manifest exists, Lens falls back to scanning for `*.parquet` files and grouping them by directory.

### Scan-based fallback

Without a manifest, Lens scans the bucket recursively for `*.parquet` files. Directories become tables:

- `raw/users/*.parquet` becomes the table `raw_users`
- `raw/orders/*.parquet` becomes the table `raw_orders`

Table names are derived from the directory path relative to the bucket root, with slashes and hyphens replaced by underscores.

### DuckDB view creation

Each discovered table becomes a DuckDB view using:

```sql
CREATE OR REPLACE VIEW <table_name> AS
SELECT * FROM read_parquet('<location>/*.parquet',
    hive_partitioning=true,
    union_by_name=true)
```

Key features:
- **`hive_partitioning=true`**: Hive-style partition directories (e.g., `year=2024/month=01/`) are automatically parsed into columns.
- **`union_by_name=true`**: Files with different schemas are merged by column name, enabling schema evolution.

---

## 12. Export

Lens supports exporting query results to three formats: CSV, JSON, and Parquet. Export is a flag (`--export` / `-e`) on the `query` and `ask` commands. The format is detected automatically from the file extension.

### Via CLI

```bash
# CSV
dataspoc-lens query "SELECT * FROM users WHERE active = true" --export active_users.csv

# JSON (array of objects, indented)
dataspoc-lens query "SELECT * FROM users WHERE active = true" -e active_users.json

# Parquet (uses DuckDB COPY TO for efficiency)
dataspoc-lens query "SELECT * FROM users WHERE active = true" -e active_users.parquet

# Export from an AI-generated query
dataspoc-lens ask "pedidos por cidade" --export cidades.csv
```

### Via the interactive shell

Inside the shell, run a query first, then export the last result:

```
lens> SELECT * FROM users WHERE active = true;
(1500 row(s), 0.034s)

lens> .export csv /tmp/active_users.csv
Exported 1500 rows to /tmp/active_users.csv (csv)

lens> .export parquet /tmp/active_users.parquet
Exported 1500 rows to /tmp/active_users.parquet (parquet)
```

### Format details

| Format | Method | Notes |
|--------|--------|-------|
| CSV | Python `csv` module | UTF-8 encoded, standard comma delimiter |
| JSON | Python `json` module | Array of objects, indented, non-ASCII preserved |
| Parquet | DuckDB `COPY TO` (CLI) / DuckDB temp table (shell) | Efficient columnar format |

---

## 13. Cache

Lens includes a local cache that copies Parquet files from a remote bucket to your machine. Once cached, queries read from the local copy instead of making cloud requests, eliminating bandwidth usage and egress costs.

### Why cache?

- **Offline work** -- keep querying data even without internet access.
- **Reduced latency** -- local disk reads are faster than cloud round-trips, especially for large scans.
- **Avoid egress costs** -- cloud providers charge for data leaving their network. Caching once avoids paying per-query.

### Commands

Cache a table:

```bash
dataspoc-lens cache orders
```

List cached tables (shows cached date, size, and freshness status):

```bash
dataspoc-lens cache --list
```

Example output:

```
         Cached Tables
┌──────────┬─────────────────────┬──────────┬────────┐
│ Table    │ Cached At           │ Size     │ Status │
├──────────┼─────────────────────┼──────────┼────────┤
│ orders   │ 2026-03-20T14:32:01 │ 24.3 MB  │ fresh  │
│ users    │ 2026-03-18T09:15:44 │ 3.1 MB   │ stale  │
└──────────┴─────────────────────┴──────────┴────────┘
```

Force a re-download (useful when the cache is stale):

```bash
dataspoc-lens cache orders --refresh
```

Clear all cached data:

```bash
dataspoc-lens cache --clear
```

Clear cache for a specific table:

```bash
dataspoc-lens cache orders --clear
```

In the interactive shell, use the `.cache` dot command:

```
lens> .cache orders
Caching 'orders'...
Cached 'orders': 4 file(s), 24.3 MB
```

### How it works

1. `dataspoc-lens cache <table>` downloads all Parquet files from the table's remote location to `~/.dataspoc-lens/cache/<table>/`.
2. A `cache_meta.json` file in the cache directory tracks each cached table's timestamp (`cached_at`), source URI, size, and file count.
3. When views are mounted (via `shell`, `query`, `notebook`, etc.), Lens checks whether a local cache exists for each table. If the cache is **fresh**, the view is created over the local path instead of the remote URI, so all queries run against local data with zero cloud I/O.
4. If no cache exists for a table, the view is created over the remote URI as usual -- caching is entirely opt-in.

### Freshness

Cache freshness is determined by comparing two timestamps:

- **`cached_at`** -- when the local cache was last written (stored in `cache_meta.json`).
- **`last_extraction`** -- the most recent extraction timestamp from the `.dataspoc/manifest.json` produced by DataSpoc Pipe.

If Pipe ran a new extraction **after** the cache was created, the cache is marked **stale**. This means the remote bucket contains newer data than your local copy. Run `dataspoc-lens cache <table> --refresh` to bring the cache up to date.

If no manifest is available (e.g., the bucket was added via directory scan), the cache is always considered **fresh** because there is no extraction timestamp to compare against.

### When to use caching

| Scenario | Recommendation |
|----------|----------------|
| Exploring data on a flight or without internet | Cache tables before going offline |
| Running many ad-hoc queries on the same table | Cache once, query many times with lower latency |
| Large tables with significant egress costs | Cache to avoid repeated download charges |
| Data that updates frequently (hourly Pipe runs) | Use `--list` to check freshness; `--refresh` when needed |
| Small tables on a fast connection | Caching is optional -- remote reads may be fast enough |

### Directory structure

```
~/.dataspoc-lens/
    cache/
        cache_meta.json          # Metadata for all cached tables
        orders/
            part-0001.parquet
            part-0002.parquet
        users/
            part-0001.parquet
```

---

## 14. Troubleshooting

### "No buckets registered"

You need to add at least one bucket before using `catalog`, `query`, or `shell`.

```bash
dataspoc-lens init
dataspoc-lens add-bucket s3://my-bucket
```

### "No tables found in this bucket"

- Verify the bucket contains `.parquet` files.
- Check that your cloud credentials are configured (see [Multi-cloud Support](#10-multi-cloud-support)).
- Ensure the URI scheme is correct (`s3://`, `gs://`, `az://`, `file://`).

### "Module 'anthropic' not found" or "Module 'openai' not found"

Install the AI extra:

```bash
pip install dataspoc-lens[ai]
```

### "Configure DATASPOC_LLM_API_KEY"

This only applies when using a cloud provider (anthropic or openai). Set the environment variable with your API key:

```bash
export DATASPOC_LLM_API_KEY=sk-...
```

If you prefer local AI with no API key, use Ollama (the default):

```bash
dataspoc-lens setup-ai
```

### "JupyterLab not found"

Install the Jupyter extra:

```bash
pip install dataspoc-lens[jupyter]
```

### httpfs errors when accessing cloud storage

Make sure the `httpfs` extension is available (it is installed automatically on first use). Verify cloud credentials:

```bash
# For AWS
aws sts get-caller-identity

# For GCP
gcloud auth application-default print-access-token

# For Azure
az account show
```

### Shell autocomplete not working

Autocomplete requires `prompt_toolkit` and `pygments`. These are installed with the base package. If missing:

```bash
pip install prompt_toolkit pygments
```

### Transform execution fails

- Check the SQL syntax in your transform file.
- Transforms execute sequentially -- a later transform can reference tables created by earlier ones, but not vice versa.
- Use `dataspoc-lens transform list` to verify your files are detected and ordered correctly.

### Parquet schema evolution issues

If files in the same directory have different schemas, DuckDB's `union_by_name=true` option will merge them. Missing columns will be filled with `NULL`. If this causes unexpected results, consider using transforms to clean the data.

---

## 15. Practical Examples

### Example 1: Exploring an e-commerce data lake

An online store uses DataSpoc Pipe to ingest order and customer data to S3.

```bash
# Initialize and register the bucket
dataspoc-lens init
dataspoc-lens add-bucket s3://ecommerce-datalake

# Check what tables are available
dataspoc-lens catalog

# Inspect the orders table
dataspoc-lens catalog --detail orders

# Quick query from the command line
dataspoc-lens query "SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status"

# Launch the shell for interactive exploration
dataspoc-lens shell
```

Inside the shell:

```
lens> SELECT DATE_TRUNC('month', created_at) AS month, COUNT(*) AS orders, SUM(total) AS revenue
      FROM orders
      GROUP BY 1
      ORDER BY 1 DESC
      LIMIT 12;

lens> .export csv /tmp/monthly_revenue.csv
```

Use AI to answer a business question:

```bash
dataspoc-lens ask "what are the top 5 customers by lifetime value?"
```

### Example 2: Building a curated layer with transforms

You have raw data and want to build clean, aggregated tables.

```bash
dataspoc-lens init
dataspoc-lens add-bucket s3://analytics-bucket
```

Create transform files:

```sql
-- ~/.dataspoc-lens/transforms/001_clean_events.sql
CREATE OR REPLACE TABLE clean_events AS
SELECT
    event_id,
    user_id,
    event_type,
    CAST(event_timestamp AS TIMESTAMP) AS event_time,
    properties
FROM raw_events
WHERE event_type IS NOT NULL
  AND event_timestamp IS NOT NULL;
```

```sql
-- ~/.dataspoc-lens/transforms/002_daily_active_users.sql
CREATE OR REPLACE TABLE daily_active_users AS
SELECT
    CAST(event_time AS DATE) AS day,
    COUNT(DISTINCT user_id) AS dau
FROM clean_events
GROUP BY 1
ORDER BY 1;
```

Run the transforms and export the result:

```bash
dataspoc-lens transform list
dataspoc-lens transform run
dataspoc-lens query "SELECT * FROM daily_active_users" --export dau.parquet
```

### Example 3: Multi-cloud analysis with Jupyter

You have data split across S3 and GCS and want to analyze it interactively in a notebook.

```bash
pip install dataspoc-lens[all]
dataspoc-lens init

# Register buckets from different clouds
dataspoc-lens add-bucket s3://company-transactions
dataspoc-lens add-bucket gs://partner-data

# Verify all tables are visible
dataspoc-lens catalog

# Launch Jupyter with everything pre-mounted
dataspoc-lens notebook
```

In JupyterLab:

```python
%%sql
SELECT
    t.transaction_id,
    t.amount,
    p.partner_name
FROM transactions t
JOIN partners p ON t.partner_id = p.partner_id
WHERE t.amount > 1000
ORDER BY t.amount DESC
LIMIT 20
```

```python
# Use the DuckDB connection directly for pandas integration
df = conn.execute("""
    SELECT DATE_TRUNC('week', transaction_date) AS week, SUM(amount) AS total
    FROM transactions
    GROUP BY 1
    ORDER BY 1
""").fetchdf()

df.plot(x='week', y='total', kind='line', title='Weekly Transaction Volume')
```

---

## 16. Part of the DataSpoc Platform

DataSpoc Lens is one component of the DataSpoc data platform. The components work together to provide a complete data workflow:

```
DataSpoc Pipe          DataSpoc Lens           DataSpoc ML
(Ingestion)     --->   (Query & Transform)  --->  (Machine Learning)
```

### DataSpoc Pipe

Data ingestion from APIs to cloud storage. Pipe writes Parquet files and produces a `.dataspoc/manifest.json` that Lens reads for fast table discovery.

### DataSpoc Lens

Virtual warehouse: SQL queries over cloud Parquet files via DuckDB. Lens reads Pipe's output, lets you explore data interactively, build curated layers with transforms, ask natural language questions via AI, and export results.

### DataSpoc ML

Machine learning on your data (commercial product). Train models on data in your bucket, serve predictions via REST API, and monitor model drift and performance. Contact ml@dataspoc.com or visit https://dataspoc.com/ml for more information.

### The Pipe-Lens-ML flow

1. **Pipe** ingests raw data from APIs and stores it as Parquet in cloud storage.
2. **Lens** mounts the raw data as SQL views, allowing exploration and transformation.
3. Transforms in Lens create curated datasets ready for analysis or modeling.
4. **ML** trains models on the curated data and serves predictions.

This architecture keeps data in cloud storage (no database server), uses open formats (Parquet), and lets each tool do what it does best.

---

## License

DataSpoc Lens is open-source software released under the Apache-2.0 license.
