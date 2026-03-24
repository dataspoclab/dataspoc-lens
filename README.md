<h1 align="center">DataSpoc Lens</h1>

<p align="center">
  <a href="#"><img src="https://img.shields.io/github/actions/workflow/status/dataspoc/dataspoc-lens/ci.yml?branch=main&style=flat-square&label=CI" alt="CI"></a>
  <a href="https://pypi.org/project/dataspoc-lens/"><img src="https://img.shields.io/pypi/v/dataspoc-lens?style=flat-square" alt="PyPI"></a>
  <a href="https://github.com/dataspoclab/dataspoc-lens/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square" alt="License"></a>
  <a href="https://pypi.org/project/dataspoc-lens/"><img src="https://img.shields.io/pypi/pyversions/dataspoc-lens?style=flat-square" alt="Python 3.10+"></a>
  <a href="#community"><img src="https://img.shields.io/badge/Discord-join%20chat-7289da?style=flat-square&logo=discord&logoColor=white" alt="Discord"></a>
</p>

<p align="center"><em>SQL over cloud Parquet. Query your data lake from the terminal.</em></p>

<p align="center">
  <a href="#features-overview">Docs</a> &nbsp;|&nbsp;
  <a href="#quick-start">Tutorial</a> &nbsp;|&nbsp;
  <a href="#community">Discord</a>
</p>

<!-- TODO: Add hero GIF/screenshot of the interactive shell running a query -->

## Why Lens?

Data teams store Parquet in S3, GCS, or Azure but still spin up heavy warehouses just to run SQL. **DataSpoc Lens** mounts cloud buckets as DuckDB views and gives you an interactive shell, notebooks, AI-powered queries, and local caching -- all from a single CLI. No servers, no infrastructure, no data copying.

## Highlights

- **Zero infrastructure** -- DuckDB runs in-process, no server or daemon
- **Multi-cloud** -- S3, GCS, and Azure Blob Storage via fsspec + httpfs
- **Interactive shell** -- SQL REPL with syntax highlighting and autocomplete
- **AI Ask** -- natural language to SQL using Ollama, Anthropic, or OpenAI
- **Local cache** -- download once, query offline
- **Transforms** -- numbered `.sql` files for repeatable data pipelines
- **Notebook ready** -- launch JupyterLab or Marimo with tables pre-mounted
- **Export anywhere** -- CSV, JSON, or Parquet with a single flag

## Installation

```bash
pip install dataspoc-lens[s3]
```

<details>
<summary>Other install options</summary>

```bash
# Google Cloud Storage
pip install dataspoc-lens[gcs]

# Azure Blob Storage
pip install dataspoc-lens[azure]

# JupyterLab integration
pip install dataspoc-lens[jupyter]

# AI natural language queries
pip install dataspoc-lens[ai]

# Everything
pip install dataspoc-lens[all]
```

</details>

## Quick Start

```bash
# Initialize configuration
dataspoc-lens init

# Register a cloud bucket
dataspoc-lens add-bucket s3://my-data-lake

# Run a SQL query
dataspoc-lens query "SELECT * FROM orders LIMIT 10"

# Launch the interactive shell
dataspoc-lens shell

# Ask a question in plain English
dataspoc-lens ask "how many orders were placed yesterday?"
```

## Features Overview

### Shell

An interactive SQL REPL powered by `prompt_toolkit` with syntax highlighting, autocomplete for table and column names, and dot commands (`.tables`, `.schema`, `.export`, `.help`).

```
$ dataspoc-lens shell
lens> SELECT customer_id, count(*) FROM orders GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
```

### Notebook

Launch JupyterLab or Marimo with all tables pre-mounted — ready to query from the first cell.

```bash
# JupyterLab (default)
pip install dataspoc-lens[jupyter]
dataspoc-lens notebook

# Marimo (reactive — cells update automatically)
pip install dataspoc-lens[marimo]
dataspoc-lens notebook --marimo
```

### AI Ask

Turn plain English into SQL. Lens sends your schema and a data sample to the LLM, gets back SQL, and executes it.

```bash
# One-time setup: install local AI (free, no API key)
dataspoc-lens setup-ai

# Ask questions
dataspoc-lens ask "top 10 customers by revenue this month"
dataspoc-lens ask "which cities have the most orders?" --export cities.csv
```

Configure in `~/.dataspoc-lens/config.yaml`:

```yaml
llm:
  provider: ollama          # ollama (local, free), anthropic, openai
  model: duckdb-nsql:7b     # or qwen2.5-coder:1.5b (lighter)
```

<details><summary>Use a cloud provider instead</summary>

```bash
export DATASPOC_LLM_PROVIDER=anthropic
export DATASPOC_LLM_API_KEY=sk-...
dataspoc-lens ask "which products have declining sales?"
```

</details>

### Cache

Copy tables from the cloud to your local machine. Work offline, reduce latency, avoid egress costs. Queries automatically use local cache when fresh.

```bash
# Cache a table locally
dataspoc-lens cache orders

# Check cache status (fresh/stale)
dataspoc-lens cache --list

# Re-download after new data arrives
dataspoc-lens cache orders --refresh

# Clear cache
dataspoc-lens cache --clear
```

Freshness: compares your cache timestamp against the manifest's `last_extraction`. If Pipe ran after your cache, it shows "stale".

### Transforms

Numbered `.sql` files in `~/.dataspoc-lens/transforms/` that run in order, writing results to the `/curated/` prefix.

```
transforms/
  001_clean_users.sql
  002_aggregate_orders.sql
  003_build_summary.sql
```

```bash
dataspoc-lens transform run
```

### Export

Add `--export` to any query or ask command to save results as CSV, JSON, or Parquet.

```bash
dataspoc-lens query "SELECT * FROM orders" --export csv -o orders.csv
dataspoc-lens ask "monthly revenue" --export parquet -o revenue.parquet
```

## Access Control

DataSpoc delegates all access control to your cloud provider's IAM. The recommended pattern is **one bucket per permission level**:

| Bucket | Audience |
|--------|----------|
| `s3://company-public` | All employees |
| `s3://company-finance` | Finance team |
| `s3://company-executive` | C-level only |

Each user's cloud credentials determine which buckets they can see. If a user lacks IAM permission, `add-bucket` fails with "Access Denied" and no data is exposed. Lens needs only **read** access.

## Part of the DataSpoc Platform

| Layer | Role |
|-------|------|
| **DataSpoc Pipe** | Ingest data from APIs into cloud storage as Parquet |
| **DataSpoc Lens** | Query cloud Parquet with SQL, shell, notebooks, and AI |
| **DataSpoc ML** | Machine learning on your data lake (commercial) |

Pipe writes. Lens reads. ML learns.

## Community

- **Discord** -- [Join the conversation](#) for questions and discussion
- **GitHub Issues** -- [Report bugs or request features](https://github.com/dataspoclab/dataspoc-lens/issues)
- **Contributing** -- PRs welcome. Run `pytest tests/ -v` before submitting.

## License

[Apache-2.0](LICENSE)
