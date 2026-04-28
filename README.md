<h1 align="center">DataSpoc Lens</h1>

<p align="center">
  <a href="https://github.com/dataspoclab/dataspoc-lens/actions"><img src="https://img.shields.io/github/actions/workflow/status/dataspoclab/dataspoc-lens/ci.yml?branch=main&style=flat-square&label=CI" alt="CI"></a>
  <a href="https://pypi.org/project/dataspoc-lens/"><img src="https://img.shields.io/pypi/v/dataspoc-lens?style=flat-square" alt="PyPI"></a>
  <a href="https://github.com/dataspoclab/dataspoc-lens/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square" alt="License"></a>
  <a href="https://pypi.org/project/dataspoc-lens/"><img src="https://img.shields.io/pypi/pyversions/dataspoc-lens?style=flat-square" alt="Python 3.10+"></a>
</p>

<p align="center"><em>SQL over cloud Parquet. Query your data lake from the terminal.</em></p>

## Why Lens?

Data teams store Parquet in S3, GCS, or Azure but still spin up heavy warehouses just to run SQL. **DataSpoc Lens** mounts cloud buckets as DuckDB views and gives you an interactive shell, notebooks, AI-powered queries, and local caching -- all from a single CLI. No servers, no infrastructure, no data copying.

## Installation

```bash
pip install dataspoc-lens
```

Cloud and feature extras:

```bash
pip install dataspoc-lens[s3]       # AWS S3
pip install dataspoc-lens[gcs]      # Google Cloud Storage
pip install dataspoc-lens[azure]    # Azure Blob Storage
pip install dataspoc-lens[jupyter]  # JupyterLab integration
pip install dataspoc-lens[ai]       # AI natural language queries
pip install dataspoc-lens[all]      # Everything
```

## Quick Start

### 1. Initialize and register a bucket

```bash
dataspoc-lens init
dataspoc-lens add-bucket s3://my-data-lake
```

Lens discovers tables automatically -- first from Pipe's `.dataspoc/manifest.json`, then by scanning for `*.parquet` files.

### 2. Explore the catalog

```bash
dataspoc-lens catalog
dataspoc-lens catalog --detail orders
```

### 3. Query with SQL

```bash
dataspoc-lens query "SELECT * FROM orders LIMIT 10"
dataspoc-lens query "SELECT status, COUNT(*) FROM orders GROUP BY status"
```

### 4. Launch the interactive shell

```bash
dataspoc-lens shell
```

```
lens> SELECT customer_id, SUM(total) FROM orders GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
lens> .tables
lens> .schema orders
lens> .export csv /tmp/orders.csv
lens> .quit
```

### 5. Configure AI and ask questions

Before using `ask`, configure an LLM provider:

**Option A -- Local AI (free, no API key):**

```bash
dataspoc-lens setup-ai
```

**Option B -- Cloud provider:**

```bash
# Anthropic (default)
export DATASPOC_LLM_API_KEY=sk-ant-...

# OpenAI
export DATASPOC_LLM_PROVIDER=openai
export DATASPOC_LLM_API_KEY=sk-...
```

Then ask questions in natural language:

```bash
dataspoc-lens ask "how many orders were placed yesterday?"
dataspoc-lens ask "top 10 customers by revenue this month"
dataspoc-lens ask --debug "average order value by month"
```

Lens sends your table schemas and sample data to the LLM, receives SQL, executes it, and prints the results. Use `--debug` to see the full prompt sent to the LLM.

### 6. Export results

Add `--export` to any `query` or `ask` command. Format is detected from the file extension:

```bash
dataspoc-lens query "SELECT * FROM orders" --export orders.csv
dataspoc-lens query "SELECT * FROM users" --export users.parquet
dataspoc-lens ask "monthly revenue" --export revenue.json
```

## Features

### Interactive Shell

SQL REPL with syntax highlighting, autocomplete, and history. Dot commands: `.tables`, `.schema <table>`, `.buckets`, `.cache <table>`, `.export <format> <path>`, `.help`, `.quit`.

### Notebook

Launch JupyterLab or Marimo with all tables pre-mounted:

```bash
pip install dataspoc-lens[jupyter]
dataspoc-lens notebook

pip install dataspoc-lens[marimo]
dataspoc-lens notebook --marimo
```

### SQL Transforms

Numbered `.sql` files in `~/.dataspoc-lens/transforms/` that run in order:

```bash
dataspoc-lens transform list
dataspoc-lens transform run
```

### Cache

Copy tables locally for offline work and reduced egress costs:

```bash
dataspoc-lens cache orders              # Cache a table
dataspoc-lens cache --list              # Check status (fresh/stale)
dataspoc-lens cache orders --refresh    # Re-download
dataspoc-lens cache --clear             # Clear all
```

Freshness: compares your cache timestamp against the manifest's `last_extraction`.

## AI Agent Integration

Lens works as an MCP server for Claude Desktop, Claude Code, Cursor, and any MCP-compatible AI agent.

```bash
pip install dataspoc-lens[mcp]
dataspoc-lens mcp                           # Start MCP server (stdio)
```

Add to your Claude Desktop config (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "dataspoc-lens": {
      "command": "dataspoc-lens",
      "args": ["mcp"]
    }
  }
}
```

Your agent can now discover tables, run SQL, ask questions in natural language, and manage cache.

### Python SDK

```python
from dataspoc_lens import LensClient

with LensClient() as client:
    tables = client.tables()
    schema = client.schema("orders")
    result = client.query("SELECT status, COUNT(*) FROM orders GROUP BY 1")
    answer = client.ask("top 10 customers by revenue")
    stale = client.cache_refresh_stale()
```

### JSON Output

All CLI commands support `--output json` for machine-readable output:

```bash
dataspoc-lens catalog --output json
dataspoc-lens query "SELECT * FROM orders LIMIT 5" --output json
dataspoc-lens ask "monthly revenue" --output json
```

## Commands

```bash
dataspoc-lens init                          # Initialize configuration
dataspoc-lens add-bucket <uri>              # Register a bucket
dataspoc-lens catalog                       # List all tables
dataspoc-lens catalog --detail <table>      # Show table schema
dataspoc-lens query "<sql>"                 # Execute SQL query
dataspoc-lens query "<sql>" --export f.csv  # Execute and export
dataspoc-lens shell                         # Interactive SQL shell
dataspoc-lens ask "<question>"              # Natural language query
dataspoc-lens ask "<question>" --debug      # Show LLM prompt
dataspoc-lens setup-ai                      # Install local AI (Ollama)
dataspoc-lens notebook                      # Launch JupyterLab
dataspoc-lens notebook --marimo             # Launch Marimo
dataspoc-lens transform list                # List transform files
dataspoc-lens transform run                 # Run all transforms
dataspoc-lens cache <table>                 # Cache a table locally
dataspoc-lens cache --list                  # List cached tables
dataspoc-lens cache --clear                 # Clear cache
dataspoc-lens mcp                           # Start MCP server for AI agents
dataspoc-lens ml activate [key]             # Activate DataSpoc ML
dataspoc-lens ml train --target col --from tbl  # Train a model
dataspoc-lens ml predict --model m --from tbl   # Generate predictions
dataspoc-lens ml models                     # List trained models
dataspoc-lens --version                     # Show version
```

## Part of the DataSpoc Platform

| Product | Role |
|---------|------|
| **[DataSpoc Pipe](https://github.com/dataspoclab/dataspoc-pipe)** | Ingestion: Singer taps to Parquet in cloud buckets |
| **[DataSpoc Lens](https://github.com/dataspoclab/dataspoc-lens)** (this) | Virtual warehouse: SQL + Jupyter + AI over your data lake |
| **DataSpoc ML** | AutoML: train and deploy models from your lake |

Pipe writes. Lens reads. ML learns.

## Community

- **GitHub Issues** -- [Report bugs or request features](https://github.com/dataspoclab/dataspoc-lens/issues)
- **Contributing** -- PRs welcome. Run `pytest tests/ -v` before submitting.

## License

[Apache-2.0](LICENSE) -- free to use, modify, and distribute.
