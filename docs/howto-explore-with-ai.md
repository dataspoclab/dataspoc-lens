# How-To: Explore Data with AI (Natural Language to SQL)

Ask questions about your data in plain language. Lens converts to SQL and returns results.

> **Prerequisite:** Bucket connected. See [howto-query-s3-lake](howto-query-s3-lake.md) first.

---

## 1. Setup AI (one-time)

### Option A: Local AI (free, no API key)

```bash
# Install Ollama + download SQL-optimized model
dataspoc-lens setup-ai
```

This installs [Ollama](https://ollama.com) and downloads `duckdb-nsql:7b` (~4 GB, one-time).

### Option B: Cloud AI

```bash
export DATASPOC_LLM_PROVIDER=anthropic
export DATASPOC_LLM_API_KEY=sk-ant-...
```

## 2. Ask questions

```bash
dataspoc-lens ask "how many orders per city?"
dataspoc-lens ask "top 10 products by revenue"
dataspoc-lens ask "average order value by month"
```

Output:
```
SQL: SELECT "City (Billing)", COUNT(*) as n FROM orders GROUP BY 1 ORDER BY n DESC LIMIT 10

┌─────────────────┬──────┐
│ City (Billing)  │ n    │
├─────────────────┼──────┤
│ São Paulo       │ 1523 │
│ Rio de Janeiro  │ 987  │
│ ...             │      │
└─────────────────┴──────┘
(10 row(s), 0.234s)
```

## 3. Ask + export

```bash
dataspoc-lens ask "revenue by month this year" --export revenue.csv
dataspoc-lens ask "customers who bought more than 3 times" -e vip.parquet
```

## 4. Debug (see the prompt)

```bash
dataspoc-lens ask "total revenue" --debug
```

Shows the full prompt with table schemas and sample data sent to the AI.

## 5. Configure model

In `~/.dataspoc-lens/config.yaml`:

```yaml
llm:
  provider: ollama              # ollama (local), anthropic, openai
  model: duckdb-nsql:7b         # optimized for DuckDB SQL
  # model: qwen2.5-coder:1.5b   # lighter alternative (1 GB)
  # api_key: sk-...              # only for anthropic/openai
```

Or via env vars:
```bash
export DATASPOC_LLM_PROVIDER=ollama
export DATASPOC_LLM_MODEL=qwen2.5-coder:1.5b
```

## 6. Ask in the shell

```bash
dataspoc-lens shell
sql> ask how many orders were placed last month?
```

## 7. Tips

- **Be specific**: "revenue by product last quarter" > "show me revenue"
- **Use table names**: "how many rows in orders?" > "how many rows?"
- **Column names**: the AI knows your column names from the schema
- **Debug first**: if results are wrong, use `--debug` to check what the AI sees
- **Local models**: smaller models (1.5B) are faster but less accurate than 7B

---

## Related

- [Query S3 Lake](howto-query-s3-lake.md)
- [Pipe: Ingest data first](https://github.com/dataspoclab/dataspoc-pipe/blob/main/docs/howto-google-sheets-s3.md)
