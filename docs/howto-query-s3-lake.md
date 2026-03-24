# How-To: Query a Data Lake on S3

Connect to an S3 bucket with Parquet data and start querying with SQL.

> **Prerequisite:** Data in the bucket. If you need to ingest data first, see [DataSpoc Pipe: howto-google-sheets-s3](https://github.com/dataspoclab/dataspoc-pipe/blob/main/docs/howto-google-sheets-s3.md).

---

## 1. Install

```bash
pip install dataspoc-lens[s3]
```

<details><summary>Install from source (development)</summary>

```bash
git clone https://github.com/dataspoclab/dataspoc-lens.git
cd dataspoc-lens
uv venv .venv && source .venv/bin/activate
uv pip install -e ".[s3]"
```

</details>

Verify:
```bash
dataspoc-lens --version
```

## 2. AWS credentials

Lens needs **read** access to the bucket.

```bash
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"
export AWS_DEFAULT_REGION="us-east-1"
```

Or `~/.aws/credentials`.

## 3. Connect

```bash
dataspoc-lens init
dataspoc-lens add-bucket s3://your-bucket
dataspoc-lens catalog
```

## 4. Query

```bash
# CLI
dataspoc-lens query "SELECT * FROM orders LIMIT 10"

# Query + export
dataspoc-lens query "SELECT * FROM orders" --export orders.csv
```

## 5. Interactive shell

```bash
dataspoc-lens shell
```

```
sql> .tables
sql> .schema orders
sql> SELECT COUNT(*) FROM orders;
sql> .quit
```

## 6. Cache locally

```bash
dataspoc-lens cache orders          # download to local
dataspoc-lens cache --list          # check status
dataspoc-lens cache orders --refresh
```

## 7. Notebook

```bash
# Jupyter
pip install dataspoc-lens[s3,jupyter]
dataspoc-lens notebook

# Marimo (reactive)
pip install dataspoc-lens[s3,marimo]
dataspoc-lens notebook --marimo
```

## 8. AI (natural language)

```bash
dataspoc-lens setup-ai
dataspoc-lens ask "how many orders per city?"
dataspoc-lens ask "top products by revenue" --export top.csv
```

## 9. Transforms (gold layer)

Create SQL transforms for analyst-level aggregations:

```bash
mkdir -p ~/.dataspoc-lens/transforms

cat > ~/.dataspoc-lens/transforms/001_monthly_revenue.sql << 'EOF'
COPY (
    SELECT DATE_TRUNC('month', order_date) AS month,
           SUM(total) AS revenue,
           COUNT(*) AS orders
    FROM orders
    GROUP BY 1 ORDER BY 1 DESC
) TO 's3://your-bucket/gold/revenue/monthly.parquet' (FORMAT PARQUET);
EOF

dataspoc-lens transform run
```

---

## Related

- [Pipe: Ingest Google Sheets → S3](https://github.com/dataspoclab/dataspoc-pipe/blob/main/docs/howto-google-sheets-s3.md)
- [Pipe: Multi-Stage Lake (Bronze → Silver → Gold)](https://github.com/dataspoclab/dataspoc-pipe/blob/main/docs/howto-multi-stage-lake.md)
