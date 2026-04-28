"""Example: Using the DataSpoc Lens Python SDK."""

from dataspoc_lens import LensClient

# Connect to your data lake (reads ~/.dataspoc-lens/config.yaml)
with LensClient() as client:
    # 1. Discover tables
    print("Tables:", client.tables())

    # 2. Inspect schema
    schema = client.schema("orders")
    for col in schema:
        print(f"  {col['column_name']}: {col['data_type']}")

    # 3. Run SQL
    result = client.query("SELECT status, COUNT(*) AS cnt FROM orders GROUP BY 1")
    print(f"\nQuery returned {result['row_count']} rows in {result['duration']:.3f}s")
    for row in result["rows"]:
        print(f"  {row}")

    # 4. Ask in natural language
    answer = client.ask("top 5 customers by total spending")
    print(f"\nSQL: {answer['sql']}")
    for row in answer["rows"]:
        print(f"  {row}")

    # 5. Cache management
    statuses = client.cache_status()
    for s in statuses:
        print(f"  {s['table']}: {s['status']}")

    # Refresh stale caches
    refreshed = client.cache_refresh_stale()
    if refreshed:
        print(f"Refreshed {len(refreshed)} table(s)")
