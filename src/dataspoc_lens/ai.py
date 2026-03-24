"""AI Ask — Natural language to SQL via LLM API."""

from __future__ import annotations

import os
import re
from typing import Any

import duckdb

from dataspoc_lens.catalog import get_catalog_tables, get_table_columns


def build_schema_context(conn: duckdb.DuckDBPyConnection) -> str:
    """Build schema context as JSON for better LLM comprehension."""
    import json

    tables = get_catalog_tables(conn)
    schema = []

    for tbl in tables:
        table_name = tbl["table_name"]
        cols = get_table_columns(conn, table_name)

        table_info = {
            "table": table_name,
            "columns": [
                {"name": c["column_name"], "type": c["data_type"]}
                for c in cols
            ],
        }

        # Sample data (3 rows as list of dicts)
        try:
            result = conn.execute(f"SELECT * FROM {table_name} LIMIT 3")
            rows = result.fetchall()
            columns = [desc[0] for desc in result.description]
            if rows:
                table_info["sample"] = [
                    {col: str(val) for col, val in zip(columns, row)}
                    for row in rows
                ]
        except Exception:
            pass

        schema.append(table_info)

    return json.dumps(schema, indent=2, ensure_ascii=False)


def build_prompt(schema_context: str, question: str) -> str:
    """Build complete prompt for LLM with schema context and user question."""
    return (
        "You are a SQL assistant. The database is DuckDB.\n\n"
        "RULES:\n"
        '1. Column names MUST use double quotes exactly as shown: "Column Name"\n'
        "2. Use ONLY the exact column names from the JSON schema below\n"
        "3. Return ONLY the SQL query. No explanation.\n\n"
        "Database schema (JSON):\n"
        f"{schema_context}\n\n"
        f'Example: SELECT COUNT(DISTINCT "City (Billing)") FROM orders;\n\n'
        f"Question: {question}\n\n"
        "SQL:"
    )


def call_llm(prompt: str, provider: str = "anthropic", api_key: str = "", model: str = "") -> str:
    """Call LLM API and return response text.

    Providers: anthropic, openai, ollama (local).
    """
    if provider == "ollama":
        return _call_ollama(prompt, model or "duckdb-nsql:7b")

    elif provider == "anthropic":
        try:
            import anthropic
        except ImportError:
            raise ImportError(
                "Module 'anthropic' not found. "
                "Install with: pip install dataspoc-lens[ai]"
            )

        client = anthropic.Anthropic(api_key=api_key)
        message = client.messages.create(
            model=model or "claude-sonnet-4-20250514",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text

    elif provider == "openai":
        try:
            import openai
        except ImportError:
            raise ImportError(
                "Module 'openai' not found. "
                "Install with: pip install dataspoc-lens[ai]"
            )

        client = openai.OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=model or "gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=1024,
        )
        return response.choices[0].message.content

    else:
        raise ValueError(
            f"Unknown provider: {provider}. "
            "Use 'ollama' (local, free), 'anthropic', or 'openai'."
        )


def _call_ollama(prompt: str, model: str = "duckdb-nsql:7b") -> str:
    """Call local Ollama API (compatible with OpenAI format)."""
    import json
    import urllib.request

    ollama_url = os.environ.get("OLLAMA_HOST", "http://localhost:11434")

    payload = json.dumps({
        "model": model,
        "prompt": prompt,
        "stream": False,
    }).encode()

    req = urllib.request.Request(
        f"{ollama_url}/api/generate",
        data=payload,
        headers={"Content-Type": "application/json"},
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())
            return data.get("response", "")
    except urllib.error.URLError:
        raise ConnectionError(
            "Ollama not running. Start with:\n"
            "  ollama serve\n"
            "Or install with:\n"
            "  curl -fsSL https://ollama.com/install.sh | sh\n"
            "  ollama pull duckdb-nsql:7b"
        )


def setup_ollama() -> bool:
    """Check Ollama installation and download recommended model.

    Returns True if ready to use.
    """
    import shutil
    import subprocess

    # Check if ollama is installed
    ollama_cmd = shutil.which("ollama")
    if not ollama_cmd:
        print("Ollama not installed. Install with:")
        print("  curl -fsSL https://ollama.com/install.sh | sh")
        return False

    # Check if ollama is running
    try:
        import urllib.request
        urllib.request.urlopen("http://localhost:11434/api/tags", timeout=5)
    except Exception:
        print("Ollama not running. Start with:")
        print("  ollama serve")
        return False

    # Check if model is available
    try:
        import json
        import urllib.request
        with urllib.request.urlopen("http://localhost:11434/api/tags", timeout=5) as resp:
            data = json.loads(resp.read())
            models = [m["name"] for m in data.get("models", [])]
    except Exception:
        models = []

    recommended = "duckdb-nsql:7b"
    if recommended not in models and f"{recommended}:latest" not in models:
        # Check without tag
        has_model = any(recommended.split(":")[0] in m for m in models)
        if not has_model:
            print(f"Downloading {recommended} (~4 GB, one-time)...")
            subprocess.run([ollama_cmd, "pull", recommended], check=False)

    print("Ollama ready!")
    print(f"  Model: {recommended}")
    print(f"  Usage: dataspoc-lens ask 'your question here'")
    print(f"  Config: export DATASPOC_LLM_PROVIDER=ollama")
    return True


def extract_sql(response: str) -> str:
    """Extract SQL from LLM response.

    Handles:
    - ```sql ... ``` blocks
    - ``` ... ``` blocks
    - Raw SQL (no fences)
    """
    # Try ```sql ... ```
    match = re.search(r"```sql\s*\n?(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()

    # Try ``` ... ```
    match = re.search(r"```\s*\n?(.*?)```", response, re.DOTALL)
    if match:
        return match.group(1).strip()

    # Raw SQL — return as-is
    return response.strip()


def fix_column_names(sql: str, conn: duckdb.DuckDBPyConnection) -> str:
    """Try to fix unquoted column names in SQL by matching against actual columns.

    Local models often generate 'city' instead of '"City (Billing)"'.
    This function does a best-effort match.
    """
    # Get all real column names from all tables
    real_columns = {}  # lowercase_simple -> "Real Name"
    try:
        tables = get_catalog_tables(conn)
        for tbl in tables:
            cols = get_table_columns(conn, tbl["table_name"])
            for c in cols:
                name = c["column_name"]
                # Map simplified versions to real name
                simple = name.lower().replace(" ", "_").replace("(", "").replace(")", "").strip("_")
                real_columns[simple] = name
                # Also map just the first word
                first_word = name.split("(")[0].split(" ")[0].lower().strip()
                if first_word and first_word not in real_columns:
                    real_columns[first_word] = name
                # Map without parenthetical
                no_paren = re.sub(r'\s*\(.*?\)', '', name).strip().lower().replace(" ", "_")
                if no_paren:
                    real_columns[no_paren] = name
    except Exception:
        return sql

    if not real_columns:
        return sql

    # Find unquoted identifiers in SQL and try to match
    fixed = sql
    # Match word tokens that could be column names (not SQL keywords, not quoted)
    sql_keywords = {
        "select", "from", "where", "and", "or", "not", "in", "like", "between",
        "join", "left", "right", "inner", "outer", "on", "group", "by", "order",
        "asc", "desc", "having", "limit", "offset", "as", "distinct", "count",
        "sum", "avg", "min", "max", "case", "when", "then", "else", "end",
        "is", "null", "true", "false", "cast", "with", "union", "all", "exists",
    }

    for token_match in re.finditer(r'\b([a-zA-Z_]\w*)\b', sql):
        token = token_match.group(1)
        token_lower = token.lower()

        if token_lower in sql_keywords:
            continue
        # Skip table names
        if token_lower in {"orders", "customers", "products", "events"}:
            continue

        if token_lower in real_columns:
            real_name = real_columns[token_lower]
            if real_name != token:  # needs quoting
                fixed = fixed.replace(token, f'"{real_name}"', 1)

    return fixed


def ask(
    conn: duckdb.DuckDBPyConnection,
    question: str,
    provider: str = "anthropic",
    api_key: str = "",
    model: str = "",
    debug: bool = False,
) -> dict[str, Any]:
    """Full AI Ask flow: build context -> call LLM -> extract SQL -> execute.

    Providers: 'ollama' (local, free), 'anthropic', 'openai'.
    Returns dict with keys: prompt, response, sql, columns, rows, duration, error.
    """
    import time

    schema_context = build_schema_context(conn)
    prompt = build_prompt(schema_context, question)

    result: dict[str, Any] = {
        "prompt": prompt,
        "response": "",
        "sql": "",
        "columns": [],
        "rows": [],
        "duration": 0.0,
        "error": None,
    }

    try:
        response = call_llm(prompt, provider, api_key, model)
        result["response"] = response
    except Exception as e:
        result["error"] = str(e)
        return result

    sql = extract_sql(response)
    # Fix unquoted column names (common with local models)
    sql = fix_column_names(sql, conn)
    result["sql"] = sql

    start = time.time()
    try:
        query_result = conn.execute(sql)
        rows = query_result.fetchall()
        columns = [desc[0] for desc in query_result.description] if query_result.description else []
        result["columns"] = columns
        result["rows"] = rows
        result["duration"] = time.time() - start
    except Exception as e:
        result["duration"] = time.time() - start
        result["error"] = f"SQL execution error: {e}. Try rephrasing your question."

    return result
