"""Tests for AI Ask — schema context, prompt building, SQL extraction, mock LLM."""

from unittest.mock import MagicMock, patch

import duckdb
import pytest

from dataspoc_lens.ai import ask, build_prompt, build_schema_context, call_llm, extract_sql


@pytest.fixture
def conn_with_views():
    """DuckDB connection with sample views."""
    conn = duckdb.connect()
    conn.execute(
        "CREATE VIEW users AS SELECT * FROM "
        "(VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Carol', 35)) "
        "AS t(id, name, age)"
    )
    conn.execute(
        "CREATE VIEW orders AS SELECT * FROM "
        "(VALUES (10, 1, 99.9), (20, 2, 149.5)) "
        "AS t(order_id, user_id, total)"
    )
    yield conn
    conn.close()


def test_build_schema_context_produces_ddl(conn_with_views):
    """build_schema_context includes DDL for all views."""
    ctx = build_schema_context(conn_with_views)
    assert "users" in ctx
    assert "orders" in ctx
    assert "id" in ctx
    assert "name" in ctx
    assert "order_id" in ctx


def test_build_schema_context_includes_sample_data(conn_with_views):
    """build_schema_context includes sample data from views."""
    ctx = build_schema_context(conn_with_views)
    assert "Alice" in ctx
    assert "sample" in ctx


def test_build_prompt_includes_schema_and_question():
    """build_prompt combines schema context with user question."""
    schema = "CREATE VIEW users (id INTEGER, name VARCHAR)"
    question = "quantos usuarios existem?"
    prompt = build_prompt(schema, question)

    assert "users" in prompt
    assert "quantos usuarios existem?" in prompt
    assert "DuckDB" in prompt
    assert "SQL" in prompt


def test_extract_sql_fenced_block():
    """extract_sql extracts SQL from ```sql fenced blocks."""
    response = "Aqui esta o SQL:\n```sql\nSELECT COUNT(*) FROM users\n```\nPronto!"
    sql = extract_sql(response)
    assert sql == "SELECT COUNT(*) FROM users"


def test_extract_sql_generic_fence():
    """extract_sql extracts from ``` generic fenced blocks."""
    response = "```\nSELECT * FROM orders WHERE total > 100\n```"
    sql = extract_sql(response)
    assert sql == "SELECT * FROM orders WHERE total > 100"


def test_extract_sql_raw():
    """extract_sql returns raw text when no fences present."""
    response = "SELECT COUNT(*) FROM users"
    sql = extract_sql(response)
    assert sql == "SELECT COUNT(*) FROM users"


def test_extract_sql_multiline_fenced():
    """extract_sql handles multiline SQL in fenced block."""
    response = "```sql\nSELECT \n  id,\n  name\nFROM users\nWHERE age > 25\n```"
    sql = extract_sql(response)
    assert "SELECT" in sql
    assert "WHERE age > 25" in sql


def test_ask_full_flow_with_mock(conn_with_views):
    """Full ask flow with mocked LLM call."""
    mock_response = "```sql\nSELECT COUNT(*) AS total FROM users\n```"

    with patch("dataspoc_lens.ai.call_llm", return_value=mock_response):
        result = ask(
            conn_with_views,
            "quantos usuarios existem?",
            provider="anthropic",
            api_key="fake-key",
        )

    assert result["error"] is None
    assert result["sql"] == "SELECT COUNT(*) AS total FROM users"
    assert len(result["rows"]) == 1
    assert result["rows"][0][0] == 3  # 3 users
    assert result["columns"] == ["total"]


def test_ask_llm_error_handled(conn_with_views):
    """ask handles LLM API errors gracefully."""
    with patch("dataspoc_lens.ai.call_llm", side_effect=Exception("API timeout")):
        result = ask(
            conn_with_views,
            "test question",
            provider="anthropic",
            api_key="fake-key",
        )

    assert result["error"] is not None
    assert "API timeout" in result["error"]


def test_ask_bad_sql_from_llm(conn_with_views):
    """ask handles bad SQL returned by LLM."""
    mock_response = "SELECT * FROM nonexistent_table_xyz"

    with patch("dataspoc_lens.ai.call_llm", return_value=mock_response):
        result = ask(
            conn_with_views,
            "test",
            provider="anthropic",
            api_key="fake-key",
        )

    assert result["error"] is not None
    assert "Try rephrasing" in result["error"]


def test_call_llm_unsupported_provider():
    """call_llm raises for unsupported provider."""
    with pytest.raises(ValueError, match="Unknown provider"):
        call_llm("test prompt", provider="gemini", api_key="key")


def test_call_llm_missing_anthropic():
    """call_llm raises ImportError when anthropic not installed."""
    import builtins
    original_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "anthropic":
            raise ImportError("No module named 'anthropic'")
        return original_import(name, *args, **kwargs)

    with patch.object(builtins, "__import__", side_effect=mock_import):
        with pytest.raises(ImportError, match="pip install dataspoc-lens"):
            call_llm("prompt", provider="anthropic", api_key="key")


def test_call_llm_missing_openai():
    """call_llm raises ImportError when openai not installed."""
    import builtins
    original_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "openai":
            raise ImportError("No module named 'openai'")
        return original_import(name, *args, **kwargs)

    with patch.object(builtins, "__import__", side_effect=mock_import):
        with pytest.raises(ImportError, match="pip install dataspoc-lens"):
            call_llm("prompt", provider="openai", api_key="key")
