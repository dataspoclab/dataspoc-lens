"""Interactive SQL shell with autocomplete and syntax highlighting."""

from __future__ import annotations

import time
from typing import Any

import duckdb
from tabulate import tabulate

SQL_KEYWORDS = [
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN",
    "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "ON",
    "GROUP", "BY", "ORDER", "ASC", "DESC", "HAVING", "LIMIT", "OFFSET",
    "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "CREATE",
    "TABLE", "VIEW", "DROP", "ALTER", "INDEX", "AS", "DISTINCT", "COUNT",
    "SUM", "AVG", "MIN", "MAX", "CASE", "WHEN", "THEN", "ELSE", "END",
    "UNION", "ALL", "EXISTS", "NULL", "IS", "TRUE", "FALSE", "CAST",
    "COALESCE", "NULLIF", "WITH", "RECURSIVE", "COPY", "TO", "FORMAT",
    "PARQUET", "CSV", "JSON", "DESCRIBE", "SHOW", "TABLES", "COLUMNS",
]


def get_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with httpfs loaded and cloud credentials configured."""
    import os

    conn = duckdb.connect()
    try:
        conn.execute("INSTALL httpfs; LOAD httpfs;")
    except Exception:
        try:
            conn.execute("LOAD httpfs;")
        except Exception:
            pass

    # Configure cloud credentials
    _configure_s3(conn)
    return conn


def _configure_s3(conn):
    """Configure S3 credentials in DuckDB from env vars or ~/.aws/credentials."""
    import os

    aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    aws_region = os.environ.get("AWS_DEFAULT_REGION", os.environ.get("AWS_REGION", "us-east-1"))

    if aws_key and aws_secret:
        conn.execute(f"SET s3_access_key_id='{aws_key}';")
        conn.execute(f"SET s3_secret_access_key='{aws_secret}';")
        conn.execute(f"SET s3_region='{aws_region}';")
        token = os.environ.get("AWS_SESSION_TOKEN", "")
        if token:
            conn.execute(f"SET s3_session_token='{token}';")
        return

    # Fallback: ~/.aws/credentials via botocore
    try:
        import botocore.session
        session = botocore.session.get_session()
        creds = session.get_credentials()
        if creds:
            resolved = creds.get_frozen_credentials()
            if resolved.access_key:
                conn.execute(f"SET s3_access_key_id='{resolved.access_key}';")
                conn.execute(f"SET s3_secret_access_key='{resolved.secret_key}';")
                conn.execute(f"SET s3_region='{aws_region}';")
                if resolved.token:
                    conn.execute(f"SET s3_session_token='{resolved.token}';")
    except ImportError:
        pass
    except Exception:
        pass


def run_query(
    conn: duckdb.DuckDBPyConnection, sql: str
) -> tuple[list[str], list[tuple], float]:
    """Execute SQL and return (columns, rows, duration_seconds)."""
    start = time.time()
    result = conn.execute(sql)
    rows = result.fetchall()
    duration = time.time() - start
    columns = [desc[0] for desc in result.description] if result.description else []
    return columns, rows, duration


def format_results(columns: list[str], rows: list[tuple], max_col_width: int = 40) -> str:
    """Format query results as an ASCII table via tabulate."""
    if not columns:
        return "(no results)"

    # Truncate wide columns
    truncated_rows = []
    for row in rows:
        new_row = []
        for val in row:
            s = str(val)
            if len(s) > max_col_width:
                s = s[: max_col_width - 3] + "..."
            new_row.append(s)
        truncated_rows.append(new_row)

    return tabulate(truncated_rows, headers=columns, tablefmt="psql")


def handle_dot_command(
    cmd: str,
    conn: duckdb.DuckDBPyConnection,
    buckets: list[str],
    last_result: tuple[list[str], list[tuple]] | None = None,
) -> str | None:
    """Handle dot commands. Returns output string or None to quit."""
    parts = cmd.strip().split(maxsplit=1)
    command = parts[0].lower()
    arg = parts[1] if len(parts) > 1 else ""

    if command in (".quit", ".exit"):
        return None  # signal to quit

    if command == ".tables":
        from dataspoc_lens.catalog import get_catalog_tables

        tables = get_catalog_tables(conn)
        if not tables:
            return "No tables found."
        lines = [f"  {t['table_name']} ({t['table_type']})" for t in tables]
        return "\n".join(lines)

    if command == ".schema":
        if not arg:
            return "Usage: .schema <table_name>"
        from dataspoc_lens.catalog import get_table_columns

        cols = get_table_columns(conn, arg.strip())
        if not cols:
            return f"Table '{arg.strip()}' not found."
        lines = [f"  {c['column_name']}  {c['data_type']}" for c in cols]
        return f"Table: {arg.strip()}\n" + "\n".join(lines)

    if command == ".buckets":
        if not buckets:
            return "No buckets registered."
        return "\n".join(f"  {b}" for b in buckets)

    if command == ".export":
        if not arg:
            return "Usage: .export <format> <output_path>"
        export_parts = arg.strip().split(maxsplit=1)
        if len(export_parts) < 2:
            return "Usage: .export <format> <output_path>"
        fmt, output_path = export_parts
        if last_result is None:
            return "No previous query result to export. Run a query first."
        from dataspoc_lens.export import export_from_result

        columns, rows = last_result
        count = export_from_result(columns, rows, fmt, output_path)
        return f"Exported {count} rows to {output_path} ({fmt})"

    if command == ".cache":
        from dataspoc_lens.cache import cache_table, list_cached_tables
        from dataspoc_lens.catalog import discover_tables

        if not arg:
            # No argument: list cached tables
            cached = list_cached_tables()
            if not cached:
                return "No cached tables. Use .cache <table> to cache a table."
            lines = []
            for c in cached:
                size_mb = c["size_bytes"] / (1024 * 1024)
                lines.append(
                    f"  {c['table']}  {c['cached_at'][:19]}  "
                    f"{size_mb:.1f}MB  [{c['status']}]"
                )
            return "Cached tables:\n" + "\n".join(lines)

        # Cache a specific table -- find its URI from discovered tables
        table_name = arg.strip()
        all_tables = []
        for bucket in buckets:
            all_tables.extend(discover_tables(bucket))

        source_uri = None
        for t in all_tables:
            if t.table == table_name:
                source_uri = t.location
                break

        if not source_uri:
            return f"Table '{table_name}' not found in registered buckets."

        info = cache_table(table_name, source_uri)
        size_mb = info["size_bytes"] / (1024 * 1024)
        return (
            f"Cached '{table_name}': {info['file_count']} file(s), "
            f"{size_mb:.1f}MB"
        )

    if command == ".help":
        return (
            "Dot commands:\n"
            "  .tables          List all tables\n"
            "  .schema <name>   Show table schema\n"
            "  .buckets         List registered buckets\n"
            "  .cache [table]   Cache a table locally (or list cached)\n"
            "  .export <fmt> <path>  Export last result (csv|json|parquet)\n"
            "  .help            Show this help\n"
            "  .quit / .exit    Exit shell"
        )

    return f"Unknown command: {command}. Type .help for available commands."


class Shell:
    """Interactive SQL shell."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, buckets: list[str] | None = None):
        self.conn = conn
        self.buckets = buckets or []
        self.last_result: tuple[list[str], list[tuple]] | None = None

    def _build_completer(self) -> Any:
        """Build a word completer from catalog + SQL keywords."""
        from prompt_toolkit.completion import WordCompleter

        from dataspoc_lens.catalog import get_catalog_tables, get_table_columns

        words = [kw.lower() for kw in SQL_KEYWORDS] + [kw.upper() for kw in SQL_KEYWORDS]

        tables = get_catalog_tables(self.conn)
        for t in tables:
            name = t["table_name"]
            words.append(name)
            cols = get_table_columns(self.conn, name)
            words.extend(c["column_name"] for c in cols)

        return WordCompleter(words, ignore_case=True)

    def run(self) -> None:
        """Launch the interactive REPL."""
        from prompt_toolkit import PromptSession
        from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
        from prompt_toolkit.history import FileHistory
        from prompt_toolkit.lexers import PygmentsLexer
        from pygments.lexers.sql import SqlLexer
        from rich.console import Console

        from dataspoc_lens.config import HISTORY_FILE

        console = Console()
        HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)

        completer = self._build_completer()
        session: PromptSession = PromptSession(
            history=FileHistory(str(HISTORY_FILE)),
            auto_suggest=AutoSuggestFromHistory(),
            lexer=PygmentsLexer(SqlLexer),
            completer=completer,
        )

        console.print("[bold]DataSpoc Lens Shell[/bold]")
        console.print("Type SQL or .help for commands. Ctrl+D or .quit to exit.\n")

        while True:
            try:
                text = session.prompt("lens> ").strip()
            except KeyboardInterrupt:
                continue
            except EOFError:
                break

            if not text:
                continue

            if text.startswith("."):
                output = handle_dot_command(text, self.conn, self.buckets, self.last_result)
                if output is None:
                    break
                console.print(output)
                continue

            # Handle 'ask' command (NL-to-SQL)
            if text.lower().startswith("ask "):
                question = text[4:].strip()
                if not question:
                    console.print("Usage: ask <your natural language question>")
                    continue
                self._handle_ask(question, console)
                continue

            try:
                columns, rows, duration = run_query(self.conn, text)
                self.last_result = (columns, rows)
                console.print(format_results(columns, rows))
                console.print(f"\n({len(rows)} row(s), {duration:.3f}s)")
            except KeyboardInterrupt:
                console.print("\nQuery cancelled.")
            except Exception as e:
                console.print(f"[red]Error:[/red] {e}")

    def _handle_ask(self, question: str, console: Any) -> None:
        """Handle 'ask' command in the shell — NL-to-SQL via LLM."""
        import os

        provider = os.environ.get("DATASPOC_LLM_PROVIDER", "anthropic")
        api_key = os.environ.get("DATASPOC_LLM_API_KEY", "")

        if not api_key:
            console.print(
                "[red]Set DATASPOC_LLM_API_KEY with your API key.[/red]\n"
                "Example: export DATASPOC_LLM_API_KEY=sk-..."
            )
            return

        try:
            from dataspoc_lens.ai import ask as ai_ask

            result = ai_ask(self.conn, question, provider=provider, api_key=api_key)

            if result["sql"]:
                console.print(f"[bold]SQL:[/bold] {result['sql']}\n")

            if result["error"]:
                console.print(f"[red]{result['error']}[/red]")
                return

            if result["columns"]:
                self.last_result = (result["columns"], result["rows"])
                console.print(format_results(result["columns"], result["rows"]))
                console.print(f"\n({len(result['rows'])} row(s), {result['duration']:.3f}s)")

        except ImportError:
            console.print(
                "[yellow]AI module not found. "
                "Install with: pip install dataspoc-lens[ai][/yellow]"
            )
