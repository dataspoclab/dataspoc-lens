"""Main CLI for DataSpoc Lens."""

from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from dataspoc_lens import __version__

console = Console()
app = typer.Typer(
    name="dataspoc-lens",
    help="DataSpoc Lens — Virtual warehouse. SQL over cloud Parquet via DuckDB.",
    no_args_is_help=True,
)

transform_app = typer.Typer(help="SQL transforms management.")
app.add_typer(transform_app, name="transform")

ml_app = typer.Typer(help="ML gateway management.")
app.add_typer(ml_app, name="ml")


def version_callback(value: bool) -> None:
    if value:
        console.print(f"dataspoc-lens {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        "-v",
        help="Show version.",
        callback=version_callback,
        is_eager=True,
    ),
) -> None:
    """DataSpoc Lens — Virtual warehouse over cloud Parquet."""


# ── Task 2: init, add-bucket, catalog ──────────────────────────────────


@app.command()
def init() -> None:
    """Initialize DataSpoc Lens configuration."""
    from dataspoc_lens.config import (
        CONFIG_FILE,
        DATASPOC_LENS_HOME,
        TRANSFORMS_DIR,
        LensConfig,
        save_config,
    )

    created = False
    for d in [DATASPOC_LENS_HOME, TRANSFORMS_DIR]:
        if not d.exists():
            d.mkdir(parents=True, exist_ok=True)
            created = True

    if not CONFIG_FILE.exists():
        save_config(LensConfig())
        created = True

    if created:
        console.print(f"[green]Initialized DataSpoc Lens in {DATASPOC_LENS_HOME}[/green]")
    else:
        console.print(f"[blue]Already initialized in {DATASPOC_LENS_HOME}[/blue]")


@app.command("add-bucket")
def add_bucket(
    uri: str = typer.Argument(..., help="Bucket URI (s3://, gs://, az://, file://)"),
) -> None:
    """Register a bucket and discover tables."""
    from dataspoc_lens.catalog import discover_tables
    from dataspoc_lens.config import load_config, save_config

    config = load_config()
    if uri in config.buckets:
        console.print(f"[yellow]Bucket already registered: {uri}[/yellow]")
    else:
        config.buckets.append(uri)
        save_config(config)
        console.print(f"[green]Bucket added: {uri}[/green]")

    console.print("Discovering tables...")
    tables = discover_tables(uri)

    if not tables:
        console.print("[yellow]No tables found in this bucket.[/yellow]")
        return

    table_view = Table(title=f"Tables in {uri}")
    table_view.add_column("Table", style="bold")
    table_view.add_column("Columns")
    table_view.add_column("Rows")
    table_view.add_column("Source")

    for t in tables:
        table_view.add_row(
            t.table,
            str(len(t.columns)),
            str(t.row_count),
            t.source,
        )

    console.print(table_view)
    console.print(f"\n[green]{len(tables)} table(s) found.[/green]")


@app.command()
def catalog(
    detail: str = typer.Option(None, "--detail", help="Show detailed schema for a table"),
) -> None:
    """List all tables from registered buckets."""
    from dataspoc_lens.catalog import discover_tables, get_table_columns, mount_views
    from dataspoc_lens.config import load_config
    from dataspoc_lens.shell import get_connection

    config = load_config()
    if not config.buckets:
        console.print("[yellow]No buckets registered. Use 'dataspoc-lens add-bucket' first.[/yellow]")
        return

    all_tables = []
    for bucket in config.buckets:
        tables = discover_tables(bucket)
        all_tables.extend(tables)

    if not all_tables:
        console.print("[yellow]No tables found in registered buckets.[/yellow]")
        return

    if detail:
        # Show detailed schema for a specific table
        conn = get_connection()
        mount_views(conn, all_tables)
        cols = get_table_columns(conn, detail)
        conn.close()
        if not cols:
            console.print(f"[red]Table '{detail}' not found.[/red]")
            return

        schema_table = Table(title=f"Schema: {detail}")
        schema_table.add_column("Column", style="bold")
        schema_table.add_column("Type")
        for c in cols:
            schema_table.add_row(c["column_name"], c["data_type"])
        console.print(schema_table)
        return

    table_view = Table(title="Catalog")
    table_view.add_column("Table", style="bold")
    table_view.add_column("Columns")
    table_view.add_column("Rows")
    table_view.add_column("Source")

    for t in all_tables:
        table_view.add_row(
            t.table,
            str(len(t.columns)),
            str(t.row_count),
            t.source,
        )

    console.print(table_view)


# ── Cache ──────────────────────────────────────────────────────────────


@app.command()
def cache(
    table: str = typer.Argument(None, help="Table name to cache"),
    list_cached: bool = typer.Option(False, "--list", help="List cached tables"),
    refresh: bool = typer.Option(False, "--refresh", help="Force re-download"),
    clear: bool = typer.Option(False, "--clear", help="Clear cached data"),
) -> None:
    """Manage local cache of remote Parquet data."""
    from dataspoc_lens.cache import cache_table, clear_cache, list_cached_tables
    from dataspoc_lens.catalog import discover_tables
    from dataspoc_lens.config import load_config

    if list_cached:
        cached = list_cached_tables()
        if not cached:
            console.print("[yellow]No cached tables.[/yellow]")
            return
        table_view = Table(title="Cached Tables")
        table_view.add_column("Table", style="bold")
        table_view.add_column("Cached At")
        table_view.add_column("Size")
        table_view.add_column("Status")
        for c in cached:
            size_mb = c["size_bytes"] / (1024 * 1024)
            table_view.add_row(
                c["table"],
                c["cached_at"][:19],
                f"{size_mb:.1f} MB",
                c["status"],
            )
        console.print(table_view)
        return

    if clear:
        cleared = clear_cache(table=table)
        if cleared:
            console.print(f"[green]Cleared cache: {', '.join(cleared)}[/green]")
        else:
            console.print("[yellow]Nothing to clear.[/yellow]")
        return

    if not table:
        console.print("[yellow]Provide a table name, or use --list / --clear.[/yellow]")
        raise typer.Exit(1)

    # Find the table URI
    config = load_config()
    all_tables = []
    for bucket in config.buckets:
        tables = discover_tables(bucket)
        all_tables.extend(tables)

    source_uri = None
    for t in all_tables:
        if t.table == table:
            source_uri = t.location
            break

    if not source_uri:
        console.print(f"[red]Table '{table}' not found in registered buckets.[/red]")
        raise typer.Exit(1)

    console.print(f"Caching '{table}'...")
    info = cache_table(table, source_uri, force=refresh)
    size_mb = info["size_bytes"] / (1024 * 1024)
    console.print(
        f"[green]Cached '{table}': {info['file_count']} file(s), "
        f"{size_mb:.1f} MB[/green]"
    )


# ── Task 3: query, shell ───────────────────────────────────────────────


@app.command()
def query(
    sql: str = typer.Argument(..., help="SQL query to execute"),
    export: str = typer.Option("", "--export", "-e", help="Export to file (format from extension: .csv, .json, .parquet)"),
) -> None:
    """Execute a SQL query and print results."""
    from dataspoc_lens.catalog import discover_tables, mount_views
    from dataspoc_lens.config import load_config
    from dataspoc_lens.shell import format_results, get_connection, run_query

    conn = get_connection()
    config = load_config()

    all_tables = []
    for bucket in config.buckets:
        tables = discover_tables(bucket)
        all_tables.extend(tables)

    mount_views(conn, all_tables)

    try:
        columns, rows, duration = run_query(conn, sql)
        console.print(format_results(columns, rows))
        console.print(f"\n({len(rows)} row(s), {duration:.3f}s)")

        if export:
            _export_results(conn, sql, export)
    except Exception as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    finally:
        conn.close()


@app.command()
def shell() -> None:
    """Launch interactive SQL shell."""
    from dataspoc_lens.catalog import discover_tables, mount_views
    from dataspoc_lens.config import load_config
    from dataspoc_lens.shell import Shell, get_connection

    conn = get_connection()
    config = load_config()

    all_tables = []
    for bucket in config.buckets:
        tables = discover_tables(bucket)
        all_tables.extend(tables)

    mount_views(conn, all_tables)

    s = Shell(conn, buckets=config.buckets)
    s.run()
    conn.close()


def _export_results(conn, sql: str, filepath: str) -> None:
    """Export query results to file. Format detected from extension."""
    from dataspoc_lens.export import export_csv, export_json, export_parquet

    ext = filepath.rsplit(".", 1)[-1].lower() if "." in filepath else "csv"
    try:
        if ext == "csv":
            count = export_csv(conn, sql, filepath)
        elif ext == "json":
            count = export_json(conn, sql, filepath)
        elif ext == "parquet":
            count = export_parquet(conn, sql, filepath)
        else:
            console.print(f"[red]Unknown extension .{ext}. Use .csv, .json, or .parquet[/red]")
            return
        console.print(f"[green]Exported {count} row(s) to {filepath}[/green]")
    except Exception as e:
        console.print(f"[red]Export error:[/red] {e}")


# ── Task 5: Transform subcommands ─────────────────────────────────────


@transform_app.command("run")
def transform_run() -> None:
    """Run SQL transforms in order."""
    from dataspoc_lens.catalog import discover_tables, mount_views
    from dataspoc_lens.config import load_config
    from dataspoc_lens.shell import get_connection
    from dataspoc_lens.transforms import run_all_transforms

    conn = get_connection()
    config = load_config()

    all_tables = []
    for bucket in config.buckets:
        tables = discover_tables(bucket)
        all_tables.extend(tables)

    mount_views(conn, all_tables)

    results = run_all_transforms(conn)
    conn.close()

    if not results:
        console.print("[yellow]No transform files found in ~/.dataspoc-lens/transforms/[/yellow]")
        return

    for filename, duration, status in results:
        if status == "OK" or status.startswith("OK"):
            console.print(f"Running {filename}... [green]{status}[/green] ({duration:.1f}s)")
        else:
            console.print(f"Running {filename}... [red]{status}[/red] ({duration:.1f}s)")
            raise typer.Exit(1)

    console.print(f"\n[green]{len(results)} transform(s) completed successfully.[/green]")


@transform_app.command("list")
def transform_list() -> None:
    """List available transform files."""
    from dataspoc_lens.transforms import discover_transforms

    files = discover_transforms()
    if not files:
        console.print("[yellow]No transform files found in ~/.dataspoc-lens/transforms/[/yellow]")
        return

    table_view = Table(title="Transforms")
    table_view.add_column("#", style="dim")
    table_view.add_column("File", style="bold")

    for i, f in enumerate(files, 1):
        table_view.add_row(str(i), f.name)

    console.print(table_view)


# ── Task 6: Notebook ─────────────────────────────────────────────────


@app.command()
def notebook(
    marimo: bool = typer.Option(False, "--marimo", help="Use Marimo instead of JupyterLab"),
) -> None:
    """Open interactive notebook with tables pre-mounted."""
    from dataspoc_lens.config import load_config

    config = load_config()

    if marimo:
        from dataspoc_lens.notebook import launch_marimo
        launch_marimo(config)
    else:
        from dataspoc_lens.notebook import launch_notebook
        launch_notebook(config)


# ── Task 7: AI Ask ───────────────────────────────────────────────────


@app.command(name="setup-ai")
def setup_ai() -> None:
    """Install and configure Ollama for local AI (free, no API key)."""
    from dataspoc_lens.ai import setup_ollama
    setup_ollama()


@app.command()
def ask(
    question: str = typer.Argument(..., help="Natural language question"),
    debug: bool = typer.Option(False, "--debug", help="Show prompt sent to LLM"),
    export: str = typer.Option("", "--export", "-e", help="Export results to file (.csv, .json, .parquet)"),
) -> None:
    """Ask a question in natural language and get SQL results."""
    import os

    from dataspoc_lens.catalog import discover_tables, mount_views
    from dataspoc_lens.config import load_config
    from dataspoc_lens.shell import format_results, get_connection

    # LLM config: env vars override config.yaml, default=ollama
    config = load_config()
    provider = os.environ.get("DATASPOC_LLM_PROVIDER", config.llm.provider)
    api_key = os.environ.get("DATASPOC_LLM_API_KEY", config.llm.api_key)
    model = os.environ.get("DATASPOC_LLM_MODEL", config.llm.model)

    # Cloud providers need API key
    if provider in ("anthropic", "openai") and not api_key:
        console.print(
            f"[yellow]Provider '{provider}' requires API key.[/yellow]\n\n"
            "Option 1 — Local AI (free, no API key):\n"
            "  [bold]dataspoc-lens setup-ai[/bold]\n\n"
            "Option 2 — Set API key:\n"
            f"  export DATASPOC_LLM_API_KEY=your-key\n\n"
            "Or add to ~/.dataspoc-lens/config.yaml:\n"
            "  llm:\n"
            f"    provider: {provider}\n"
            "    api_key: your-key"
        )
        raise typer.Exit(1)

    conn = get_connection()
    config = load_config()

    all_tables = []
    for bucket in config.buckets:
        tables = discover_tables(bucket)
        all_tables.extend(tables)

    mount_views(conn, all_tables)

    try:
        from dataspoc_lens.ai import ask as ai_ask

        result = ai_ask(conn, question, provider=provider, api_key=api_key, model=model, debug=debug)

        if debug:
            console.print("[dim]--- Prompt sent to LLM ---[/dim]")
            console.print(result["prompt"])
            console.print("[dim]--- End of prompt ---[/dim]\n")

        if result["sql"]:
            console.print(f"[bold]SQL:[/bold] {result['sql']}\n")

        if result["error"]:
            console.print(f"[red]{result['error']}[/red]")
            raise typer.Exit(1)

        if result["columns"]:
            console.print(format_results(result["columns"], result["rows"]))
            console.print(f"\n({len(result['rows'])} row(s), {result['duration']:.3f}s)")

            if export and result["sql"]:
                _export_results(conn, result["sql"], export)

    except ImportError:
        console.print(
            "[yellow]AI module not found. "
            "Install with: pip install dataspoc-lens[ai][/yellow]"
        )
        raise typer.Exit(1)
    finally:
        conn.close()


# ── Task 8: ML subcommands ───────────────────────────────────────────


def _call_ml(*args: str) -> int:
    """Call dataspoc-ml CLI as subprocess."""
    import shutil
    import subprocess

    ml_cmd = shutil.which("dataspoc-ml")
    if not ml_cmd:
        console.print("[red]dataspoc-ml not installed.[/red]")
        console.print("Install with: pip install dataspoc-ml")
        raise typer.Exit(1)
    result = subprocess.run([ml_cmd, *args], capture_output=False)
    return result.returncode


def _resolve_table_uri(table_name: str) -> str:
    """Resolve a table name to its bucket URI using the Lens catalog."""
    from dataspoc_lens.catalog import discover_tables
    from dataspoc_lens.config import load_config

    config = load_config()
    for bucket in config.buckets:
        tables = discover_tables(bucket)
        for t in tables:
            if t.table == table_name:
                return t.location

    console.print(f"[red]Table '{table_name}' not found in registered buckets.[/red]")
    raise typer.Exit(1)


def _get_first_bucket() -> str:
    """Get the first registered bucket URI."""
    from dataspoc_lens.config import load_config

    config = load_config()
    if not config.buckets:
        console.print("[red]No buckets registered. Use 'dataspoc-lens add-bucket' first.[/red]")
        raise typer.Exit(1)
    return config.buckets[0]


@ml_app.command("activate")
def ml_activate(
    key: str = typer.Argument(None, help="License key for DataSpoc ML"),
) -> None:
    """Activate DataSpoc ML with a license key."""
    if not key:
        console.print(
            "[bold]DataSpoc ML[/bold] is a commercial product of the DataSpoc platform.\n\n"
            "With DataSpoc ML you can:\n"
            "  - Train models directly on your bucket data\n"
            "  - Serve predictions via REST API\n"
            "  - Monitor drift and model performance\n\n"
            "To activate: dataspoc-lens ml activate <key>\n"
            "Learn more: [link]https://dataspoc.com/ml[/link]\n"
            "Sales contact: ml@dataspoc.com"
        )
        return
    rc = _call_ml("activate", key)
    if rc != 0:
        raise typer.Exit(rc)


@ml_app.command("status")
def ml_status() -> None:
    """Show DataSpoc ML license status."""
    rc = _call_ml("status")
    if rc != 0:
        raise typer.Exit(rc)


@ml_app.command("train")
def ml_train(
    target: str = typer.Option(..., "--target", help="Target column name"),
    source: str = typer.Option(..., "--from", help="Table name or path"),
) -> None:
    """Train a model via DataSpoc ML."""
    # Resolve table name to URI if it doesn't look like a path
    if not source.startswith(("/", "s3://", "gs://", "az://", "file://")):
        source = _resolve_table_uri(source)

    bucket = _get_first_bucket()
    rc = _call_ml("train", "--target", target, "--from", source, "--bucket", bucket)
    if rc != 0:
        raise typer.Exit(rc)


@ml_app.command("predict")
def ml_predict(
    model: str = typer.Option(..., "--model", help="Model name"),
    source: str = typer.Option(..., "--from", help="Table name or path"),
) -> None:
    """Generate predictions via DataSpoc ML."""
    if not source.startswith(("/", "s3://", "gs://", "az://", "file://")):
        source = _resolve_table_uri(source)

    rc = _call_ml("predict", "--model", model, "--from", source)
    if rc != 0:
        raise typer.Exit(rc)


@ml_app.command("models")
def ml_models() -> None:
    """List trained ML models."""
    bucket = _get_first_bucket()
    rc = _call_ml("models", "--bucket", bucket)
    if rc != 0:
        raise typer.Exit(rc)


@ml_app.command("explain")
def ml_explain(
    model: str = typer.Option(..., "--model", help="Model name"),
) -> None:
    """Explain a trained model via DataSpoc ML."""
    rc = _call_ml("explain", "--model", model)
    if rc != 0:
        raise typer.Exit(rc)
