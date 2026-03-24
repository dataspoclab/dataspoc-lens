"""Notebook — JupyterLab launch with pre-mounted DuckDB tables."""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

from dataspoc_lens.config import LensConfig


def generate_startup_script(config: LensConfig) -> str:
    """Generate a Python startup script for IPython/JupyterLab.

    The script:
    - Creates a DuckDB connection
    - Mounts views from all registered buckets
    - Loads jupysql extension
    - Prints a welcome message
    """
    bucket_list = repr(config.buckets)

    script = textwrap.dedent(f"""\
        # DataSpoc Lens — JupyterLab startup script (auto-generated)
        import duckdb

        # Create DuckDB connection (in-memory, shared across notebook)
        conn = duckdb.connect()

        try:
            conn.execute("INSTALL httpfs; LOAD httpfs;")
        except Exception:
            try:
                conn.execute("LOAD httpfs;")
            except Exception:
                pass

        # Discover and mount tables from registered buckets
        from dataspoc_lens.catalog import discover_tables, mount_views

        _buckets = {bucket_list}
        _all_tables = []
        for _bucket in _buckets:
            _tables = discover_tables(_bucket)
            _all_tables.extend(_tables)

        mount_views(conn, _all_tables)

        _table_count = len(_all_tables)
        _table_names = [t.table for t in _all_tables]

        # Load jupysql and connect it to the SAME DuckDB connection (with views)
        try:
            get_ipython().run_line_magic("load_ext", "sql")
            # Pass the actual connection object so jupysql uses our views
            get_ipython().run_line_magic("sql", "conn --alias duckdb")
        except Exception:
            pass

        print()
        print("=" * 60)
        print("  DataSpoc Lens - Virtual Warehouse")
        print("=" * 60)
        print(f"  {{_table_count}} table(s) mounted:")
        for _tn in _table_names:
            print(f"    - {{_tn}}")
        print()
        print("  Usage:")
        print("    conn.sql('SELECT * FROM iris LIMIT 5').df()")
        print("    %sql SELECT * FROM iris LIMIT 5")
        print("    %%sql")
        print("    SELECT species, AVG(sepal_length) FROM iris GROUP BY species")
        print("=" * 60)
    """)

    return script


def launch_notebook(config: LensConfig) -> None:
    """Generate startup script and launch JupyterLab."""
    # Check if jupyterlab is installed
    try:
        import jupyterlab  # noqa: F401
    except ImportError:
        print("JupyterLab not found. Install with: pip install dataspoc-lens[jupyter]")
        return

    # Write startup script to IPython profile
    ipython_profile = Path.home() / ".ipython" / "profile_default" / "startup"
    ipython_profile.mkdir(parents=True, exist_ok=True)
    startup_file = ipython_profile / "00-dataspoc-lens.py"

    script = generate_startup_script(config)
    startup_file.write_text(script, encoding="utf-8")

    # Generate welcome notebook if it doesn't exist
    notebook_dir = Path.home() / "notebooks"
    notebook_dir.mkdir(parents=True, exist_ok=True)
    # Always regenerate the welcome notebook with current tables
    # User's own notebooks are preserved (they save with different names)
    welcome_nb = notebook_dir / "dataspoc-lens.ipynb"
    _create_welcome_notebook(welcome_nb, config)

    # Launch jupyter lab
    import os

    cmd = [
        sys.executable, "-m", "jupyter", "lab",
        "--port=8888",
        f"--notebook-dir={notebook_dir}",
        f"--LabApp.default_url=/lab/tree/dataspoc-lens.ipynb",
    ]

    # Allow root in Docker containers
    if os.getuid() == 0:
        cmd.append("--allow-root")

    subprocess.run(cmd, check=False)


def _create_welcome_notebook(path: Path, config: LensConfig) -> None:
    """Create a welcome notebook with examples pre-filled."""
    import json as json_mod

    from dataspoc_lens.catalog import discover_tables

    # Discover tables to customize the notebook
    table_names = []
    for bucket in config.buckets:
        try:
            tables = discover_tables(bucket)
            table_names.extend(t.table for t in tables)
        except Exception:
            pass

    first_table = table_names[0] if table_names else "my_table"
    table_list = ", ".join(table_names) if table_names else "(none found)"

    nb = {
        "nbformat": 4,
        "nbformat_minor": 5,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        },
        "cells": [
            {
                "id": "welcome",
                "cell_type": "markdown",
                "source": f"# DataSpoc Lens\n\n"
                    f"Your data lake tables are already mounted via `conn`.\n\n"
                    f"**Available tables:** {table_list}",
                "metadata": {},
            },
            {
                "id": "show-tables",
                "cell_type": "code",
                "source": "# Show available tables\nconn.sql('SHOW TABLES').show()",
                "metadata": {},
                "outputs": [],
                "execution_count": None,
            },
            {
                "id": "preview",
                "cell_type": "code",
                "source": f"# Data preview\nconn.sql('SELECT * FROM {first_table} LIMIT 10').show()",
                "metadata": {},
                "outputs": [],
                "execution_count": None,
            },
            {
                "id": "schema",
                "cell_type": "code",
                "source": f"# Table schema\nconn.sql('DESCRIBE {first_table}').show()",
                "metadata": {},
                "outputs": [],
                "execution_count": None,
            },
            {
                "id": "analysis",
                "cell_type": "markdown",
                "source": "## Your analysis\n\nWrite your queries below:",
                "metadata": {},
            },
            {
                "id": "query",
                "cell_type": "code",
                "source": f"# Your query here\nconn.sql('SELECT COUNT(*) as total FROM {first_table}').show()",
                "metadata": {},
                "outputs": [],
                "execution_count": None,
            },
            {
                "id": "dataframe",
                "cell_type": "code",
                "source": f"# Convert to DataFrame\ndf = conn.sql('SELECT * FROM {first_table}').df()\ndf.describe()",
                "metadata": {},
                "outputs": [],
                "execution_count": None,
            },
        ],
    }

    with open(path, "w") as f:
        json_mod.dump(nb, f, indent=2, ensure_ascii=False)


# ── Marimo ──────────────────────────────────────────────────────────


def launch_marimo(config: LensConfig) -> None:
    """Generate a Marimo notebook and launch it."""
    try:
        import marimo  # noqa: F401
    except ImportError:
        print("Marimo not found. Install with: pip install marimo")
        return

    notebook_dir = Path.home() / "notebooks"
    notebook_dir.mkdir(parents=True, exist_ok=True)
    marimo_file = notebook_dir / "dataspoc-lens.py"

    _create_marimo_notebook(marimo_file, config)

    print(f"Launching Marimo: {marimo_file}")
    subprocess.run(
        [sys.executable, "-m", "marimo", "edit", str(marimo_file), "--port=8889"],
        check=False,
    )


def _create_marimo_notebook(path: Path, config: LensConfig) -> None:
    """Create a Marimo notebook with DuckDB tables pre-mounted."""
    from dataspoc_lens.catalog import discover_tables

    table_names = []
    for bucket in config.buckets:
        try:
            tables = discover_tables(bucket)
            table_names.extend(t.table for t in tables)
        except Exception:
            pass

    first_table = table_names[0] if table_names else "my_table"
    bucket_list = repr(config.buckets)

    script = textwrap.dedent(f'''\
        import marimo

        __generated_with = "0.1.0"
        app = marimo.App(width="medium")


        @app.cell
        def setup():
            import marimo as mo
            from dataspoc_lens.connect import connect
            conn, table_names = connect()
            return conn, mo, table_names


        @app.cell
        def welcome(table_names, mo):
            tables_str = ", ".join(table_names)
            mo.md(f"# DataSpoc Lens\\n\\nTables from your data lake are ready to query.\\n\\n**Available tables:** {{tables_str}}")


        @app.cell
        def show_tables(conn):
            tables_df = conn.sql("SHOW TABLES").df()
            tables_df


        @app.cell
        def preview(conn):
            preview_df = conn.sql("SELECT * FROM {first_table} LIMIT 10").df()
            preview_df


        @app.cell
        def table_schema(conn):
            schema_df = conn.sql("DESCRIBE {first_table}").df()
            schema_df


        @app.cell
        def row_count(conn):
            count_df = conn.sql("SELECT COUNT(*) as total FROM {first_table}").df()
            count_df


        @app.cell
        def statistics(conn):
            stats_df = conn.sql("SELECT * FROM {first_table}").df().describe()
            stats_df


        @app.cell
        def your_query(conn):
            # Write your SQL here
            result = conn.sql("SELECT * FROM {first_table} LIMIT 10").df()
            result


        if __name__ == "__main__":
            app.run()
    ''')

    path.write_text(script, encoding="utf-8")


# ── Rill ────────────────────────────────────────────────────────────


def launch_rill(config: LensConfig) -> None:
    """Launch Rill Developer for visual data exploration."""
    import shutil

    rill_cmd = shutil.which("rill")
    if not rill_cmd:
        print("Rill not found. Install with:")
        print("  curl https://rill.sh | sh")
        print("  # or")
        print("  brew install rilldata/tap/rill")
        return

    # Rill works with local files — need to cache data first
    from dataspoc_lens.catalog import discover_tables

    project_dir = Path.home() / "notebooks" / "rill-project"
    project_dir.mkdir(parents=True, exist_ok=True)
    sources_dir = project_dir / "sources"
    sources_dir.mkdir(exist_ok=True)

    # Create rill.yaml
    rill_yaml = project_dir / "rill.yaml"
    if not rill_yaml.exists():
        rill_yaml.write_text("compiler: rill-beta\n")

    # Create Rill sources — use local cache if available, otherwise S3
    from dataspoc_lens.cache import get_cache_meta, get_local_cache_path, is_cache_fresh

    cache_meta = get_cache_meta()
    tables_found = []
    for bucket in config.buckets:
        try:
            tables = discover_tables(bucket)
            for t in tables:
                tables_found.append(t)
                source_yaml = sources_dir / f"{t.table}.yaml"

                # Prefer local cache (faster, no S3 egress)
                local_path = get_local_cache_path(t.table)
                if local_path and is_cache_fresh(t.table, cache_meta):
                    location = str(local_path)
                    print(f"  {t.table}: using local cache")
                else:
                    location = t.location.rstrip("/")
                    print(f"  {t.table}: using {location}")

                source_yaml.write_text(
                    f"connector: duckdb\n"
                    f'sql: "SELECT * FROM read_parquet(\'{location}/**/*.parquet\', '
                    f'hive_partitioning=true, union_by_name=true)"\n'
                )
        except Exception as e:
            print(f"Warning: failed to discover tables in {bucket}: {e}")

    if not tables_found:
        print("No tables found. Add a bucket first: dataspoc-lens add-bucket <uri>")
        return

    print(f"Rill project: {project_dir}")
    print(f"Tables: {', '.join(t.table for t in tables_found)}")
    print("Opening Rill Developer...")

    subprocess.run([rill_cmd, "start", str(project_dir)], check=False)
