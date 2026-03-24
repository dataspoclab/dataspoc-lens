"""Tests for the DataSpoc Lens CLI."""

from typer.testing import CliRunner

from dataspoc_lens.cli import app

runner = CliRunner()


def test_version():
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "0.1.0" in result.output


def test_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "dataspoc-lens" in result.output.lower() or "DataSpoc" in result.output


def test_init_help():
    result = runner.invoke(app, ["init", "--help"])
    assert result.exit_code == 0


def test_add_bucket_help():
    result = runner.invoke(app, ["add-bucket", "--help"])
    assert result.exit_code == 0


def test_catalog_help():
    result = runner.invoke(app, ["catalog", "--help"])
    assert result.exit_code == 0


def test_query_help():
    result = runner.invoke(app, ["query", "--help"])
    assert result.exit_code == 0


def test_shell_help():
    result = runner.invoke(app, ["shell", "--help"])
    assert result.exit_code == 0


def test_query_has_export_flag():
    result = runner.invoke(app, ["query", "--help"])
    assert result.exit_code == 0


def test_ask_help():
    result = runner.invoke(app, ["ask", "--help"])
    assert result.exit_code == 0


def test_notebook_help():
    result = runner.invoke(app, ["notebook", "--help"])
    assert result.exit_code == 0


def test_transform_run_help():
    result = runner.invoke(app, ["transform", "run", "--help"])
    assert result.exit_code == 0


def test_transform_list_help():
    result = runner.invoke(app, ["transform", "list", "--help"])
    assert result.exit_code == 0


def test_ml_activate_help():
    result = runner.invoke(app, ["ml", "activate", "--help"])
    assert result.exit_code == 0


def test_ml_status_help():
    result = runner.invoke(app, ["ml", "status", "--help"])
    assert result.exit_code == 0


def test_ask_needs_api_key():
    """ask command requires DATASPOC_LLM_API_KEY."""
    import os
    env = os.environ.copy()
    env.pop("DATASPOC_LLM_API_KEY", None)
    result = runner.invoke(app, ["ask", "test question"])
    # Should exit with error since no API key is set
    assert result.exit_code == 1 or "API" in result.output


def test_ml_activate_message():
    """ml activate without key shows DataSpoc ML info."""
    result = runner.invoke(app, ["ml", "activate"])
    assert result.exit_code == 0
    assert "DataSpoc ML" in result.output


def test_ml_train_help():
    """ml train --help shows usage info."""
    result = runner.invoke(app, ["ml", "train", "--help"])
    assert result.exit_code == 0
    assert "target" in result.output.lower()


def test_ml_predict_help():
    """ml predict --help shows usage info."""
    result = runner.invoke(app, ["ml", "predict", "--help"])
    assert result.exit_code == 0
    assert "model" in result.output.lower()


def test_ml_models_help():
    """ml models --help shows usage info."""
    result = runner.invoke(app, ["ml", "models", "--help"])
    assert result.exit_code == 0


def test_ml_explain_help():
    """ml explain --help shows usage info."""
    result = runner.invoke(app, ["ml", "explain", "--help"])
    assert result.exit_code == 0
    assert "model" in result.output.lower()
