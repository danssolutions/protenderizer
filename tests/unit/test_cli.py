import pytest
from click.testing import CliRunner
from analyzer.cli import cli
import pandas as pd


@pytest.mark.cli
def test_fetch_missing_required_arg():
    """fetch fails without required start/end dates."""
    runner = CliRunner()
    result = runner.invoke(cli, ["fetch"])
    assert result.exit_code != 0
    assert "Missing option" in result.output


@pytest.mark.cli
def test_sync_command_invokes_scheduler(monkeypatch):
    """sync should call start_scheduler()."""
    runner = CliRunner()
    monkeypatch.setattr("analyzer.sync.start_scheduler",
                        lambda **kwargs: print("[stub] scheduler launched"))

    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0
    assert "[stub] scheduler launched" in result.output


@pytest.mark.cli
def test_detect_outliers_fails_with_too_little_data(monkeypatch):
    """detect-outliers should fail gracefully with insufficient data."""
    runner = CliRunner()

    # Simulate minimal data
    monkeypatch.setattr("analyzer.arima.prepare_monthly_counts",
                        lambda df: pd.Series([1, 2], index=pd.date_range("2020-01-01", periods=2, freq="MS")))

    # Simulate ARIMA error
    monkeypatch.setattr("analyzer.arima.train_and_forecast_arima",
                        lambda series, **kwargs: (_ for _ in ()).throw(ValueError("Insufficient data for training")))

    # Bypass database access and simulate chunked iterator
    monkeypatch.setattr("pandas.read_sql_table",
                        lambda table, con, **kwargs: iter([
                            pd.DataFrame({"publication-date": ["2020-01-01"]}),
                            pd.DataFrame({"publication-date": ["2020-02-01"]})
                        ])
                        )

    result = runner.invoke(cli, [
        "detect-outliers",
        "--output", "csv"
    ])

    assert result.exit_code != 0
    assert "Insufficient data" in result.output


@pytest.mark.cli
def test_list_outliers_requires_input():
    """list-outliers fails if --input not provided."""
    runner = CliRunner()
    result = runner.invoke(cli, ["list-outliers"])
    assert result.exit_code != 0
    assert "Missing option '--input'" in result.output


@pytest.mark.cli
def test_list_outliers_file_not_found():
    """list-outliers should show error message if file doesn't exist."""
    runner = CliRunner()
    result = runner.invoke(cli, [
        "list-outliers",
        "--input", "nonexistent.json"
    ])
    assert result.exit_code == 0
    assert "not found" in result.output
