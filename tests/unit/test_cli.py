import pytest
from click.testing import CliRunner
from analyzer.cli import cli

# Dummy client for monkeypatching


class DummyClient:
    def __init__(self, *args, **kwargs):
        pass

    def build_query(self, start, end, filters=None):
        return "dummy_query"

    def fetch_all_scroll(self, *args, **kwargs):
        pass

    def search_notices(self, *args, **kwargs):
        return {"notices": [{"id": "TEST123"}]}

    def save_notices_as_csv(self, *args, **kwargs):
        pass

    def save_notices_as_json(self, *args, **kwargs):
        pass


@pytest.mark.cli
def test_cli_fetch_minimal(monkeypatch):
    """Test fetch command with minimal args."""
    runner = CliRunner()
    monkeypatch.setattr("analyzer.api.TEDAPIClient", DummyClient)

    result = runner.invoke(cli, [
        "fetch",
        "--start-date", "20240101",
        "--end-date", "20240131"
    ])

    assert result.exit_code == 0
    assert "Saved output to" in result.output


@pytest.mark.cli
def test_cli_fetch_invalid_missing_args():
    """Test fetch command fails if required args missing."""
    runner = CliRunner()
    result = runner.invoke(cli, ["fetch"])
    assert result.exit_code != 0
    assert "Missing option" in result.output


@pytest.mark.cli
def test_cli_sync_starts(monkeypatch):
    """Test sync command starts scheduler."""
    runner = CliRunner()

    # Patch start_scheduler
    monkeypatch.setattr("analyzer.sync.start_scheduler",
                        lambda **kwargs: print("[stub] start_scheduler called"))

    result = runner.invoke(cli, ["sync"])
    assert result.exit_code == 0
    assert "[stub] start_scheduler called" in result.output


@pytest.mark.cli
def test_cli_preprocess_stub():
    """Test preprocess command prints stub output."""
    runner = CliRunner()
    result = runner.invoke(cli, [
        "preprocess",
        "--input", "dummy_input.csv"
    ])
    assert result.exit_code == 0
    assert "[preprocess]" in result.output


@pytest.mark.cli
def test_cli_logs_stub():
    """Test logs command prints stub output."""
    runner = CliRunner()
    result = runner.invoke(cli, ["logs"])
    assert result.exit_code == 0
    assert "[logs]" in result.output


@pytest.mark.cli
def test_cli_detect_outliers_stub():
    """Test detect-outliers command prints stub output."""
    runner = CliRunner()
    result = runner.invoke(cli, ["detect-outliers"])
    assert result.exit_code == 0
    assert "[detect-outliers]" in result.output


@pytest.mark.cli
def test_cli_list_outliers_stub():
    """Test list-outliers command prints stub output."""
    runner = CliRunner()
    result = runner.invoke(cli, [
        "list-outliers",
        "--input", "dummy_outliers.json"
    ])
    assert result.exit_code == 0
    assert "[list-outliers]" in result.output
