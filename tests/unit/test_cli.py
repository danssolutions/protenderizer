import pytest
from click.testing import CliRunner
from analyzer.cli import cli


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
def test_logs_stub_output():
    """logs should return placeholder output."""
    runner = CliRunner()
    result = runner.invoke(cli, ["logs"])
    assert result.exit_code == 0
    assert "[logs]" in result.output


@pytest.mark.cli
def test_detect_outliers_stub_output():
    """detect-outliers should run and print placeholder."""
    runner = CliRunner()
    result = runner.invoke(cli, ["detect-outliers"])
    assert result.exit_code == 0
    assert "[detect-outliers]" in result.output


@pytest.mark.cli
def test_list_outliers_requires_input():
    """list-outliers fails if --input not provided."""
    runner = CliRunner()
    result = runner.invoke(cli, ["list-outliers"])
    assert result.exit_code != 0
    assert "Missing option '--input'" in result.output


@pytest.mark.cli
def test_list_outliers_stub_output():
    """list-outliers runs with required --input."""
    runner = CliRunner()
    result = runner.invoke(cli, ["list-outliers", "--input", "dummy.json"])
    assert result.exit_code == 0
    assert "[list-outliers]" in result.output
