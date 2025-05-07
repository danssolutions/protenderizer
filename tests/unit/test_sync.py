import pytest
import pandas as pd
import json
from analyzer import sync
from unittest.mock import patch


@pytest.mark.sync
def test_sync_once_success_csv(tmp_path, requests_mock):
    """Test successful sync_once saving data to CSV."""
    output_file = tmp_path / "notices_sync.csv"
    last_sync_file = tmp_path / ".last_sync"

    url = "https://api.ted.europa.eu/v3/notices/search"
    requests_mock.post(
        url, json={
            "notices": [{"publication-number": "PUB1"}],
            "iterationNextToken": "dummy_token_1"   # <-- ADD THIS!
        }, status_code=200)

    with patch("time.sleep", return_value=None):
        sync.sync_once(
            start_days_ago=7,
            filters=None,
            output_file=str(output_file),
            output_format="csv",
            last_sync_file=str(last_sync_file)
        )

    assert output_file.exists()
    assert last_sync_file.exists()

    saved_timestamp = last_sync_file.read_text(encoding="utf-8").strip()
    from datetime import datetime, timezone
    expected_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    assert saved_timestamp == expected_timestamp

    df = pd.read_csv(output_file)
    assert "publication-number" in df.columns
    assert set(df["publication-number"]) == {"PUB1"}


@pytest.mark.sync
def test_sync_once_success_json(tmp_path, requests_mock):
    """Test successful sync_once saving data to JSON."""
    output_file = tmp_path / "notices_sync.json"
    last_sync_file = tmp_path / ".last_sync"

    url = "https://api.ted.europa.eu/v3/notices/search"
    requests_mock.post(
        url, json={
            "notices": [{"publication-number": "PUB1"}],
            "iterationNextToken": "dummy_token_1"
        }, status_code=200)

    with patch("time.sleep", return_value=None):
        sync.sync_once(
            start_days_ago=7,
            filters=None,
            output_file=str(output_file),
            output_format="json",
            last_sync_file=str(last_sync_file)
        )

    assert output_file.exists()
    assert last_sync_file.exists()

    saved_timestamp = last_sync_file.read_text(encoding="utf-8").strip()
    from datetime import datetime, timezone
    expected_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    assert saved_timestamp == expected_timestamp, f"Expected {expected_timestamp}, got {saved_timestamp}"

    with open(output_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert isinstance(data, list)
    assert any(n["publication-number"] == "PUB1" for n in data)


@pytest.mark.sync
def test_sync_once_api_failure(tmp_path, requests_mock):
    """Test sync_once handles API failure cleanly."""
    output_file = tmp_path / "notices_sync.csv"
    last_sync_file = tmp_path / ".last_sync"

    url = "https://api.ted.europa.eu/v3/notices/search"
    requests_mock.post(url, status_code=503, text="Service Unavailable")

    with patch("time.sleep", return_value=None):
        sync.sync_once(
            start_days_ago=7,
            filters=None,
            output_file=str(output_file),
            output_format="csv",
            last_sync_file=str(last_sync_file)
        )

    # Even if sync fails, files should not exist
    assert not output_file.exists()
    assert not last_sync_file.exists()
