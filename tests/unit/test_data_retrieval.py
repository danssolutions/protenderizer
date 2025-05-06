import json
import time
import pytest
import pandas as pd
import requests
from unittest.mock import patch
from analyzer import api

# region Basic Search Tests


@pytest.mark.basic
def test_search_success(requests_mock):
    """Test that a successful API call returns expected data."""
    sample_response = {"totalCount": 2, "results": [
        {"id": "1-2025"}, {"id": "2-2025"}]}
    url = "https://api.ted.europa.eu/v3/notices/search"
    requests_mock.post(url, json=sample_response, status_code=200)

    client = api.TEDAPIClient()
    result = client.search_notices(query="CPV=12345678", page=1, limit=2)

    assert result == sample_response
    sent_payload = requests_mock.last_request.json()
    assert sent_payload["query"] == "CPV=12345678"
    assert sent_payload["page"] == 1
    assert sent_payload["limit"] == 2
    assert sent_payload["scope"] == "ALL"
    assert sent_payload["paginationMode"] == "PAGE_NUMBER"
    assert "fields" in sent_payload and isinstance(
        sent_payload["fields"], list)


@pytest.mark.basic
def test_search_with_fields(requests_mock):
    """Test that specifying fields includes them in the request payload."""
    sample_response = {"totalCount": 1, "results": [
        {"id": "XYZ-2025", "title": "Test Notice"}]}
    url = "https://api.ted.europa.eu/v3/notices/search"
    requests_mock.post(url, json=sample_response, status_code=200)

    client = api.TEDAPIClient()
    fields = ["ID", "TITLE"]
    result = client.search_notices(query="abc", fields=fields, page=1, limit=1)

    assert result == sample_response
    sent_payload = requests_mock.last_request.json()
    assert sent_payload["fields"] == fields


@pytest.mark.basic
def test_search_iteration_mode(requests_mock):
    """Test using ITERATION pagination mode includes the token."""
    sample_response = {"totalCount": 0, "results": []}
    url = "https://api.ted.europa.eu/v3/notices/search"
    requests_mock.post(url, json=sample_response, status_code=200)

    client = api.TEDAPIClient()
    token = "TEST_TOKEN_123"
    result = client.search_notices(
        query="abc", page=1, limit=10, pagination_mode="ITERATION", iteration_token=token)

    assert result == sample_response
    sent_payload = requests_mock.last_request.json()
    assert sent_payload["paginationMode"] == "ITERATION"
    assert sent_payload["iterationNextToken"] == token
# endregion

# region Scroll Mode Tests


@pytest.mark.scroll
def test_fetch_all_scroll_multiple_pages_csv(tmp_path, requests_mock):
    """Test fetch_all_scroll saving to CSV incrementally."""
    url = "https://api.ted.europa.eu/v3/notices/search"
    output_file = tmp_path / "notices.csv"

    requests_mock.post(url, [
        {"json": {"notices": [{"publication-number": "PUB1"}],
                  "iterationNextToken": "TOKEN123"}, "status_code": 200},
        {"json": {"notices": [{"publication-number": "PUB2"}],
                  "iterationNextToken": "TOKEN456"}, "status_code": 200},
        {"json": {"notices": [], "iterationNextToken": "TOKEN_END"}, "status_code": 200},
    ])

    client = api.TEDAPIClient()

    with patch("time.sleep", return_value=None):
        results = client.fetch_all_scroll(
            query="test", limit=1, output_file=str(output_file), output_format="csv")

    assert len(results) == 2
    assert output_file.exists()

    df = pd.read_csv(output_file)
    assert set(df["publication-number"]) == {"PUB1", "PUB2"}


@pytest.mark.scroll
def test_fetch_all_scroll_multiple_pages_json(tmp_path, requests_mock):
    """Test fetch_all_scroll saving final result to JSON."""
    url = "https://api.ted.europa.eu/v3/notices/search"
    output_file = tmp_path / "notices.json"

    requests_mock.post(url, [
        {"json": {"notices": [{"publication-number": "PUB1"}],
                  "iterationNextToken": "TOKEN123"}, "status_code": 200},
        {"json": {"notices": [{"publication-number": "PUB2"}],
                  "iterationNextToken": "TOKEN456"}, "status_code": 200},
        {"json": {"notices": [], "iterationNextToken": "TOKEN_END"}, "status_code": 200},
    ])

    client = api.TEDAPIClient()

    with patch("time.sleep", return_value=None):
        results = client.fetch_all_scroll(
            query="test", limit=1, output_file=str(output_file), output_format="json")

    assert len(results) == 2
    assert output_file.exists()

    with open(output_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert set(n["publication-number"] for n in data) == {"PUB1", "PUB2"}


@pytest.mark.scroll
def test_fetch_all_scroll_checkpoint_resume_csv(tmp_path, requests_mock):
    """Test checkpoint resumption appending correctly."""
    url = "https://api.ted.europa.eu/v3/notices/search"
    checkpoint_file = tmp_path / "checkpoint.txt"
    output_file = tmp_path / "notices.csv"

    checkpoint_file.write_text("TOKEN123")

    requests_mock.post(url, [
        {"json": {"notices": [{"publication-number": "PUB2"}],
                  "iterationNextToken": "TOKEN456"}, "status_code": 200},
        {"json": {"notices": [], "iterationNextToken": "TOKEN_END"}, "status_code": 200},
    ])

    client = api.TEDAPIClient()

    with patch("time.sleep", return_value=None):
        results = client.fetch_all_scroll(query="test", limit=1,
                                          checkpoint_file=str(checkpoint_file),
                                          output_file=str(output_file),
                                          output_format="csv")

    assert len(results) == 1
    assert output_file.exists()

    df = pd.read_csv(output_file)
    assert df.iloc[0]["publication-number"] == "PUB2"
    assert not checkpoint_file.exists()
# endregion

# region Error Handling Tests


@pytest.mark.error_handling
def test_search_http_error_json(requests_mock):
    """Test that an HTTP error with a JSON body raises TEDAPIError."""
    url = "https://api.ted.europa.eu/v3/notices/search"
    requests_mock.post(
        url, json={"error": "Invalid query syntax"}, status_code=400)

    client = api.TEDAPIClient()
    with pytest.raises(api.TEDAPIError) as excinfo:
        client.search_notices(query="INVALID QUERY")
    assert "400" in str(excinfo.value)


@pytest.mark.error_handling
def test_search_http_error_text(requests_mock):
    """Test that an HTTP error with a plain text body raises TEDAPIError."""
    url = "https://api.ted.europa.eu/v3/notices/search"
    requests_mock.post(url, text="Service Unavailable", status_code=503)

    client = api.TEDAPIClient()
    with pytest.raises(api.TEDAPIError) as excinfo:
        client.search_notices(query="ANY")
    assert "503" in str(excinfo.value)


@pytest.mark.error_handling
def test_search_network_error(monkeypatch):
    """Test that a network error raises TEDAPIError."""
    client = api.TEDAPIClient()

    def fake_post(*args, **kwargs):
        raise requests.exceptions.ConnectTimeout("Connection timed out")
    monkeypatch.setattr(requests, "post", fake_post)

    with pytest.raises(api.TEDAPIError) as excinfo:
        client.search_notices(query="ANY")
    assert "Max retries exceeded" in str(excinfo.value)
# endregion

# region Retry and Rate Limit Tests


@pytest.mark.retry
def test_fetch_notices_exponential_retry(monkeypatch):
    """Test exponential backoff retry."""
    client = api.TEDAPIClient(max_retries=2, backoff_factor=0.5)
    calls = []

    def fake_post(*args, **kwargs):
        calls.append(time.time())
        raise requests.exceptions.ConnectTimeout("timeout!")
    monkeypatch.setattr(requests, "post", fake_post)

    with pytest.raises(api.TEDAPIError):
        client.search_notices(query="any")

    assert len(calls) == 3


@pytest.mark.rate_limit
def test_fetch_notices_rate_limit(monkeypatch):
    """Test that rate limiting enforces minimum interval."""
    client = api.TEDAPIClient(rate_limit_per_minute=60)
    times = []

    def fake_post(*args, **kwargs):
        times.append(time.time())

        class FakeResponse:
            ok = True
            def json(self): return {"notices": []}
        return FakeResponse()
    monkeypatch.setattr(requests, "post", fake_post)

    client.search_notices(query="test")
    client.search_notices(query="test")

    assert times[1] - times[0] >= 1.0


@pytest.mark.retry
def test_fetch_notices_retry_limit(monkeypatch):
    """Test that retries stop after max_retries."""
    client = api.TEDAPIClient(max_retries=2)

    def fake_post(*args, **kwargs):
        raise requests.exceptions.RequestException("fail")
    monkeypatch.setattr(requests, "post", fake_post)

    with pytest.raises(api.TEDAPIError) as e:
        client.search_notices(query="any")
    assert "Max retries exceeded" in str(e.value)
# endregion

# region Logging Tests


@pytest.mark.logging
def test_fetch_notices_log_success(tmp_path, requests_mock):
    """Test successful request logging."""
    log_file = tmp_path / "log_success.txt"
    client = api.TEDAPIClient(log_file=str(log_file))

    sample_response = {"notices": [{"id": "test"}]}
    requests_mock.post(
        "https://api.ted.europa.eu/v3/notices/search", json=sample_response)

    client.search_notices(query="test")

    logs = log_file.read_text()
    assert "SUCCESS" in logs


@pytest.mark.logging
def test_fetch_notices_log_append_only(tmp_path, requests_mock):
    """Test that logs are appended, not overwritten."""
    log_file = tmp_path / "log_append.txt"
    client = api.TEDAPIClient(log_file=str(log_file))

    requests_mock.post("https://api.ted.europa.eu/v3/notices/search",
                       json={"notices": [{"id": "test1"}]})
    client.search_notices(query="test")

    requests_mock.post("https://api.ted.europa.eu/v3/notices/search",
                       json={"notices": [{"id": "test2"}]})
    client.search_notices(query="test2")

    logs = log_file.read_text()
    assert logs.count("SUCCESS") == 2


@pytest.mark.logging
def test_fetch_notices_log_abnormal_response(tmp_path, requests_mock):
    """Test error logging when API returns error."""
    log_file = tmp_path / "log_error.txt"
    client = api.TEDAPIClient(log_file=str(log_file))

    requests_mock.post("https://api.ted.europa.eu/v3/notices/search",
                       status_code=503, text="Server Error")

    with pytest.raises(api.TEDAPIError):
        client.search_notices(query="test")

    logs = log_file.read_text()
    assert "ERROR" in logs and "503" in logs
# endregion
