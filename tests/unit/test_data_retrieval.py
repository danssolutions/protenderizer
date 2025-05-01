import os
import pytest
import requests
import time
import json
import pandas as pd
from unittest.mock import patch
from analyzer import api

# region Basic Search Tests
def test_search_success(requests_mock):
    """Test that a successful API call returns the expected data."""
    sample_response = {"totalCount": 2, "results": [{"id": "1-2025"}, {"id": "2-2025"}]}
    # Prepare the mock to intercept the POST request
    url = "https://api.ted.europa.eu/v3/notices/search"
    requests_mock.post(url, json=sample_response, status_code=200)
    client = api.TEDAPIClient()
    result = client.search_notices(query="CPV=12345678", page=1, limit=2)
    # Verify that the response data is returned correctly
    assert result == sample_response
    # Verify that the request was made with the correct payload
    sent_payload = requests_mock.last_request.json()
    assert sent_payload["query"] == "CPV=12345678"
    assert sent_payload["page"] == 1
    assert sent_payload["limit"] == 2
    assert sent_payload["scope"] == "ALL"
    assert sent_payload["paginationMode"] == "PAGE_NUMBER"
    # 'fields' should be in the payload even if not provided, because client adds default fields
    assert "fields" in sent_payload
    assert isinstance(sent_payload["fields"], list)
    assert len(sent_payload["fields"]) > 0

def test_search_with_fields(requests_mock):
    """Test that specifying fields includes them in the request payload."""
    sample_response = {"totalCount": 1, "results": [{"id": "XYZ-2025", "title": "Test Notice"}]}
    url = "https://api.ted.europa.eu/v3/notices/search"
    requests_mock.post(url, json=sample_response, status_code=200)
    client = api.TEDAPIClient()
    fields = ["ID", "TITLE"]
    result = client.search_notices(query="abc", fields=fields, page=1, limit=1, scope="ALL")
    # The result should match the mocked response
    assert result == sample_response
    # The last request payload should include the fields list
    sent_payload = requests_mock.last_request.json()
    assert sent_payload["fields"] == fields
    assert sent_payload["query"] == "abc"
    assert sent_payload["page"] == 1
    assert sent_payload["limit"] == 1
    assert sent_payload["scope"] == "ALL"

def test_search_iteration_mode(requests_mock):
    """Test that using iteration mode includes the token and mode in the payload."""
    sample_response = {"totalCount": 0, "results": []}
    url = "https://api.ted.europa.eu/v3/notices/search"
    requests_mock.post(url, json=sample_response, status_code=200)
    client = api.TEDAPIClient()
    token = "TEST_TOKEN_123"
    result = client.search_notices(query="abc", page=1, limit=10, pagination_mode="ITERATION", iteration_token=token)
    # The result should be returned (empty in this case)
    assert result == sample_response
    # Check that the request payload has the correct mode and token
    sent_payload = requests_mock.last_request.json()
    assert sent_payload["paginationMode"] == "ITERATION"
    assert sent_payload["iterationNextToken"] == token

def test_fetch_all_scroll_multiple_pages_csv(tmp_path, requests_mock):
    """Test fetch_all_scroll saving to CSV incrementally."""
    url = "https://api.ted.europa.eu/v3/notices/search"
    output_file = tmp_path / "notices.csv"

    # Simulate multiple scroll batches
    requests_mock.post(url, [
        {"json": {"notices": [{"publication-number": "PUB1"}], "iterationNextToken": "TOKEN123"}, "status_code": 200},
        {"json": {"notices": [{"publication-number": "PUB2"}], "iterationNextToken": "TOKEN456"}, "status_code": 200},
        {"json": {"notices": [], "iterationNextToken": "TOKEN_END"}, "status_code": 200},
    ])

    client = api.TEDAPIClient()

    with patch("time.sleep", return_value=None):
        results = client.fetch_all_scroll(query="test", limit=1, output_file=str(output_file), output_format="csv")

    assert len(results) == 2
    assert os.path.exists(output_file)

    df = pd.read_csv(output_file)
    assert len(df) == 2
    assert "publication-number" in df.columns
    assert set(df["publication-number"].values) == {"PUB1", "PUB2"}

def test_fetch_all_scroll_multiple_pages_json(tmp_path, requests_mock):
    """Test fetch_all_scroll saving final result to JSON."""
    url = "https://api.ted.europa.eu/v3/notices/search"
    output_file = tmp_path / "notices.json"

    requests_mock.post(url, [
        {"json": {"notices": [{"publication-number": "PUB1"}], "iterationNextToken": "TOKEN123"}, "status_code": 200},
        {"json": {"notices": [{"publication-number": "PUB2"}], "iterationNextToken": "TOKEN456"}, "status_code": 200},
        {"json": {"notices": [], "iterationNextToken": "TOKEN_END"}, "status_code": 200},
    ])

    client = api.TEDAPIClient()

    with patch("time.sleep", return_value=None):
        results = client.fetch_all_scroll(query="test", limit=1, output_file=str(output_file), output_format="json")

    assert len(results) == 2
    assert os.path.exists(output_file)

    with open(output_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert isinstance(data, list)
    assert len(data) == 2
    assert set(n["publication-number"] for n in data) == {"PUB1", "PUB2"}
# endregion

# region Error Handling Tests
def test_search_http_error_json(requests_mock):
    """Test that an HTTP error with a JSON body raises TEDAPIError with an appropriate message."""
    error_body = {"error": "Invalid query syntax"}
    url = "https://api.ted.europa.eu/v3/notices/search"
    requests_mock.post(url, json=error_body, status_code=400)
    client = api.TEDAPIClient()
    with pytest.raises(api.TEDAPIError) as excinfo:
        client.search_notices(query="INVALID QUERY")
    # The exception should contain the status code and error message
    err = excinfo.value
    assert "400" in str(err) and "Invalid query syntax" in str(err)

def test_search_http_error_text(requests_mock):
    """Test that an HTTP error with a plain text body raises TEDAPIError with that text."""
    error_text = "Service Unavailable"
    url = "https://api.ted.europa.eu/v3/notices/search"
    # Simulate a 503 error with a plain text response body
    requests_mock.post(url, text=error_text, status_code=503)
    client = api.TEDAPIClient()
    with pytest.raises(api.TEDAPIError) as excinfo:
        client.search_notices(query="ANY")
    err = excinfo.value
    # Error message should contain the status and the text
    assert "503" in str(err) and "Service Unavailable" in str(err)

def test_search_network_error(monkeypatch):
    """Test that a network error (e.g., timeout) raises TEDAPIError."""
    client = api.TEDAPIClient()
    # Monkeypatch requests.post to raise a ConnectTimeout exception
    def fake_post(*args, **kwargs):
        raise requests.exceptions.ConnectTimeout("Connection timed out")
    monkeypatch.setattr(requests, "post", fake_post)
    with pytest.raises(api.TEDAPIError) as excinfo:
        client.search_notices(query="ANY")
    err = excinfo.value
    # The error message should indicate a network error occurred
    assert "Max retries exceeded" in str(err) and "Connection timed out" in str(err)
# endregion

# region Retry and Rate Limit Tests
def test_fetch_notices_exponential_retry(monkeypatch):
    client = api.TEDAPIClient(max_retries=2, backoff_factor=0.5)

    calls = []

    def fake_post(*args, **kwargs):
        calls.append(time.time())
        raise requests.exceptions.ConnectTimeout("timeout!")

    monkeypatch.setattr(requests, "post", fake_post)

    with pytest.raises(api.TEDAPIError):
        client.search_notices(query="any")

    # Expect 3 attempts (1 original + 2 retries)
    assert len(calls) == 3

def test_fetch_notices_rate_limit(monkeypatch):
    client = api.TEDAPIClient(rate_limit_per_minute=60)  # 1 request per second max
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

def test_fetch_notices_retry_limit(monkeypatch):
    client = api.TEDAPIClient(max_retries=2)

    def fake_post(*args, **kwargs):
        raise requests.exceptions.RequestException("fail")

    monkeypatch.setattr(requests, "post", fake_post)

    with pytest.raises(api.TEDAPIError) as e:
        client.search_notices(query="any")

    assert "Max retries exceeded" in str(e.value)

def test_fetch_all_scroll_checkpoint_resume_csv(tmp_path, requests_mock):
    """Test that checkpoint resumption works correctly with CSV appending."""
    url = "https://api.ted.europa.eu/v3/notices/search"
    checkpoint_file = tmp_path / "checkpoint.txt"
    output_file = tmp_path / "notices.csv"

    # Create checkpoint manually
    checkpoint_file.write_text("TOKEN123")

    # Mock two pages: one after checkpoint
    requests_mock.post(url, [
        {"json": {"notices": [{"publication-number": "PUB2"}], "iterationNextToken": "TOKEN456"}, "status_code": 200},
        {"json": {"notices": [], "iterationNextToken": "TOKEN_END"}, "status_code": 200},
    ])

    client = api.TEDAPIClient()

    with patch("time.sleep", return_value=None):
        results = client.fetch_all_scroll(query="test", limit=1,
                                          checkpoint_file=str(checkpoint_file),
                                          output_file=str(output_file),
                                          output_format="csv")

    assert len(results) == 1
    assert os.path.exists(output_file)

    df = pd.read_csv(output_file)
    assert len(df) == 1
    assert "publication-number" in df.columns
    assert df.iloc[0]["publication-number"] == "PUB2"

    assert not checkpoint_file.exists()
# endregion

# region Logging Tests
def test_fetch_notices_log_success(tmp_path, requests_mock):
    log_file = tmp_path / "log_success.txt"
    client = api.TEDAPIClient(log_file=str(log_file))

    sample_response = {"notices": [{"id": "test"}]}
    requests_mock.post("https://api.ted.europa.eu/v3/notices/search", json=sample_response)

    client.search_notices(query="test")

    logs = log_file.read_text()
    assert "SUCCESS" in logs

def test_fetch_notices_log_append_only(tmp_path, requests_mock):
    log_file = tmp_path / "log_append.txt"
    client = api.TEDAPIClient(log_file=str(log_file))

    sample_response = {"notices": [{"id": "test1"}]}
    requests_mock.post("https://api.ted.europa.eu/v3/notices/search", json=sample_response)
    client.search_notices(query="test")

    sample_response2 = {"notices": [{"id": "test2"}]}
    requests_mock.post("https://api.ted.europa.eu/v3/notices/search", json=sample_response2)
    client.search_notices(query="test2")

    logs = log_file.read_text()
    assert logs.count("SUCCESS") == 2

def test_fetch_notices_log_abnormal_response(tmp_path, requests_mock):
    log_file = tmp_path / "log_error.txt"
    client = api.TEDAPIClient(log_file=str(log_file))

    requests_mock.post("https://api.ted.europa.eu/v3/notices/search", status_code=503, text="Server Error")

    with pytest.raises(api.TEDAPIError):
        client.search_notices(query="test")

    logs = log_file.read_text()
    assert "ERROR" in logs
    assert "503" in logs
# endregion
