import pytest
import requests
from analyzer import api

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
    assert err.status_code == 400
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
    assert err.status_code == 503
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
    assert "Network error occurred" in str(err)

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
