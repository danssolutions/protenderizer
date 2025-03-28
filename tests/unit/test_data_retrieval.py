import pytest
import requests
from analyzer.api import fetch_notices

def test_fetch_notices_success(requests_mock):
    mock_url = "https://ted.europa.eu/api/notices?page=1"
    mock_data = {"notices": [{"id": "TED001", "title": "Example"}]}
    requests_mock.get(mock_url, json=mock_data)

    notices = fetch_notices(page=1)
    assert len(notices) == 1
    assert notices[0]["id"] == "TED001"

def test_fetch_notices_rate_limit(requests_mock):
    mock_url = "https://ted.europa.eu/api/notices?page=1"
    requests_mock.get(mock_url, status_code=429)

    with pytest.raises(Exception):  # Replace with your actual exception
        fetch_notices(page=1)
