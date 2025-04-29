import pytest
from analyzer import api

@pytest.mark.integration
def test_fetch_notices_success_pagination():
    client = api.TEDAPIClient()
    data = client.search_notices(query="classification-cpv=33600000", page=1, limit=1)
    assert isinstance(data, dict)
    assert "notices" in data

@pytest.mark.integration
def test_fetch_notices_success_scroll():
    client = api.TEDAPIClient()
    data = client.search_notices(query="classification-cpv=33600000", pagination_mode="ITERATION", page=1, limit=1)
    assert isinstance(data, dict)
    assert "notices" in data
    assert "iterationNextToken" in data

@pytest.mark.integration
def test_fetch_notices_time_interval_scroll():
    client = api.TEDAPIClient()
    query = "dispatch-date>20240101 AND dispatch-date<20241231"
    data = client.search_notices(query=query, pagination_mode="ITERATION", page=1, limit=1)
    assert isinstance(data, dict)
    assert "notices" in data

@pytest.mark.integration
def test_fetch_notices_filters_scroll():
    client = api.TEDAPIClient()
    query = "buyer-country=DEU"
    data = client.search_notices(query=query, pagination_mode="ITERATION", page=1, limit=1)
    assert isinstance(data, dict)
    assert "notices" in data
