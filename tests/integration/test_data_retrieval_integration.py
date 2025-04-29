import pytest
from analyzer import api

def test_search_notices_success():
    client = api.TEDAPIClient()
    data = client.search_notices(query="classification-cpv=33600000", page=1, limit=1)
    assert isinstance(data, dict), "Response should be a dictionary"
    assert "totalNoticeCount" in data, "Response missing totalNoticeCount"
    assert "notices" in data, "Response missing notices list"
    if data["notices"]:
        first = data["notices"][0]
        assert isinstance(first, dict), "Each notice should be a dictionary"
        assert any(k.lower() in first for k in ["id", "noticeid", "publicationnumber", "title", "links"]), \
            "Notice item does not contain expected fields"

def test_search_notices_success_pagination():
    """Integration test to verify pagination by comparing page 1 and page 2 results."""
    client = api.TEDAPIClient()
    data_page1 = client.search_notices(query="classification-cpv=33600000", page=1, limit=1)
    total = data_page1.get("totalNoticeCount", 0)
    if total < 2:
        pytest.skip("Not enough results to test pagination (only one result found)")
    data_page2 = client.search_notices(query="classification-cpv=33600000", page=2, limit=1)
    assert isinstance(data_page2, dict) and "notices" in data_page2
    assert data_page1.get("notices") != data_page2.get("notices"), "Page 2 notices should differ from Page 1"

def test_search_notices_success_scroll():
    """Integration test to verify basic scroll mode (iteration) behavior."""
    client = api.TEDAPIClient()
    # First scroll request (no token)
    data = client.search_notices(query="classification-cpv=33600000", pagination_mode="ITERATION", page=1, limit=5)
    assert isinstance(data, dict), "Response should be a dictionary"
    assert "notices" in data, "Response missing notices list"
    assert "iterationNextToken" in data, "Response missing iteration token"
    # If there is a next token, fetch the next scroll batch
    token = data.get("iterationNextToken")
    if token:
        data_next = client.search_notices(query="classification-cpv=33600000", pagination_mode="ITERATION",
                                          iteration_token=token, limit=5)
        assert isinstance(data_next, dict), "Next scroll batch response should be a dictionary"
        assert "notices" in data_next, "Next scroll batch missing notices list"
