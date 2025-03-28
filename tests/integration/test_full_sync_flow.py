from src.sync import sync_data

def test_full_sync(monkeypatch):
    dummy_data = [{"id": "TED001"}]

    def mock_fetch_new_notices():
        return dummy_data

    def mock_store_notices(data):
        assert data == dummy_data

    monkeypatch.setattr("src.sync.fetch_new_notices", mock_fetch_new_notices)
    monkeypatch.setattr("src.sync.store_notices", mock_store_notices)

    sync_data()
