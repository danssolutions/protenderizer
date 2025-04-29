# from analyzer.sync import sync_data

# def test_full_sync(monkeypatch):
#     dummy_data = [{"id": "TED001"}]

#     def mock_fetch_notices():
#         return dummy_data

#     def mock_insert_notices(data):
#         assert data == dummy_data

#     monkeypatch.setattr("analyzer.sync.fetch_notices", mock_fetch_notices)
#     monkeypatch.setattr("analyzer.sync.insert_notice", mock_insert_notices)

#     sync_data()
