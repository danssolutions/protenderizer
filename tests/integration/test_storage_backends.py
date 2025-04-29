# import pytest
# from analyzer.storage import SQLStorage, NoSQLStorage

# @pytest.mark.parametrize("storage_cls", [SQLStorage, NoSQLStorage])
# def test_store_and_retrieve(storage_cls):
#     storage = storage_cls()
#     storage.insert({"id": "TED001", "value": 123})
#     assert storage.get("TED001")["value"] == 123
