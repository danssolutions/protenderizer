import time
from analyzer.sync import sync_data

def test_sync_under_load():
    start = time.time()
    for _ in range(10):
        sync_data()  # Should use mocked API
    assert time.time() - start < 5  # Must complete quickly
