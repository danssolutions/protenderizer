from analyzer.preprocessing import clean_notice

def test_clean_notice_missing_fields():
    raw = {"id": "TED001", "value": None}
    cleaned = clean_notice(raw)
    assert cleaned["id"] == "TED001"
    assert "value" in cleaned
