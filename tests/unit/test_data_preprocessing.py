import pytest
import pandas as pd
import numpy as np
from analyzer.preprocessing import preprocess_notices


@pytest.fixture
def raw_data():
    return pd.DataFrame({
        'contract-nature': [['services'], ['works'], ['supplies'], ['services'], ['works']],
        'classification-cpv': ['79970000', np.nan, '71200000', '72000000', '30100000'],
        'dispatch-date': ['2024-01-01T10:00:00', '2024-01-15T09:30:00', '2024-02-01T12:00:00', '2024-03-12T08:45:00', '2024-04-01T11:00:00'],
        'tender-value-lowest': [700000.0, 450000.0, 320000.0, 980000.0, 600000.0],
        'tender-value': [850000.0, 500000.0, 400000.0, np.nan, 650000.0],
        'publication-date': ['2024-01-02T10:00:00', '2024-01-16T09:30:00', '2024-02-02T12:00:00', '2024-03-13T08:45:00', '2024-04-02T11:00:00'],
        'notice-type': [['cn-standard'], ['can-social'], None, ['pin-only'], ['can-standard']],
        'recurrence-lot': ['Y', 'N', 'Y', 'N', 'Y'],
        'buyer-country': [['DEU'], ['FRA'], ['ITA'], None, ['ESP']],
        'main-activity': [['education'], ['defence'], ['gen-pub'], ['rail'], ['health']],
        'duration-period-value-lot': [12, 24, 18, 6, 9],
        'term-performance-lot': [np.nan, 'Urgent', 'Standard', 'Long-Term', 'Short-Term'],
        'TV_CUR': ['EUR', 'EUR', 'EUR', 'EUR', 'EUR'],
        'renewal-maximum-lot': ['1', '0', '1', '0', np.nan],
        'TVH': [850000.0, 500000.0, 400000.0, 1000000.0, 650000.0]
    })


@pytest.fixture
def empty_data():
    return pd.DataFrame(columns=[
        "contract-nature",
        "classification-cpv",
        "dispatch-date",
        "tender-value-lowest",
        "tender-value",
        "publication-date",
        "notice-type",
        "recurrence-lot",
        "buyer-country",
        "main-activity",
        "duration-period-value-lot",
        "term-performance-lot",
        "TV_CUR",
        "renewal-maximum-lot",
        "TVH"])


def test_preprocessing_handles_missing_values(raw_data):
    df = raw_data.copy()
    processed = preprocess_notices(df)
    assert not processed.isnull().values.any(
    ), "Preprocessed DataFrame contains null values"


def test_preprocessing_preserves_row_count(raw_data):
    df = raw_data.copy()
    processed = preprocess_notices(df)
    assert len(processed) == len(df), "Preprocessing changed row count"


def test_preprocessing_with_empty_dataframe(empty_data):
    df = empty_data.copy()
    processed = preprocess_notices(df)
    assert processed.empty, "Processing empty DataFrame should return empty DataFrame"


def test_numeric_columns_are_filled(raw_data):
    df = raw_data.copy()
    processed = preprocess_notices(df)
    for col in ['tender-value', 'TVH', 'tender-value-lowest']:
        assert col in processed.columns, f"{col} is missing in preprocessed data"
        assert processed[col].dtype != object, f"{col} should be numeric"
        assert not processed[col].isnull().any(), f"{col} still contains nulls"


def test_categorical_columns_are_encoded(raw_data):
    df = raw_data.copy()
    processed = preprocess_notices(df)
    assert any(col.startswith("notice-type_")
               for col in processed.columns), "One-hot encoding for notice-type missing"
    assert any(col.startswith("main-activity_")
               for col in processed.columns), "One-hot encoding for main-activity missing"
