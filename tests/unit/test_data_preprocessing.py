from analyzer.preprocessing import clean_notice
import pytest
import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal


@pytest.fixture
def raw_data():
    return pd.DataFrame({
        'contract-nature': ['4', '1', '3', '4', '2'],
        'classification-cpv': ['79970000', np.nan, '71200000', '72000000', '30100000'],
        'dispatch-date': ['20240101', '20240115', '20240201', '20240312', '20240401'],
        'tender-value-lowest': [700000.0, 450000.0, 320000.0, 980000.0, 600000.0],
        'tender-value': [850000.0, 500000.0, 400000.0, np.nan, 650000.0],
        'publication-date': ['20240102', '20240116', '20240202', '20240313', '20240402'],
        'notice-type': ['7', '3', np.nan, '2', '7'],
        'recurrence-lot': ['Y', 'N', 'Y', 'N', 'Y'],
        'buyer-country': ['DE', 'FR', 'IT', np.nan, 'ES'],
        'main-activity': ['A', 'B', 'Z', 'E', 'F'],
        'duration-period-value-lot': [12, 24, 18, 6, 9],
        'term-performance-lot': [np.nan, 'Urgent', 'Standard', 'Long-Term', 'Short-Term'],
        'TV_CUR': ['20240102', '20240116', '20240202', '20240313', '20240402'],
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


def test_missing_values_handled(raw_data):
    # clean_notice should be changed to the name of the preprocess method
    processed = clean_notice(raw_data)
    assert not processed.isnull().values.any()


def test_empty_input(empty_data):
    # clean_notice should be changed to the name of the preprocess method
    processed = clean_notice(empty_data)
    assert processed.empty
