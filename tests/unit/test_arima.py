import pytest
import pandas as pd
import numpy as np
from analyzer import arima
import warnings
from statsmodels.tools.sm_exceptions import ConvergenceWarning


def test_cusum_detects_known_shift():
    series = pd.Series([10]*10 + [50]*10)
    _, _, detections = arima.cusum_mean_detection(
        series, threshold=5.0, drift=0.5)
    assert len(detections) >= 1


def test_impute_outliers_basic_interpolation():
    series = pd.Series([10, 10, 1000, 10, 10], index=pd.date_range(
        "2020-01-01", periods=5, freq="ME"))

    # Patch cusum_mean_detection to force detection at index 2
    original_cusum = arima.cusum_mean_detection

    def mock_cusum(series, **kwargs):
        s_pos = pd.Series([0, 0, 20, 0, 0], index=series.index)
        s_neg = pd.Series([0, 0, 0, 0, 0], index=series.index)
        return s_pos, s_neg, [2]

    arima.cusum_mean_detection = mock_cusum

    try:
        imputed, explanations = arima.impute_outliers_cusum(series)
        assert imputed.iloc[2] != 1000
        assert explanations[2].startswith("Detected spike:")
    finally:
        arima.cusum_mean_detection = original_cusum


def test_prepare_monthly_counts():
    dates = pd.date_range("2020-01-01", periods=5, freq="D")
    df = pd.DataFrame({"publication-date": dates})
    series = arima.prepare_monthly_counts(df)
    assert isinstance(series, pd.Series)
    assert series.index.freqstr == "ME"


def test_arima_training_and_forecast():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", ConvergenceWarning)
        index = pd.date_range("2020-01-01", periods=24, freq="ME")
        series = pd.Series(np.arange(24), index=index)
        train, test, _, forecast = arima.train_and_forecast_arima(series)
        assert len(forecast) == 12
        assert all(forecast.index > series.index[-1])


def test_train_fails_with_insufficient_data():
    index = pd.date_range("2020-01-01", periods=6, freq="ME")
    series = pd.Series(np.arange(6), index=index)
    with pytest.raises(ValueError):
        arima.train_and_forecast_arima(series)
