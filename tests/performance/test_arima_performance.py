from analyzer import arima
import pandas as pd
import numpy as np


def test_prepare_monthly_counts_performance(benchmark):
    # Simulate 50k notices over several years
    # hourly notices for ~6 years
    dates = pd.date_range("2015-01-01", periods=50000, freq="h")
    df = pd.DataFrame({"publication-date": dates})
    # Benchmark the monthly aggregation
    result_series = benchmark(arima.prepare_monthly_counts, df)
    # Verify result length (should be number of months) and no NaNs
    assert result_series.index.freqstr == "ME"
    assert not result_series.isna().any()


def test_detect_outliers_pipeline_performance(benchmark):
    # Simulate ~10k notices over 3 years (to produce ~36 monthly data points)
    dates = pd.date_range("2019-01-01", periods=10000, freq="h")
    df = pd.DataFrame({"publication-date": dates})
    # Define a function that runs the full pipeline (excluding DB connection)

    def run_pipeline():
        series = arima.prepare_monthly_counts(df)
        series_imp = arima.impute_outliers_cusum(series)
        # Use a small ARIMA order for speed in test, or patch ARIMA.fit if needed
        train, test, forecast = arima.train_and_forecast_arima(
            series_imp, order=(4, 2, 3), forecast_steps=12, plot=False)
        return forecast
    result_forecast = benchmark(run_pipeline)
    # After benchmarking, optionally verify forecast length
    assert len(result_forecast) == 12


def test_arima_forecast_performance(benchmark):
    # Create a series of 120 monthly points
    index = pd.date_range("2010-01-31", periods=120, freq="ME")
    series = pd.Series(np.random.rand(120), index=index)
    # Time the ARIMA training+forecast
    train, test, forecast = benchmark(lambda: arima.train_and_forecast_arima(
        series, order=(4, 2, 3), forecast_steps=12, plot=False))
    assert len(forecast) == 12
