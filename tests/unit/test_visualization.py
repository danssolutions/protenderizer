import os
import tempfile
import pandas as pd
import pytest
from analyzer.visualization import plot_forecast_results


@pytest.mark.visual
def test_plot_forecast_results_creates_file():
    # Create synthetic forecast data with datetime index
    dates = pd.date_range("2023-01-01", periods=12, freq="ME")
    df = pd.DataFrame({
        "date": dates,
        "count": [100 + i for i in range(12)],
        "train": [100 + i if i < 8 else None for i in range(12)],
        "test": [None if i < 8 else 100 + i for i in range(12)],
        "predicted": [None if i < 8 else 98 + i for i in range(12)],
        "forecast": [None]*12
    })

    with tempfile.TemporaryDirectory() as tmpdir:
        plot_path = os.path.join(tmpdir, "forecast_test.png")
        plot_forecast_results(df, plot_path=plot_path)

        assert os.path.exists(plot_path), "Plot file was not created"
        assert os.path.getsize(plot_path) > 0, "Plot file is empty"
