import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error
import warnings
from typing import Tuple, Optional

warnings.filterwarnings("ignore")


def cusum_mean_detection(series, target_mean=None, threshold=5.0, drift=0.5, min_change_magnitude=0):
    if series.empty:
        return pd.Series(dtype=float), pd.Series(dtype=float), []
    if target_mean is None:
        initial_segment_len = max(10, int(len(series) * 0.2))
        target_mean = series.iloc[:initial_segment_len].mean() if len(
            series) > initial_segment_len else series.mean()
        target_mean = 0 if pd.isna(target_mean) else target_mean

    s_pos, s_neg = 0, 0
    cusum_pos_values, cusum_neg_values = [], []
    detections, last_detection_idx = [], -float('inf')

    valid_drift = drift if pd.notna(drift) else 0
    valid_threshold = threshold if pd.notna(threshold) else float('inf')

    for i, x_val in enumerate(series):
        error = 0 if pd.isna(x_val) else x_val - target_mean
        s_pos = max(0, s_pos + error - valid_drift)
        s_neg = max(0, s_neg - error - valid_drift)
        cusum_pos_values.append(s_pos)
        cusum_neg_values.append(s_neg)

        if (s_pos > valid_threshold and abs(s_pos - valid_threshold) > min_change_magnitude or
                s_neg > valid_threshold and abs(s_neg - valid_threshold) > min_change_magnitude):
            if i > last_detection_idx + int(len(series) * 0.1):
                detections.append(i)
                s_pos, s_neg, last_detection_idx = 0, 0, i

    return pd.Series(cusum_pos_values, index=series.index), \
        pd.Series(cusum_neg_values, index=series.index), \
        detections


def prepare_monthly_counts(df: pd.DataFrame, date_col: str = "publication-date") -> pd.Series:
    if date_col not in df.columns:
        raise ValueError(f"Missing required date column: {date_col}")
    temp = df[date_col].astype(str).str.split('+', n=1).str[0]
    df[date_col] = pd.to_datetime(temp, format="%Y-%m-%d", errors="coerce")
    df = df.dropna(subset=[date_col]).copy()
    df.set_index(date_col, inplace=True)
    series = df.resample("M").size()
    series.name = "Notice Count"
    return series


def impute_outliers_cusum(series: pd.Series) -> pd.Series:
    std = series.std()
    threshold = std * 1.5 if std else 1
    drift = std * 0.3 if pd.notna(std) else 0
    min_mag = std * 0.1 if pd.notna(std) else 0

    s_pos, s_neg, outlier_idx = cusum_mean_detection(
        series, threshold=threshold, drift=drift, min_change_magnitude=min_mag)

    series_imputed = series.copy()
    for idx in sorted(outlier_idx):
        if 0 < idx < len(series_imputed) - 1:
            neighbors = [series_imputed.iloc[idx - 1],
                         series_imputed.iloc[idx + 1]]
            valid = [v for v in neighbors if pd.notna(v)]
            if valid:
                series_imputed.iloc[idx] = sum(valid) / len(valid)
        elif idx == 0:
            if pd.notna(series_imputed.iloc[1]):
                series_imputed.iloc[0] = series_imputed.iloc[1]
        elif idx == len(series_imputed) - 1:
            if pd.notna(series_imputed.iloc[-2]):
                series_imputed.iloc[-1] = series_imputed.iloc[-2]
    return series_imputed


def train_and_forecast_arima(series: pd.Series, order=(4, 2, 3), forecast_steps=12, plot=False, plot_path=None) -> Tuple[pd.Series, pd.Series, pd.Series]:
    split_idx = int(len(series) * 0.8)
    train, test = series[:split_idx], series[split_idx:]

    if len(train) < 5:
        raise ValueError("Insufficient data for training")

    model = ARIMA(train, order=order, freq='M')
    model_fit = model.fit()

    predictions = pd.Series(dtype=float)
    if not test.empty:
        predictions = model_fit.predict(
            start=test.index[0], end=test.index[-1]).reindex(test.index)
        rmse = np.sqrt(mean_squared_error(test, predictions.fillna(0)))
        print(f"Test RMSE: {rmse:.2f}")

    forecast_index = pd.date_range(
        series.index[-1] + pd.DateOffset(months=1), periods=forecast_steps, freq='M')
    forecast = model_fit.forecast(steps=forecast_steps)
    forecast = pd.Series(forecast.values, index=forecast_index)

    # if plot:
    #     plt.figure(figsize=(14, 7))
    #     plt.plot(series, label="Original", alpha=0.4, linestyle=":")
    #     plt.plot(train, label="Train", color="blue")
    #     plt.plot(test, label="Test", color="green")
    #     if not predictions.empty:
    #         plt.plot(predictions, label="ARIMA Predictions",
    #                  linestyle="--", color="orange")
    #     plt.plot(forecast, label="Forecast", linestyle="--", color="red")
    #     plt.legend()
    #     plt.grid(True)
    #     plt.tight_layout()
    #     if plot_path:
    #         plt.savefig(plot_path)
    #     else:
    #         plt.show()

    return train, test, forecast
