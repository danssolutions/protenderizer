import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error
import warnings
from typing import Tuple, Optional

warnings.filterwarnings("ignore")


def cusum_mean_detection(series, target_mean=None, threshold=5.0, drift=0.5, min_change_magnitude=0):
    print("Starting CUSUM detection...")
    if series.empty:
        return pd.Series(dtype=float), pd.Series(dtype=float), []
    # Determine initial target mean from first segment or overall mean
    if target_mean is None:
        initial_segment_len = max(10, int(len(series) * 0.2))
        if len(series) > initial_segment_len:
            target_mean = series.iloc[:initial_segment_len].mean()
        else:
            target_mean = series.mean()
        if pd.isna(target_mean):
            target_mean = 0
    s_pos, s_neg = 0, 0
    cusum_pos_values, cusum_neg_values = [], []
    detections, last_detection_idx = [], -float('inf')
    valid_drift = drift if pd.notna(drift) else 0
    valid_threshold = threshold if pd.isna(
        threshold) or threshold == 0 else threshold
    for i, x_val in enumerate(series):
        error = 0 if pd.isna(x_val) else x_val - target_mean
        s_pos = max(0, s_pos + error - valid_drift)
        s_neg = max(0, s_neg - error - valid_drift)
        cusum_pos_values.append(s_pos)
        cusum_neg_values.append(s_neg)
        if s_pos > valid_threshold and abs(s_pos - valid_threshold) > min_change_magnitude:
            # Upward shift detected
            if i > last_detection_idx + int(len(series) * 0.1):
                detections.append(i)
                s_pos = s_neg = 0
                last_detection_idx = i
                print(
                    f"CUSUM: Upward change detected at index {i}, value {x_val:.2f}")
        elif s_neg > valid_threshold and abs(s_neg - valid_threshold) > min_change_magnitude:
            # Downward shift detected
            if i > last_detection_idx + int(len(series) * 0.1):
                detections.append(i)
                s_pos = s_neg = 0
                last_detection_idx = i
                print(
                    f"CUSUM: Downward change detected at index {i}, value {x_val:.2f}")
    print(f"CUSUM detection complete — {len(detections)} outliers found.")
    return pd.Series(cusum_pos_values, index=series.index), pd.Series(cusum_neg_values, index=series.index), detections


def prepare_monthly_counts(df: pd.DataFrame, date_col: str = "publication-date") -> pd.Series:
    print("Preparing monthly counts...")
    if date_col not in df.columns:
        raise ValueError(f"Missing required date column: {date_col}")
    # Parse full datetime (including time and offset) and drop rows with invalid dates
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce", utc=True)
    df = df.dropna(subset=[date_col]).copy()
    df[date_col] = df[date_col].dt.tz_localize(None)  # make tz-naive if needed
    df.set_index(date_col, inplace=True)
    series = df.resample("M").size()
    series.name = "Notice Count"
    print(f"Monthly count series prepared with {len(series)} data points.")
    return series


def impute_outliers_cusum(series: pd.Series) -> pd.Series:
    print("Imputing outliers...")
    std = series.std()
    # Set CUSUM parameters relative to data volatility
    threshold = std * 1.5 if std and not pd.isna(std) else 1
    drift = std * 0.3 if pd.notna(std) else 0
    min_mag = std * 0.1 if pd.notna(std) else 0

    s_pos, s_neg, outlier_idx = cusum_mean_detection(
        series, threshold=threshold, drift=drift, min_change_magnitude=min_mag
    )

    series_imputed = series.copy()
    for idx in sorted(outlier_idx):
        original_value = series_imputed.iloc[idx]
        imputed_value = np.nan

        # If outlier is not at the edges, use neighbors to interpolate
        if 0 < idx < len(series_imputed) - 1:
            prev_val = series_imputed.iloc[idx - 1]
            next_val = series_imputed.iloc[idx + 1]
            if pd.notna(prev_val) and pd.notna(next_val):
                imputed_value = (prev_val + next_val) / 2
            elif pd.notna(prev_val):
                imputed_value = prev_val
            elif pd.notna(next_val):
                imputed_value = next_val
        # Edge cases: first or last point
        elif idx == 0 and len(series_imputed) > 1:
            next_val = series_imputed.iloc[idx + 1]
            if pd.notna(next_val):
                imputed_value = next_val
        elif idx == len(series_imputed) - 1 and len(series_imputed) > 1:
            prev_val = series_imputed.iloc[idx - 1]
            if pd.notna(prev_val):
                imputed_value = prev_val

        if pd.notna(imputed_value):
            print(f"  Index {idx} ({series_imputed.index[idx].strftime('%Y-%m-%d')}): "
                  f"Original={original_value:.2f}, Imputed={imputed_value:.2f}")
            series_imputed.iloc[idx] = imputed_value
        else:
            # If we couldn't compute an imputed value (should be rare), leave it as is
            print(f"  Index {idx} ({series_imputed.index[idx].strftime('%Y-%m-%d')}): "
                  f"Original={original_value:.2f}, Could not impute.")
    print("Outlier imputation complete.")
    return series_imputed


def train_and_forecast_arima(
    series: pd.Series,
    order=(4, 2, 3),
    forecast_steps=12,
    plot=False,
    plot_path=None
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    print("Training ARIMA model...")
    # Basic sanity check for sufficient data
    if len(series) < 12:
        raise ValueError("Insufficient data for training")
    # Split data into train/test for model validation (80/20 split)
    split_idx = int(len(series) * 0.8)
    train, test = series[:split_idx], series[split_idx:]
    print(f"Training data points: {len(train)}")
    print(f"Testing data points: {len(test)}")

    # Fit ARIMA model on training data
    model = ARIMA(train, order=order, freq='M')
    model_fit = model.fit()

    # In-sample prediction for test period
    predictions = pd.Series(dtype=float)
    if not test.empty:
        predictions = model_fit.predict(
            start=test.index[0], end=test.index[-1])
        # Align predictions index with test index
        predictions = predictions.reindex(test.index)
        # Calculate RMSE on test set for diagnostic
        rmse = np.sqrt(mean_squared_error(test, predictions.fillna(0)))
        print(f"Test RMSE: {rmse:.2f}")
    else:
        print("Test set is empty. Skipping evaluation.")

    # Forecast future values beyond the end of the series
    forecast_index = pd.date_range(start=series.index[-1] + pd.DateOffset(months=1),
                                   periods=forecast_steps, freq='M')
    forecast = model_fit.forecast(steps=forecast_steps)
    forecast = pd.Series(forecast.values, index=forecast_index)

    # Optionally, generate and save/show a plot of the results
    if plot:
        plt.figure(figsize=(14, 8))
        # Original full series
        plt.plot(series.index, series, label='Original',
                 linestyle=':', color='grey')
        # Training portion
        plt.plot(train.index, train, label='Train', color='blue')
        # Testing portion (actual values)
        if not test.empty:
            plt.plot(test.index, test, label='Test', color='green')
        # Model predictions on the test period
        if not predictions.empty:
            plt.plot(predictions.index, predictions,
                     label='Predicted', color='orange', linestyle='--')
        # Forecasted future values
        if not forecast.empty:
            plt.plot(forecast.index, forecast, label='Forecast',
                     color='red', linestyle='--')
        plt.title("ARIMA Forecast with CUSUM Imputation")
        plt.xlabel("Date")
        plt.ylabel("Notices")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        if plot_path:
            plt.savefig(plot_path)
            print(f"Saved plot to {plot_path}")
        else:
            plt.show()
    print(f"Forecasting complete. Forecasted {forecast_steps} months ahead.")
    return train, test, forecast
