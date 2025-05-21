import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_squared_error
from typing import Tuple
import logging
import time

logger = logging.getLogger("ARIMA")
logger.setLevel(logging.INFO)
if not logger.handlers:
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler("arima.log", mode='w', encoding='utf-8')
    formatter = logging.Formatter(
        "[%(levelname)s] %(asctime)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


def cusum_mean_detection(series, target_mean=None, threshold=5.0, drift=0.5, min_change_magnitude=0):
    logger.info("Starting CUSUM detection...")
    if series.empty:
        return pd.Series(dtype=float), pd.Series(dtype=float), []

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
            if i > last_detection_idx + int(len(series) * 0.1):
                detections.append(i)
                s_pos = s_neg = 0
                last_detection_idx = i
                logger.info(
                    f"CUSUM: Upward change at index {i}, value {x_val:.2f}")
        elif s_neg > valid_threshold and abs(s_neg - valid_threshold) > min_change_magnitude:
            if i > last_detection_idx + int(len(series) * 0.1):
                detections.append(i)
                s_pos = s_neg = 0
                last_detection_idx = i
                logger.info(
                    f"CUSUM: Downward change at index {i}, value {x_val:.2f}")

    logger.info(
        f"CUSUM detection complete - {len(detections)} outliers found.")
    return pd.Series(cusum_pos_values, index=series.index), pd.Series(cusum_neg_values, index=series.index), detections


def prepare_monthly_counts(df: pd.DataFrame, date_col: str = "publication-date") -> pd.Series:
    logger.info("Preparing monthly counts...")

    if date_col not in df.columns:
        raise ValueError(f"Missing required date column: {date_col}")

    # Fast datetime parsing - try format if known, otherwise cache=True
    try:
        df[date_col] = pd.to_datetime(
            df[date_col], format="%Y-%m-%d", errors="coerce", utc=True)
    except Exception:
        df[date_col] = pd.to_datetime(
            df[date_col], errors="coerce", utc=True, cache=True)

    # Drop rows with bad/missing dates
    valid_dates = df[date_col].notna()
    df = df.loc[valid_dates, [date_col]]

    # Remove timezone - faster if done as a Series op before set_index
    df[date_col] = df[date_col].dt.tz_localize(None)

    # Avoid making a full copy of the DataFrame, and avoid inplace operations
    df = df.sort_values(by=date_col)

    # Set index and resample in one go
    series = (
        df.set_index(date_col)
        .resample("ME")  # Month-End frequency
        .size()
    )

    series.name = "Notice Count"
    logger.info(f"Monthly count series prepared with {len(series)} points.")
    return series


def impute_outliers_cusum(series: pd.Series) -> Tuple[pd.Series, dict]:
    logger.info("Starting outlier imputation...")
    std = series.std()
    threshold = std * 1.5 if std and not pd.isna(std) else 1
    drift = std * 0.3 if pd.notna(std) else 0
    min_mag = std * 0.1 if pd.notna(std) else 0

    start = time.time()
    s_pos, s_neg, outlier_idx = cusum_mean_detection(
        series, threshold=threshold, drift=drift, min_change_magnitude=min_mag)
    logger.info(f"CUSUM detection took {time.time() - start:.2f}s")

    series_imputed = series.astype(float).copy()
    explanations = {}

    for idx in sorted(outlier_idx):
        original_value = series_imputed.iloc[idx]
        imputed_value = np.nan

        if 0 < idx < len(series_imputed) - 1:
            prev_val = series_imputed.iloc[idx - 1]
            next_val = series_imputed.iloc[idx + 1]
            if pd.notna(prev_val) and pd.notna(next_val):
                imputed_value = (prev_val + next_val) / 2
            elif pd.notna(prev_val):
                imputed_value = prev_val
            elif pd.notna(next_val):
                imputed_value = next_val
        elif idx == 0 and len(series_imputed) > 1:
            next_val = series_imputed.iloc[idx + 1]
            if pd.notna(next_val):
                imputed_value = next_val
        elif idx == len(series_imputed) - 1 and len(series_imputed) > 1:
            prev_val = series_imputed.iloc[idx - 1]
            if pd.notna(prev_val):
                imputed_value = prev_val

        if pd.notna(imputed_value):
            series_imputed.iloc[idx] = imputed_value
            deviation = abs(original_value - series.mean()) / \
                (series.mean() + 1e-6)
            explanations[idx] = (
                f"Detected spike: value={original_value:.0f}, "
                f"mean={series.mean():.0f}, deviation={deviation:.2f}x"
            )
            logger.info(
                f"Imputed index {idx}: {original_value:.2f} -> {imputed_value:.2f}")
        else:
            logger.warning(
                f"Could not impute value at index {idx} - original={original_value:.2f}")

    logger.info("Outlier imputation complete.")
    return series_imputed, explanations


def train_and_forecast_arima(
    series: pd.Series,
    order=(4, 2, 3),
    forecast_steps=12,
) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    """
    Trains an ARIMA model and returns the train, test, predictions, and forecast series.
    """
    logger.info("Starting ARIMA training and forecasting...")
    if len(series) < 12:
        raise ValueError("Insufficient data for training")

    split_idx = int(len(series) * 0.8)
    train, test = series[:split_idx], series[split_idx:]
    logger.info(f"Train size: {len(train)}, Test size: {len(test)}")

    start_time = time.time()
    model = ARIMA(train, order=order, freq="ME",
                  enforce_stationarity=False,
                  enforce_invertibility=False)
    model_fit = model.fit()
    logger.info(f"Model training completed in {time.time() - start_time:.2f}s")

    predictions = pd.Series(dtype=float)
    if not test.empty:
        predictions = model_fit.predict(
            start=test.index[0], end=test.index[-1])
        predictions = predictions.reindex(test.index)
        rmse = np.sqrt(mean_squared_error(test, predictions.fillna(0)))
        logger.info(f"Test RMSE: {rmse:.2f}")
    else:
        logger.warning("Test set is empty, skipping prediction.")

    forecast_index = pd.date_range(
        start=series.index[-1] + pd.DateOffset(months=1),
        periods=forecast_steps,
        freq="ME"
    )
    forecast = model_fit.forecast(steps=forecast_steps)
    forecast = pd.Series(forecast.values, index=forecast_index)

    logger.info(f"Forecast complete. {forecast_steps} steps generated.")
    return train, test, predictions, forecast
