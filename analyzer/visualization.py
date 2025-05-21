import pandas as pd
import matplotlib.pyplot as plt
import logging

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


def plot_forecast_results(df: pd.DataFrame, plot_path: str = None):
    """
    Plot forecasted and historical data with ARIMA training and prediction phases.
    Args:
        df (pd.DataFrame): DataFrame with columns like 'count', 'train', 'test',
                           'predicted', 'forecast', and datetime index.
        plot_path (str): Optional path to save the figure. If None, it will show the plot interactively.
    """
    if df.empty or "date" not in df.columns:
        logger.warning("No data available for plotting.")
        return

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.set_index("date").sort_index()

    plt.figure(figsize=(14, 8))

    # Optional layers depending on available columns
    if "count" in df.columns:
        plt.plot(df.index, df["count"], label="Original",
                 linestyle=":", color="grey")
    if "train" in df.columns and df["train"].notna().any():
        plt.plot(df.index, df["train"], label="Train", color="blue")
    if "test" in df.columns and df["test"].notna().any():
        plt.plot(df.index, df["test"], label="Test", color="green")
    if "predicted" in df.columns and df["predicted"].notna().any():
        plt.plot(df.index, df["predicted"],
                 label="Predicted", color="orange", linestyle="--")
    if "forecast" in df.columns and df["forecast"].notna().any():
        plt.plot(df.index, df["forecast"],
                 label="Forecast", color="red", linestyle="--")

    plt.title("Procurement Notice Forecast with ARIMA + CUSUM")
    plt.xlabel("Date")
    plt.ylabel("Notice Count")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    if plot_path:
        plt.savefig(plot_path)
        logger.info(f"Saved plot to {plot_path}")
        plt.close("all")
    else:
        plt.show()
