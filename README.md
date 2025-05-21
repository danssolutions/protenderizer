![image](logo.png)
# protenderizer

**protenderizer** is a command-line tool for analyzing public procurement data from the EU’s [Tenders Electronic Daily (TED)](https://ted.europa.eu/). It supports data retrieval, preprocessing, storage, anomaly detection using time series and CUSUM-based outlier detection, and exporting analytical results.

---

## Features

- **Retrieve procurement data** from TED API using pagination or scroll mode
- **Filter by date, region, and contract type**
- **Scheduled sync** support via CLI or background execution
- **Preprocessing** of numeric and categorical fields for machine learning
- **Automatic PostgreSQL storage** with schema inference and extension
- **Anomaly detection** using ARIMA forecasting and CUSUM outlier detection
- **Forecasting** procurement trends into the future
- **CLI export** to JSON or CSV with attribution
- **Modular design**, easily extensible for future GUI or ML models

---

## Installation

Clone the repository and install in editable mode:

```bash
pip install -e .
```

This enables you to run the CLI tool from anywhere:

```bash
protenderizer --help
```

If you modify any dependencies, simply reinstall:

```bash
pip install -e .
```

---

## CLI Commands

Below are the primary commands supported by the tool. You can also add `--help` after each command to get more information on the command's usage and parameters.

### `fetch`

Fetches data from the TED API and writes to file or database.

```bash
protenderizer fetch --start-date 2024-01-01 --end-date 2024-01-31 --format csv --output-file data.csv
```

### `sync`

Synchronizes new procurement data from the last known sync point.

```bash
protenderizer sync --db-url postgres://user:pass@localhost/dbname --db-table notices --last-sync-file .last_sync
```

### `detect-outliers`

Performs ARIMA + CUSUM-based outlier detection and forecasting.

```bash
protenderizer detect-outliers --db-url postgres://... --table full_notices_v2 --plot --plot-file trend.png --output json --output-file result.json
```

---

## Testing

We use `pytest` for all tests.

### Run all tests

```bash
pytest
```

### Run only unit tests

```bash
pytest tests/unit
```

### Run only integration tests

```bash
pytest tests/integration
```

### Run with verbose output

```bash
pytest -v
```

---

## Data Attribution

All data retrieved using `protenderizer` is sourced from the EU’s Tenders Electronic Daily (TED) and is subject to the TED API’s legal disclaimer and copyright policies.


---

## License

See [LICENSE](LICENSE) for full terms.

---

## Acknowledgements

- EU Open Data Portal (TED API)
- scikit-learn, statsmodels, pandas, click, psycopg2

---