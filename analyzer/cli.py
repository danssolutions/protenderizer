import click
import datetime
import pandas as pd
import os
import sqlalchemy
from urllib.parse import urlparse
from dotenv import load_dotenv
from analyzer import api, preprocessing, storage, sync as sync_module, arima

load_dotenv()


def validate_date_yyyymmdd(ctx, param, value):
    if value is None:
        return None
    try:
        datetime.datetime.strptime(value, "%Y%m%d")
        return value
    except ValueError:
        raise click.BadParameter(
            f"Date '{value}' must be in YYYYMMDD format (e.g., 20250101)")


def parse_db_url(url):
    parsed = urlparse(url)
    return {
        'host': parsed.hostname,
        'port': parsed.port,
        'database': parsed.path[1:],
        'user': parsed.username,
        'password': parsed.password
    }


def resolve_db_config(db_url):
    db_url = db_url or os.getenv("DB_URL")
    if not db_url:
        raise click.UsageError(
            "Database output selected but no --db or DB_URL found in .env")
    return parse_db_url(db_url)


def resolve_output_settings(output, output_file):
    if output == "csv" and not output_file:
        return "notices.csv"
    elif output == "json" and not output_file:
        return "notices.json"
    return output_file


@click.group()
@click.version_option("0.1.0")
def cli():
    """An intelligent tool for monitoring and analyzing EU public procurement data."""
    pass


@cli.command(help="Fetch and store procurement notices from TED API.")
@click.option("--start-date", required=True, callback=validate_date_yyyymmdd)
@click.option("--end-date", required=True, callback=validate_date_yyyymmdd)
@click.option("--mode", type=click.Choice(["pagination", "scroll", "full-scroll"]), default="full-scroll", show_default=True)
@click.option("--filters", required=False)
@click.option("--output", type=click.Choice(["none", "csv", "json", "db"]), default="db", show_default=True, help="Storage destination")
@click.option("--output-file", required=False, help="Filename to store CSV or JSON output")
@click.option("--db", "--db-url", required=False, help="PostgreSQL connection URL (overrides DB_URL env)")
@click.option("--table", "--db-table", default="notices", show_default=True)
def fetch(start_date, end_date, mode, filters, output, output_file, db, table):

    client = api.TEDAPIClient()
    query = client.build_query(start_date, end_date, filters)
    pagination_mode = "ITERATION" if mode == "scroll" else "PAGE_NUMBER"

    store_to_db = output == "db"
    preprocess = output == "db"
    resolved_output_file = resolve_output_settings(output, output_file)

    db_options = None
    if store_to_db:
        db_options = {
            "config": resolve_db_config(db),
            "table": table,
            "preprocess": preprocess
        }

    if mode == "full-scroll":
        client.fetch_all_scroll(
            query=query,
            output_file=None if output == "none" or store_to_db else resolved_output_file,
            output_format=output if output in ("csv", "json") else "json",
            store_db=store_to_db,
            db_options=db_options,
            progress_start_date=start_date,
            progress_end_date=end_date
        )
    else:
        data = client.search_notices(
            query=query, pagination_mode=pagination_mode)
        notices = data.get("notices", [])
        if not notices:
            click.echo("No notices retrieved.")
            return

        df = pd.DataFrame(notices)

        if output in ("csv", "json"):
            if output == "csv":
                client.save_notices_as_csv(notices, resolved_output_file)
            else:
                client.save_notices_as_json(notices, resolved_output_file)
            click.echo(f"Saved {len(df)} records to {resolved_output_file}")

        if store_to_db:
            df_cleaned = preprocessing.preprocess_notices(df)
            storage.store_dataframe_to_postgres(
                df_cleaned, table, db_options["config"])
            click.echo(
                f"Stored {len(df_cleaned)} rows to table '{table}' in PostgreSQL.")


@cli.command(help="Schedule periodic synchronization from TED API.")
@click.option("--interval", type=int, default=1440, show_default=True, help="Synchronization interval in minutes.")
@click.option("--start-days-ago", type=int, default=7, show_default=True, help="How many days to look back if no previous sync.")
@click.option("--filters", required=False, help="Additional filters for notices.")
@click.option("--output", type=click.Choice(["none", "csv", "json", "db"]), default="db", show_default=True)
@click.option("--output-file", required=False)
@click.option("--db", "--db-url", required=False, help="PostgreSQL connection URL.")
@click.option("--table", "--db-table", default="notices", show_default=True)
def sync(interval, start_days_ago, filters, output, output_file, db, table):

    resolved_output_file = resolve_output_settings(output, output_file)
    preprocess = output == "db"

    sync_module.start_scheduler(
        interval_minutes=interval,
        start_days_ago=start_days_ago,
        filters=filters,
        output_format=output,
        output_file=resolved_output_file,
        db_url=db or os.getenv("DB_URL"),
        db_table=table,
        preprocess=preprocess
    )


@cli.command("detect-outliers", help="Detect outliers using time-series ARIMA + CUSUM.")
@click.option("--db", "--db-url", required=False)
@click.option("--table", "--db-table", default="notices", show_default=True)
@click.option("--output", type=click.Choice(["csv", "json"]), default="json", show_default=True)
@click.option("--output-file", required=False)
@click.option("--arima-order", default="4,2,3", help="ARIMA order as comma-separated values (p,d,q). Default is 4,2,3")
@click.option("--forecast-steps", default=12, show_default=True, help="Number of future months to forecast.")
@click.option("--plot", is_flag=True, help="Generate and save a forecast plot.")
@click.option("--plot-file", required=False, help="Path to save the plot if --plot is enabled.")
def detect_outliers(db, table, output, output_file, arima_order, forecast_steps, plot, plot_file):
    try:
        db_config = resolve_db_config(db)
        engine = sqlalchemy.create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        arima_order_tuple = tuple(int(x) for x in arima_order.split(","))

        click.echo(f"Fetching data from '{table}'...")
        with engine.connect().execution_options(stream_results=True) as connection:
            chunks = pd.read_sql_table(table, con=connection, chunksize=10000)
            df = pd.concat(chunks, ignore_index=True)

        # Compute and impute
        series = arima.prepare_monthly_counts(df)
        series_imputed, explanations = arima.impute_outliers_cusum(series)
        train, test, forecast = arima.train_and_forecast_arima(
            series_imputed,
            order=arima_order_tuple,
            forecast_steps=forecast_steps,
            plot=plot,
            plot_path=plot_file
        )

        # Merge results
        output_file = resolve_output_settings(output, output_file)
        full_index = series_imputed.index.union(forecast.index)

        result_df = pd.DataFrame(index=full_index)
        result_df["count"] = series.reindex(full_index)
        result_df["imputed"] = series_imputed.reindex(full_index)
        result_df["forecast"] = forecast.reindex(full_index)

        # Add human-readable explanations
        result_df["explanation"] = None
        for idx, reason in explanations.items():
            ts_index = series_imputed.index[idx]
            result_df.loc[ts_index, "explanation"] = reason

        # Final output
        result_df = result_df.reset_index().rename(columns={"index": "date"})

        if output == "csv":
            result_df.to_csv(output_file, index=False)
        else:
            result_df.to_json(output_file, orient="records",
                              indent=2, date_format="iso")

        click.echo(f"Saved outlier analysis to {output_file}")
        click.echo(
            "Attribution: This data is sourced from the EU’s Tenders Electronic Daily (TED).")

    except Exception as e:
        raise click.ClickException(str(e))


@cli.command("list-outliers", help="List previously detected outliers with filtering and explanations.")
@click.option("--input", required=True, help="Input CSV or JSON file containing anomaly results.")
@click.option("--start-date", required=False, callback=validate_date_yyyymmdd)
@click.option("--end-date", required=False, callback=validate_date_yyyymmdd)
@click.option("--filter", required=False, help="Keyword to filter by explanation or metadata (case-insensitive).")
@click.option("--min-cost-deviation", type=float, help="Minimum tender-value deviation from historical mean.")
@click.option("--min-bidders", type=int, help="Minimum number of bidders (if present).")
@click.option("--max-count", type=int, help="Limit the number of anomalies shown.")
def list_outliers(input, start_date, end_date, filter, min_cost_deviation, min_bidders, max_count):
    import numpy as np

    if not os.path.exists(input):
        click.echo(f"Input file {input} not found.")
        return

    ext = os.path.splitext(input)[-1].lower()
    if ext == ".csv":
        df = pd.read_csv(input)
    elif ext == ".json":
        df = pd.read_json(input)
    else:
        click.echo("Unsupported file format. Use CSV or JSON.")
        return

    # Normalize date column
    if "date" not in df.columns:
        click.echo("Input data must contain a 'date' column.")
        return
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Basic date filtering
    if start_date:
        df = df[df["date"] >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df["date"] <= pd.to_datetime(end_date)]

    # Keyword filter
    if filter:
        keyword = filter.lower()
        df = df[df.apply(lambda row: keyword in str(row).lower(), axis=1)]

    # Cost deviation filter
    if min_cost_deviation and "tender-value" in df.columns:
        historical_mean = df["tender-value"].mean(skipna=True)
        df = df[df["tender-value"] > historical_mean + min_cost_deviation]

    # Min bidders filter
    if min_bidders and "number-of-bidders" in df.columns:
        df = df[df["number-of-bidders"] >= min_bidders]

    # Limit result size
    if max_count:
        df = df.head(max_count)

    if df.empty:
        click.echo("No matching outliers found.")
        return

    # Pick subset of relevant columns to display
    columns_to_show = ["date", "tender-value",
                       "number-of-bidders", "forecast", "explanation"]
    existing_cols = [col for col in columns_to_show if col in df.columns]
    display_df = df[existing_cols +
                    [c for c in df.columns if c not in existing_cols]].copy()

    click.echo(display_df.to_markdown(index=False, tablefmt="grid"))
