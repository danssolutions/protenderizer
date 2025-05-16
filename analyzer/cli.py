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
            db_options=db_options
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
def detect_outliers(db, table, output, output_file):

    try:
        db_config = resolve_db_config(db)
        engine = sqlalchemy.create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )
        df = pd.read_sql_table(table, con=engine)
        series = arima.prepare_monthly_counts(df)
        series_imputed = arima.impute_outliers_cusum(series)
        train, test, forecast = arima.train_and_forecast_arima(series_imputed)

        # Output results
        output_file = resolve_output_settings(output, output_file)
        result_df = pd.DataFrame({
            "date": series_imputed.index,
            "count": series,
            "imputed": series_imputed,
            "forecast": pd.concat(
                [pd.Series([None] * len(series_imputed)), forecast],
                ignore_index=False
            )
        })

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


@cli.command("list-outliers", help="List previously detected outliers.")
@click.option("--input", required=True, help="Input CSV or JSON file containing anomaly results.")
@click.option("--start-date", required=False, callback=validate_date_yyyymmdd)
@click.option("--end-date", required=False, callback=validate_date_yyyymmdd)
@click.option("--filter", required=False, help="Keyword to filter by country or anomaly type (stub).")
def list_outliers(input, start_date, end_date, filter):
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

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if start_date:
        df = df[df["date"] >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df["date"] <= pd.to_datetime(end_date)]
    if filter:
        df = df[df.apply(lambda row: filter.lower()
                         in str(row).lower(), axis=1)]

    if df.empty:
        click.echo("No matching outliers found.")
    else:
        click.echo(df.to_markdown(index=False))
