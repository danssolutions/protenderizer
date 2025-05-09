import click
import datetime
import pandas as pd
from urllib.parse import urlparse


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
        'database': parsed.path[1:],  # remove leading slash
        'user': parsed.username,
        'password': parsed.password
    }


@click.group()
@click.version_option("0.1.0")
def cli():
    """An intelligent tool for monitoring and analyzing EU public procurement data."""
    pass


@cli.command(help="Fetch and optionally store procurement notices from TED API.")
@click.option("--start-date", required=True, callback=validate_date_yyyymmdd)
@click.option("--end-date", required=True, callback=validate_date_yyyymmdd)
@click.option("--mode", type=click.Choice(["pagination", "scroll", "full-scroll"]), default="pagination")
@click.option("--filters", required=False)
@click.option("--output", type=click.Choice(["csv", "json", "none"]), default="none", show_default=True)
@click.option("--output-file", required=False)
@click.option("--db", "--db-url", required=False, help="PostgreSQL connection URL.")
@click.option("--table", "--db-table", default="notices", show_default=True)
@click.option("--no-preprocess", is_flag=True, help="Disable preprocessing (required for DB storage).")
def fetch(start_date, end_date, mode, filters, output, output_file, db, table, no_preprocess):
    from analyzer import api, preprocessing, storage

    client = api.TEDAPIClient()
    query = client.build_query(start_date, end_date, filters)

    if output_file is None and output != "none":
        output_file = "notices.csv" if output == "csv" else "notices.json"

    store_to_db = bool(db)
    db_options = None
    if store_to_db:
        if no_preprocess:
            raise click.UsageError(
                "Raw data cannot be stored to the database. Remove --no-preprocess to continue.")
        db_options = {
            "config": parse_db_url(db),
            "table": table,
            "preprocess": True
        }

    if mode == "full-scroll":
        client.fetch_all_scroll(
            query=query,
            output_file=None if output == "none" else output_file,
            output_format=output if output != "none" else "json",
            store_db=store_to_db,
            db_options=db_options
        )
    else:
        pagination_mode = "ITERATION" if mode == "scroll" else "PAGE_NUMBER"
        data = client.search_notices(
            query=query, pagination_mode=pagination_mode)
        notices = data.get("notices", [])

        if not notices:
            click.echo("No notices retrieved.")
            return

        df = pd.DataFrame(notices)

        if output != "none":
            if output == "csv":
                client.save_notices_as_csv(notices, output_file)
            elif output == "json":
                client.save_notices_as_json(notices, output_file)
            click.echo(f"Saved {len(df)} records to {output_file}")

        if store_to_db:
            df_cleaned = preprocessing.preprocess_notices(df)
            storage.store_dataframe_to_postgres(
                df_cleaned, table, db_options["config"])
            click.echo(
                f"Stored {len(df_cleaned)} rows to table '{table}' in PostgreSQL.")


@cli.command(help="View system logs.")
@click.option("--since", required=False, help="Show logs since this datetime (YYYYMMDD format).")
@click.option("--errors-only", is_flag=True, help="Show only error log entries.")
@click.option("--filter", "log_filter", required=False, help="Keyword filter for log messages.")
def logs(since, errors_only, log_filter):
    click.echo(f"[logs] {since=} {errors_only=} {log_filter=}")


@cli.command("detect-outliers", help="Detect outliers in procurement data.")
@click.option("--start-date", required=False, callback=validate_date_yyyymmdd, help="Start date filter (YYYYMMDD).")
@click.option("--end-date", required=False, callback=validate_date_yyyymmdd, help="End date filter (YYYYMMDD).")
@click.option("--method", type=click.Choice(["time-series", "clustering", "nlp"]), default="clustering", show_default=True, help="Detection method.")
@click.option("--confidence", type=float, default=0.9, show_default=True, help="Confidence threshold for detection.")
@click.option("--output", type=click.Choice(["json", "csv"]), default="json", show_default=True, help="Output format.")
def detect_outliers(start_date, end_date, method, confidence, output):
    click.echo(
        f"[detect-outliers] {start_date=} {end_date=} {method=} {confidence=} {output=}")


@cli.command("list-outliers", help="List detected outliers from previous runs.")
@click.option("--input", required=True, help="Input file containing detected outliers.")
@click.option("--start-date", required=False, callback=validate_date_yyyymmdd, help="Optional start date filter (YYYYMMDD).")
@click.option("--end-date", required=False, callback=validate_date_yyyymmdd, help="Optional end date filter (YYYYMMDD).")
@click.option("--filter", required=False, help="Optional keyword filter.")
def list_outliers(input, start_date, end_date, filter):
    click.echo(f"[list-outliers] {input=} {start_date=} {end_date=} {filter=}")
