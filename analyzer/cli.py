import click
import datetime

# --- Helpers ---


def validate_date_yyyymmdd(ctx, param, value):
    if value is None:
        return None
    try:
        datetime.datetime.strptime(value, "%Y%m%d")
        return value
    except ValueError:
        raise click.BadParameter(
            f"Date '{value}' must be in YYYYMMDD format (e.g., 20250101)")

# --- CLI Root ---


@click.group()
@click.version_option("0.1.0")
def cli():
    """An intelligent tool for monitoring and analyzing EU public procurement data."""
    pass

# --- Commands ---


@cli.command(help="Fetch procurement notices from TED API.")
@click.option("--start-date", required=True, callback=validate_date_yyyymmdd, help="Start date in YYYYMMDD format.")
@click.option("--end-date", required=True, callback=validate_date_yyyymmdd, help="End date in YYYYMMDD format.")
@click.option("--mode", type=click.Choice(["pagination", "scroll", "full-scroll"]), default="pagination", show_default=True, help="Fetching mode.")
@click.option("--filters", required=False, help="Additional filters for TED API query.")
@click.option("--output", type=click.Choice(["csv", "json"]), default="csv", show_default=True, help="Output format.")
@click.option("--output-file", required=False, help="Optional custom output file name.")
def fetch(start_date, end_date, mode, filters, output, output_file):
    from analyzer import api  # lazy import
    client = api.TEDAPIClient()

    query = client.build_query(start_date, end_date, filters)
    if output_file is None:
        output_file = "notices.csv" if output == "csv" else "notices.json"

    if mode == "full-scroll":
        client.fetch_all_scroll(
            query=query, output_file=output_file, output_format=output)
    else:
        pagination_mode = "ITERATION" if mode == "scroll" else "PAGE_NUMBER"
        data = client.search_notices(
            query=query, pagination_mode=pagination_mode)
        notices = data.get("notices", [])
        if output == "csv":
            client.save_notices_as_csv(notices, output_file)
        elif output == "json":
            client.save_notices_as_json(notices, output_file)

    click.echo(f"Saved output to {output_file}")


@cli.command(help="Start scheduled synchronization of procurement notices.")
@click.option("--interval", type=int, default=1440, show_default=True, help="Sync interval in minutes.")
@click.option("--start-days-ago", type=int, default=7, show_default=True, help="Days to look back if no previous sync.")
@click.option("--filters", required=False, help="Additional filters for TED API.")
@click.option("--output", type=click.Choice(["csv", "json"]), default="csv", show_default=True, help="Output format.")
@click.option("--output-file", required=False, help="Optional custom output file name.")
def sync(interval, start_days_ago, filters, output, output_file):
    from analyzer import sync
    sync.start_scheduler(
        interval_minutes=interval,
        start_days_ago=start_days_ago,
        filters=filters,
        output_format=output,
        output_file=output_file,
    )


@cli.command(help="Preprocess retrieved notices (optional validation, storage, logging).")
@click.option("--input", required=True, help="Input file path (CSV or JSON).")
@click.option("--output", required=False, help="Optional output file path.")
@click.option("--validate", is_flag=True, help="Enable validation of input data.")
@click.option("--db", required=False, help="Optional database URL to store processed notices.")
@click.option("--log", is_flag=True, help="Enable detailed logging.")
def preprocess(input, output, validate, db, log):
    click.echo(f"[preprocess] {input=} {output=} {validate=} {db=} {log=}")


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
