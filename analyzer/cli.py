import click


@click.group()
@click.version_option("0.1.0")
def cli():
    """An intelligent tool for monitoring and analyzing EU public procurement data."""
    pass


@cli.command()
@click.option("--start-date", required=True)
@click.option("--end-date", required=True)
@click.option("--mode", type=click.Choice(["pagination", "scroll", "full-scroll"]), default="pagination")
@click.option("--filters", required=False)
@click.option("--output", type=click.Choice(["csv", "json"]), default="csv")
@click.option("--output-file", required=False)
def fetch(start_date, end_date, mode, filters, output, output_file):
    from analyzer import api  # lazy import
    client = api.TEDAPIClient()

    query = client.build_query(start_date, end_date, filters)
    # Default filenames
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


@cli.command()
@click.option("--interval", type=int, default=1440, help="Synchronization interval in minutes.")
@click.option("--start-days-ago", type=int, default=7, help="How many days to look back if no previous sync.")
@click.option("--filters", required=False, help="Additional filters for notices.")
@click.option("--output", type=click.Choice(["csv", "json"]), default="csv", help="Output format.")
@click.option("--output-file", required=False, help="Output file name.")
def sync(interval, start_days_ago, filters, output, output_file):
    from analyzer import sync

    sync.start_scheduler(
        interval_minutes=interval,
        start_days_ago=start_days_ago,
        filters=filters,
        output_format=output,
        output_file=output_file,
    )


@cli.command()
@click.option("--input", required=True)
@click.option("--validate", is_flag=True)
@click.option("--db", required=False)
@click.option("--log", is_flag=True)
def preprocess(input, output, validate, db, log):
    click.echo(f"[preprocess] {input=} {output=} {validate=} {db=} {log=}")


@cli.command()
@click.option("--since", required=False)
@click.option("--errors-only", is_flag=True)
@click.option("--filter", "log_filter", required=False)
def logs(since, errors_only, log_filter):
    click.echo(f"[logs] {since=} {errors_only=} {log_filter=}")


@cli.command("detect-outliers")
@click.option("--start-date", required=False)
@click.option("--end-date", required=False)
@click.option("--method", type=click.Choice(["time-series", "clustering", "nlp"]), default="clustering")
@click.option("--confidence", type=float, default=0.9)
@click.option("--output", type=click.Choice(["json", "csv"]), default="json")
def detect_outliers(start_date, end_date, method, confidence, output):
    click.echo(
        f"[detect-outliers] {start_date=} {end_date=} {method=} {confidence=} {output=}")


@cli.command("list-outliers")
@click.option("--input", required=True)
@click.option("--start-date", required=False)
@click.option("--end-date", required=False)
@click.option("--filter", required=False)
def list_outliers(input, start_date, end_date, filter):
    click.echo(f"[list-outliers] {input=} {start_date=} {end_date=} {filter=}")
