import click

@click.group()
@click.version_option("0.1.0")
def cli():
    """An intelligent tool for monitoring and analyzing EU public procurement data."""
    pass

@cli.command()
@click.option("--start-date", required=True)
@click.option("--end-date", required=True)
@click.option("--mode", type=click.Choice(["pagination", "scroll"]), default="pagination")
@click.option("--filters", required=False)
@click.option("--output", type=click.Choice(["json", "csv"]), default="json")
def fetch(start_date, end_date, mode, filters, output):
    click.echo(f"[fetch] {start_date=} {end_date=} {mode=} {filters=} {output=}")

@cli.command()
@click.option("--interval", type=click.Choice(["daily", "weekly", "monthly"]), default="daily")
@click.option("--config", required=False)
def sync(interval, config):
    click.echo(f"[sync] {interval=} {config=}")

@cli.command()
@click.option("--input", required=True)
@click.option("--output", required=False)
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
    click.echo(f"[detect-outliers] {start_date=} {end_date=} {method=} {confidence=} {output=}")

@cli.command("list-outliers")
@click.option("--input", required=True)
@click.option("--start-date", required=False)
@click.option("--end-date", required=False)
@click.option("--filter", required=False)
def list_outliers(input, start_date, end_date, filter):
    click.echo(f"[list-outliers] {input=} {start_date=} {end_date=} {filter=}")
