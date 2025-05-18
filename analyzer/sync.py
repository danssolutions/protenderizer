import os
import time
import schedule
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from analyzer.api import TEDAPIClient, TEDAPIError


def parse_db_url(url):
    parsed = urlparse(url)
    return {
        'host': parsed.hostname,
        'port': parsed.port,
        'user': parsed.username,
        'password': parsed.password,
        'dbname': parsed.path.lstrip('/')
    }


def load_last_sync_time(start_days_ago: int, last_sync_file: str = ".last_sync") -> str:
    if os.path.exists(last_sync_file):
        try:
            with open(last_sync_file, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            pass
    return (datetime.now(timezone.utc) - timedelta(days=start_days_ago)).strftime("%Y%m%d")


def save_last_sync_time(last_sync_file: str = ".last_sync"):
    now = datetime.now(timezone.utc).strftime("%Y%m%d")
    with open(last_sync_file, "w", encoding="utf-8") as f:
        f.write(now)


def sync_once(start_days_ago=7, filters=None,
              output_file=None, output_format="none", last_sync_file=".last_sync",
              db_url=None, db_table="notices", preprocess=True):
    client = TEDAPIClient()
    last_sync = load_last_sync_time(start_days_ago, last_sync_file)
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    query = client.build_query(
        start_date=last_sync, end_date=today, additional_filters=filters)

    print(f"[sync] Syncing notices from {last_sync} to {today}.")

    store_to_db = bool(db_url)
    db_options = None
    if store_to_db:
        if output_format == "db" and not preprocess:
            raise ValueError(
                "Cannot store raw data to DB. Preprocessing must be enabled.")
        db_options = {
            "config": parse_db_url(db_url),
            "table": db_table,
            "preprocess": preprocess
        }

    try:
        client.fetch_all_scroll(
            query=query,
            output_file=None if output_format == "none" else output_file,
            output_format=output_format,
            store_db=store_to_db,
            db_options=db_options,
            progress_start_date=last_sync,
            progress_end_date=today
        )
        save_last_sync_time(last_sync_file)
        print("[sync] Synchronization completed successfully.")
    except TEDAPIError as e:
        print(f"[sync] API error during sync: {e}")
    except Exception as e:
        print(f"[sync] Unexpected error during sync: {e}")


def start_scheduler(interval_minutes=1440, **kwargs):
    sync_once(**kwargs)
    schedule.every(interval_minutes).minutes.do(sync_once, **kwargs)

    print(f"[sync] Scheduler started — every {interval_minutes} minutes")
    while True:
        schedule.run_pending()
        time.sleep(1)
