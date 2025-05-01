import os
import time
import schedule
from datetime import datetime, timedelta, timezone
from analyzer.api import TEDAPIClient, TEDAPIError


def load_last_sync_time(start_days_ago: int, last_sync_file: str = ".last_sync") -> str:
    """Load the last successful sync date, or fallback to a dynamic lookback."""
    if os.path.exists(last_sync_file):
        try:
            with open(last_sync_file, "r", encoding="utf-8") as f:
                timestamp = f.read().strip()
                return timestamp
        except Exception:
            pass
    return (datetime.now(timezone.utc) - timedelta(days=start_days_ago)).strftime("%Y%m%d")


def save_last_sync_time(last_sync_file: str = ".last_sync"):
    """Save the current date as the last successful sync time."""
    now = datetime.now(timezone.utc).strftime("%Y%m%d")
    with open(last_sync_file, "w", encoding="utf-8") as f:
        f.write(now)


def sync_once(start_days_ago=7, filters=None, output_file="notices_sync.csv",
              output_format="csv", last_sync_file=".last_sync"):
    """Perform a single synchronization attempt."""
    client = TEDAPIClient()

    last_sync = load_last_sync_time(start_days_ago, last_sync_file)
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    query = client.build_query(last_sync, today, filters)

    print(f"[sync] Syncing notices from {last_sync} to {today}...")

    try:
        client.fetch_all_scroll(
            query=query,
            output_file=output_file,
            output_format=output_format
        )
        save_last_sync_time(last_sync_file)
        print("[sync] Synchronization completed successfully.")
    except TEDAPIError as e:
        print(f"[sync] API error during sync: {e}")
    except Exception as e:
        print(f"[sync] Unexpected error during sync: {e}")


def start_scheduler(interval_minutes=1440, start_days_ago=7, filters=None,
                    output_format="csv", output_file=None, last_sync_file=".last_sync"):
    """Start the periodic synchronization scheduler."""
    output_file = output_file or (
        "notices_sync.csv" if output_format == "csv" else "notices_sync.json"
    )

    # First immediate sync
    sync_once(start_days_ago, filters, output_file,
              output_format, last_sync_file)

    # Schedule future syncs
    schedule.every(interval_minutes).minutes.do(sync_once,
                                                start_days_ago=start_days_ago,
                                                filters=filters,
                                                output_file=output_file,
                                                output_format=output_format,
                                                last_sync_file=last_sync_file)

    print(
        f"[sync] Scheduler started — every {interval_minutes} minutes, output={output_format}")

    # Poll the schedule
    while True:
        schedule.run_pending()
        time.sleep(1)
