import time
import requests
import logging
import os
import pandas as pd
from analyzer import preprocessing, storage


class TEDAPIError(Exception):
    """Custom exception for TED API client errors."""

    def __init__(self, message: str, status_code: int = None):
        super().__init__(message)
        self.status_code = status_code


class TEDAPIClient:
    """
    Enhanced Client for TED API v3 (Search API).
    - Supports retries with backoff
    - Supports rate limiting
    - Supports logging of all API interactions
    - Handles graceful exit after max retries
    """

    def __init__(self,
                 base_url: str = None,
                 timeout: int = 10,
                 max_retries: int = 3,
                 backoff_factor: float = 1.5,
                 rate_limit_per_minute: int = 600,
                 log_file: str = "ted_api_client.log"):

        self.base_url = base_url or "https://api.ted.europa.eu"
        if self.base_url.endswith("/"):
            self.base_url = self.base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.rate_limit_per_minute = rate_limit_per_minute
        self.min_interval = 60.0 / rate_limit_per_minute
        self.last_request_time = None

        self.search_path = "/v3/notices/search"
        self.logger = self._init_logger(log_file)

    def _init_logger(self, log_file: str):
        logger = logging.getLogger("TEDAPIClient")
        logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _respect_rate_limit(self):
        if self.last_request_time is None:
            return
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_interval:
            sleep_time = self.min_interval - elapsed
            time.sleep(sleep_time)

    def _post_with_retries(self, url, payload):
        retries = 0
        retry_delay = self.backoff_factor  # start with base backoff
        while retries <= self.max_retries:
            try:
                self._respect_rate_limit()
                response = requests.post(
                    url, json=payload, timeout=self.timeout)
                self.last_request_time = time.time()
                if response.ok:
                    self._log_success(url, response, payload)
                    return response
                else:
                    self._log_error(url, response, payload)
                    error_msg = response.text
                    if response.headers.get("Content-Type", "").startswith("text/html"):
                        error_msg = f"HTTP {response.status_code}: HTML error page received"
                    raise TEDAPIError(
                        f"API request failed with status {response.status_code}: {error_msg}", status_code=response.status_code)
            except (requests.RequestException, TEDAPIError) as e:
                retries += 1
                if retries > self.max_retries:
                    self.logger.error(f"Max retries exceeded: {str(e)}")
                    raise TEDAPIError(f"Max retries exceeded: {str(e)}") from e
                sleep = retry_delay
                self.logger.warning(
                    f"Retry {retries}/{self.max_retries} after error: {str(e)} (waiting {sleep:.2f}s)")
                time.sleep(sleep)
                retry_delay *= 2  # exponential backoff

    def _log_success(self, url, response, payload):
        try:
            notices = len(response.json().get("notices", []))
        except Exception:
            notices = "unknown"
        self.logger.info(f"SUCCESS: {url} - Retrieved {notices} notices")

    def _log_error(self, url, response, payload):
        content_type = response.headers.get("Content-Type", "")
        if content_type.startswith("text/html"):
            body = "[HTML error page omitted]"
        else:
            body = response.text
        self.logger.error(
            f"Error: {url} - Status {response.status_code} - {body}")

    def build_query(self, start_date: str, end_date: str, additional_filters: str = None) -> str:
        """
        Build the search query string for TED API based on date range and optional filters.
        """
        base_query = f"dispatch-date>={start_date} AND dispatch-date<={end_date}"
        if additional_filters:
            return f"({base_query}) AND ({additional_filters})"
        return base_query

    def save_notices_as_csv(self, notices, output_file: str, append: bool = False):
        """Save notices to a CSV file, append if needed."""
        if not notices:
            self.logger.warning(f"No notices to save to {output_file}")
            return
        df = pd.DataFrame(notices)
        if "links" in df.columns:
            df.drop(columns=["links"], inplace=True)
        write_mode = "a" if append else "w"
        header = not append
        df.to_csv(output_file, mode=write_mode, header=header,
                  index=False, encoding="utf-8")
        self.logger.info(f"Saved {len(df)} notices to CSV: {output_file}")

    def save_notices_as_json(self, notices, output_file: str):
        """Save notices to a JSON file."""
        if not notices:
            self.logger.warning(f"No notices to save to {output_file}")
            return
        with open(output_file, "w", encoding="utf-8") as f:
            import json
            json.dump(notices, f, indent=2, ensure_ascii=False)
        self.logger.info(
            f"Saved {len(notices)} notices to JSON: {output_file}")

    def search_notices(self, query: str, fields: list = None, page: int = 1, limit: int = 20,
                       scope: str = "ALL", check_query_syntax: bool = False,
                       pagination_mode: str = "PAGE_NUMBER", iteration_token: str = None) -> dict:
        payload = {
            "query": query,
            "page": page,
            "limit": limit,
            "scope": scope,
            "checkQuerySyntax": check_query_syntax,
            "paginationMode": pagination_mode,
            "onlyLatestVersions": True
        }
        if fields is None:
            # For the default fields we make sure to try to not pull any info containing PII
            # (not that it's needed for ML purposes anyway)
            fields = [
                "contract-nature",
                "classification-cpv",
                "dispatch-date",
                "tender-value-lowest",
                "tender-value",
                "publication-date",
                "notice-type",
                "recurrence-lot",
                "buyer-country",
                "main-activity",
                "duration-period-value-lot",
                "term-performance-lot",
                "TV_CUR",  # Tender Value Currency
                "renewal-maximum-lot",
                "TVH"  # Tender Value Highest
            ]
        payload["fields"] = fields
        if pagination_mode == "ITERATION" and iteration_token:
            payload["iterationNextToken"] = iteration_token

        url = self.base_url + self.search_path
        response = self._post_with_retries(url, payload)

        try:
            return response.json()
        except ValueError as e:
            raise TEDAPIError(f"Invalid JSON in response: {e}") from e

    def fetch_all_scroll(
        self,
        query: str,
        fields: list = None,
        limit: int = 250,
        checkpoint_file: str = ".token",
        output_file: str = None,
        output_format: str = "csv",
        store_db: bool = False,
        db_options: dict = None
    ) -> list:
        """
        Fetch all available notices using scroll (iteration) mode.

        Supports:
        - Crash recovery with checkpoint token
        - Incremental saving to CSV or JSON
        - Streaming to PostgreSQL with optional preprocessing
        """
        all_notices = []
        seen_pub_ids = set()
        iteration_token = None
        duplicate_batch_streak = 0
        last_batch_ids = None
        batch_count = 0

        resuming_from_checkpoint = False
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, "r", encoding="utf-8") as f:
                    iteration_token = f.read().strip()
                self.logger.warning(
                    f"Resuming scroll from saved checkpoint token: {iteration_token}...")
                resuming_from_checkpoint = True
            except Exception as e:
                self.logger.error(f"Failed to read checkpoint file: {e}")

        if output_file and not resuming_from_checkpoint and os.path.exists(output_file) and output_format == "csv":
            try:
                os.remove(output_file)
                self.logger.info(
                    f"Deleted previous output file {output_file} (fresh retrieval)")
            except Exception as e:
                self.logger.error(f"Failed to delete output file: {e}")

        first_batch = not os.path.exists(output_file) if output_file else False

        while True:
            batch_count += 1

            response = self.search_notices(
                query=query,
                fields=fields,
                page=1,
                limit=limit,
                pagination_mode="ITERATION",
                iteration_token=iteration_token
            )
            notices = response.get("notices", [])
            token = response.get("iterationNextToken")
            pub_ids = [n.get("publication-number") for n in notices]

            self.logger.info(
                f"Scroll batch {batch_count}: {len(notices)} notices, token={token if token else 'None'}")

            if not notices:
                self.logger.warning("Empty scroll batch detected — aborting")
                break

            if token is None or not isinstance(token, str) or not token.strip():
                self.logger.error(
                    "Invalid or empty iteration token received — aborting")
                print("ERROR: Invalid iteration token received. Check logs. Aborting.")
                break

            if last_batch_ids is not None and pub_ids == last_batch_ids:
                duplicate_batch_streak += 1
                self.logger.warning(
                    f"Duplicate batch detected (#{duplicate_batch_streak}) with IDs: {pub_ids}")
                if duplicate_batch_streak >= 2:
                    self.logger.error(
                        "Detected 2 consecutive duplicate batches — aborting scroll")
                    break
            else:
                duplicate_batch_streak = 0

            batch_data = []
            for notice in notices:
                pub_id = notice.get("publication-number")
                if pub_id not in seen_pub_ids:
                    all_notices.append(notice)
                    seen_pub_ids.add(pub_id)
                    batch_data.append(notice)

            if output_file and output_format == "csv" and batch_data:
                self.save_notices_as_csv(
                    batch_data, output_file, append=not first_batch)
                first_batch = False

            # Save to DB (incremental, with optional preprocessing)
            if store_db and batch_data:
                df = pd.DataFrame(batch_data)
                if db_options.get("preprocess", True):
                    try:
                        df = preprocessing.preprocess_notices(df)
                    except Exception as e:
                        self.logger.error(f"Preprocessing failed: {e}")
                        continue  # skip this batch
                try:
                    storage.store_dataframe_to_postgres(
                        df, db_options["table"], db_options["config"])
                except Exception as e:
                    self.logger.error(f"Database insert failed: {e}")

            iteration_token = token
            last_batch_ids = pub_ids
            try:
                with open(checkpoint_file, "w", encoding="utf-8") as f:
                    f.write(iteration_token)
            except Exception as e:
                self.logger.error(f"Failed to write checkpoint file: {e}")

            time.sleep(0.6)

        # Done scrolling
        if os.path.exists(checkpoint_file):
            try:
                os.remove(checkpoint_file)
                self.logger.info(f"Deleted checkpoint file {checkpoint_file}")
            except Exception as e:
                self.logger.error(f"Failed to delete checkpoint file: {e}")

        if output_file and output_format == "json":
            self.save_notices_as_json(all_notices, output_file)

        return all_notices
