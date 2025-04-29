"""
Module: api
Provides a TEDAPIClient class to interact with the TED API v3 (Search API).
"""
import requests

class TEDAPIError(Exception):
    """Custom exception for TED API client errors."""
    def __init__(self, message: str, status_code: int = None):
        super().__init__(message)
        self.status_code = status_code

class TEDAPIClient:
    """
    Client for TED API v3 (Search API).
    Supports searching published notices via expert query.
    """
    def __init__(self, base_url: str = None, timeout: int = 10):
        """
        Initialize the API client.
        :param base_url: Base URL for the TED API (defaults to production).
        :param timeout: Timeout for API requests (in seconds).
        """
        # Base URL defaults to the production TED API endpoint
        self.base_url = base_url or "https://api.ted.europa.eu"
        # Ensure no trailing slash in base_url
        if self.base_url.endswith("/"):
            self.base_url = self.base_url.rstrip("/")
        self.timeout = timeout
        # Endpoint path for search (v3 notices search)
        self.search_path = "/v3/notices/search"
    
    def search_notices(self, query: str, fields: list = None, page: int = 1, limit: int = 20,
                       scope: str = "ALL", check_query_syntax: bool = False,
                       pagination_mode: str = "PAGE_NUMBER", iteration_token: str = None) -> dict:
        """
        Search for notices using the TED API Search endpoint.
        :param query: Expert search query string to filter and sort notices.
        :param fields: Optional list of fields to return for each notice.
        :param page: Page number for pagination (starting at 1, used in PAGE_NUMBER mode).
        :param limit: Number of notices per page (max 250).
        :param scope: Search scope (e.g. 'ALL', 'ACTIVE', 'LATEST').
        :param check_query_syntax: If True, only check query syntax without returning results.
        :param pagination_mode: 'PAGE_NUMBER' (default) for standard pagination or 'ITERATION' for scroll.
        :param iteration_token: Token for the next page in ITERATION mode (not needed for first call).
        :return: A dictionary (parsed JSON) with search results and metadata.
        :raises TEDAPIError: on network issues or API error responses.
        """
        # Build the request payload according to TED API specifications
        payload = {
            "query": query,
            "page": page,
            "limit": limit,
            "scope": scope,
            "checkQuerySyntax": check_query_syntax,
            "paginationMode": pagination_mode,
            "onlyLatestVersions": True  # Force this field
        }
        if fields is None:
            fields = [
                "contract-nature",
                "classification-cpv",
                "dispatch-date",
                "tender-value",
                "publication-date",
                "notice-type",
                "organisation-country-buyer",
                "buyer-country",
                "main-activity"
            ]
        payload["fields"] = fields
        if pagination_mode == "ITERATION" and iteration_token:
            payload["iterationNextToken"] = iteration_token
        url = self.base_url + self.search_path
        try:
            response = requests.post(url, json=payload, timeout=self.timeout)
        except requests.RequestException as e:
            # Network-level errors (connection issues, timeouts, etc.)
            raise TEDAPIError(f"Network error occurred: {e}") from e
        # If the response status code indicates an error, handle it
        if not response.ok:
            # Try to parse error details from response (JSON or text)
            error_message = ""
            try:
                error_json = response.json()
                if isinstance(error_json, dict):
                    # Attempt to extract a meaningful message from common fields
                    error_message = error_json.get("message") or error_json.get("error") or ""
            except ValueError:
                # Response content is not JSON
                error_message = response.text or ""
            status = response.status_code
            raise TEDAPIError(f"API request failed with status {status}: {error_message}", status_code=status)
        # Parse the JSON response
        try:
            data = response.json()
        except ValueError as e:
            raise TEDAPIError(f"Invalid JSON in response: {e}") from e
        return data
