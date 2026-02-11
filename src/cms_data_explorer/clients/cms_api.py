"""Client for data.cms.gov data-api/v1 endpoints."""

from __future__ import annotations

import logging
import time

import pandas as pd
import requests

from cms_data_explorer.clients.base import BaseClient
from cms_data_explorer.registry.models import Dataset

logger = logging.getLogger(__name__)


class CMSDataApiClient(BaseClient):
    """Client for data.cms.gov data-api/v1 endpoints.

    These datasets use a different query interface than SODA:
    - size/offset for pagination
    - filter[ColumnName]=value for filtering
    - keyword for full-text search
    """

    BASE_URL = "https://data.cms.gov/data-api/v1/dataset"

    def __init__(self) -> None:
        self._session = requests.Session()

    def fetch(
        self,
        dataset: Dataset,
        params: dict | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> pd.DataFrame:
        """Fetch data from a CMS Data API endpoint.

        Args:
            dataset: Dataset metadata.
            params: Filters as key=value pairs. Keys are converted to
                    filter[Key]=value format.
            limit: Max records (called 'size' in this API).
            offset: Starting record.
        """
        url = f"{self.BASE_URL}/{dataset.id}/data"
        query_params: dict = {"size": limit, "offset": offset}

        if params:
            for key, value in params.items():
                if key == "keyword":
                    query_params["keyword"] = value
                elif key.startswith("filter["):
                    query_params[key] = value
                else:
                    query_params[f"filter[{key}]"] = value

        resp = self._request_with_retry(url, query_params)
        data = resp.json()

        if isinstance(data, list):
            return pd.DataFrame(data) if data else pd.DataFrame()
        if isinstance(data, dict) and "data" in data:
            return pd.DataFrame(data["data"]) if data["data"] else pd.DataFrame()
        return pd.DataFrame()

    def fetch_all(
        self,
        dataset: Dataset,
        params: dict | None = None,
        max_records: int = 100000,
    ) -> pd.DataFrame:
        """Fetch all records with pagination."""
        all_frames: list[pd.DataFrame] = []
        offset = 0
        page_size = 5000
        total_fetched = 0

        while total_fetched < max_records:
            remaining = max_records - total_fetched
            current_limit = min(page_size, remaining)

            df = self.fetch(dataset, params=params, limit=current_limit, offset=offset)
            if df.empty:
                break

            all_frames.append(df)
            total_fetched += len(df)

            if len(df) < current_limit:
                break

            offset += len(df)
            logger.info(f"Fetched {total_fetched} records so far...")

        if not all_frames:
            return pd.DataFrame()
        return pd.concat(all_frames, ignore_index=True)

    def _request_with_retry(
        self, url: str, params: dict, max_retries: int = 3
    ) -> requests.Response:
        """Make HTTP request with retry and backoff."""
        for attempt in range(max_retries + 1):
            try:
                resp = self._session.get(url, params=params, timeout=60)
                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"Rate limited. Waiting {wait}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp
            except requests.RequestException:
                if attempt == max_retries:
                    raise
                wait = 2 ** (attempt + 1)
                logger.warning(f"Request failed. Retrying in {wait}s...")
                time.sleep(wait)
        raise RuntimeError("Max retries exceeded")
