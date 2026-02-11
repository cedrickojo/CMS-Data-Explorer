"""Socrata/SODA API client for data.medicare.gov, data.medicaid.gov, etc."""

from __future__ import annotations

import logging
import time

import pandas as pd
import requests

from cms_data_explorer.clients.base import BaseClient
from cms_data_explorer.registry.models import Dataset

logger = logging.getLogger(__name__)


class SodaClient(BaseClient):
    """Client for Socrata SODA API endpoints.

    Supports data.medicare.gov, data.medicaid.gov, data.cdc.gov,
    openpaymentsdata.cms.gov, and data.cms.gov (SODA datasets).
    """

    def __init__(self, app_token: str = "") -> None:
        self._app_token = app_token
        self._session = requests.Session()
        if app_token:
            self._session.headers["X-App-Token"] = app_token

    def fetch(
        self,
        dataset: Dataset,
        params: dict | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> pd.DataFrame:
        """Fetch data from a SODA API endpoint.

        Args:
            dataset: Dataset metadata.
            params: SoQL parameters ($where, $select, $group, $order)
                    or simple key=value filters.
            limit: Max records per page (max 50,000).
            offset: Starting record.
        """
        url = dataset.api_endpoint
        query_params: dict = {"$limit": min(limit, 50000), "$offset": offset}

        if params:
            for key, value in params.items():
                if key.startswith("$"):
                    query_params[key] = value
                else:
                    query_params[key] = value

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
        """Fetch all records with automatic pagination.

        Args:
            dataset: Dataset metadata.
            params: Query parameters.
            max_records: Safety limit on total records fetched.
        """
        all_frames: list[pd.DataFrame] = []
        offset = 0
        page_size = 50000
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
