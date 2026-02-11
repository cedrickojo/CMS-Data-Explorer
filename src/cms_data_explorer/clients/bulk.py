"""Bulk download client for CSV/ZIP files from CMS websites."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import requests

from cms_data_explorer.clients.base import BaseClient
from cms_data_explorer.registry.models import Dataset

logger = logging.getLogger(__name__)


class BulkDownloadClient(BaseClient):
    """Client for downloading bulk CSV/ZIP files from CMS."""

    def __init__(self, cache_dir: str) -> None:
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._session = requests.Session()

    def fetch(
        self,
        dataset: Dataset,
        params: dict | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> pd.DataFrame:
        """Download and read a bulk CSV file.

        For bulk downloads, params are used as pandas query filters
        after loading the data (not as API parameters).
        """
        csv_url = dataset.api_endpoint
        local_path = self._download_if_needed(dataset.id, csv_url)

        df = pd.read_csv(local_path, nrows=limit if limit < 100000 else None)

        if params:
            for key, value in params.items():
                if key in df.columns:
                    df = df[df[key].astype(str) == str(value)]

        if offset > 0:
            df = df.iloc[offset:]
        if limit:
            df = df.head(limit)

        return df

    def fetch_all(
        self,
        dataset: Dataset,
        params: dict | None = None,
        max_records: int = 100000,
    ) -> pd.DataFrame:
        """Load full bulk file with optional filtering."""
        return self.fetch(dataset, params=params, limit=max_records)

    def _download_if_needed(self, dataset_id: str, url: str) -> Path:
        """Download file if not already cached."""
        filename = f"{dataset_id}.csv"
        local_path = self._cache_dir / filename

        if local_path.exists():
            logger.info(f"Using cached file: {local_path}")
            return local_path

        logger.info(f"Downloading {url}...")
        resp = self._session.get(url, stream=True, timeout=300)
        resp.raise_for_status()

        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"Downloaded to {local_path}")
        return local_path
