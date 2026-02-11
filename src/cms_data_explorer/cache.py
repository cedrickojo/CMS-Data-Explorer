"""Cache manager for locally storing fetched CMS data."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from pathlib import Path

import pandas as pd
import requests

logger = logging.getLogger(__name__)


class CacheManager:
    """Manages local caching of downloaded datasets as Parquet files."""

    DEFAULT_TTL = 86400 * 7  # 7 days

    def __init__(self, cache_dir: str) -> None:
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._index_path = self._cache_dir / "cache_index.json"
        self._index = self._load_index()

    def _load_index(self) -> dict:
        """Load the cache index from disk."""
        if self._index_path.exists():
            with open(self._index_path) as f:
                return json.load(f)
        return {}

    def _save_index(self) -> None:
        """Persist the cache index to disk."""
        with open(self._index_path, "w") as f:
            json.dump(self._index, f, indent=2)

    def _make_key(self, dataset_id: str, discriminator: str) -> str:
        """Create a cache key from dataset ID and query discriminator."""
        raw = f"{dataset_id}:{discriminator}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _is_expired(self, entry: dict, ttl: int) -> bool:
        """Check if a cache entry has expired."""
        downloaded_at = entry.get("downloaded_at", 0)
        return (time.time() - downloaded_at) > ttl

    def get_cached_df(
        self, dataset_id: str, params: dict | None = None, ttl: int | None = None
    ) -> pd.DataFrame | None:
        """Retrieve a cached DataFrame if available and not expired.

        Args:
            dataset_id: Dataset identifier.
            params: Query parameters used as part of the cache key.
            ttl: Time-to-live in seconds (defaults to 7 days).

        Returns:
            Cached DataFrame, or None if not cached/expired.
        """
        cache_key = self._make_key(
            dataset_id, json.dumps(params or {}, sort_keys=True)
        )
        entry = self._index.get(cache_key)

        if not entry:
            return None

        if self._is_expired(entry, ttl or self.DEFAULT_TTL):
            logger.info(f"Cache expired for {dataset_id}")
            return None

        path = Path(entry["path"])
        if not path.exists():
            return None

        logger.info(f"Cache hit for {dataset_id} ({entry.get('row_count', '?')} rows)")
        return pd.read_parquet(path)

    def cache_df(
        self, dataset_id: str, df: pd.DataFrame, params: dict | None = None
    ) -> Path:
        """Cache a DataFrame as a Parquet file.

        Args:
            dataset_id: Dataset identifier.
            df: DataFrame to cache.
            params: Query parameters (used as part of cache key).

        Returns:
            Path to the cached Parquet file.
        """
        cache_key = self._make_key(
            dataset_id, json.dumps(params or {}, sort_keys=True)
        )
        local_path = self._cache_dir / f"{cache_key}.parquet"

        df.to_parquet(local_path, index=False)

        self._index[cache_key] = {
            "dataset_id": dataset_id,
            "params": params or {},
            "path": str(local_path),
            "downloaded_at": time.time(),
            "size_bytes": local_path.stat().st_size,
            "row_count": len(df),
        }
        self._save_index()

        logger.info(f"Cached {len(df)} rows for {dataset_id} at {local_path}")
        return local_path

    def download_file(self, url: str, filename: str) -> Path:
        """Download a file and cache it locally.

        Args:
            url: URL to download.
            filename: Local filename for the cached file.

        Returns:
            Path to the downloaded file.
        """
        local_path = self._cache_dir / filename
        if local_path.exists():
            logger.info(f"Using cached file: {local_path}")
            return local_path

        logger.info(f"Downloading {url}...")
        resp = requests.get(url, stream=True, timeout=300)
        resp.raise_for_status()

        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        return local_path

    def list_cached(self) -> list[dict]:
        """List all cached datasets with metadata."""
        entries = []
        for key, entry in self._index.items():
            entry_copy = dict(entry)
            entry_copy["cache_key"] = key
            entry_copy["exists"] = Path(entry["path"]).exists()
            entries.append(entry_copy)
        return entries

    def clear(self, dataset_id: str | None = None) -> int:
        """Remove cached entries.

        Args:
            dataset_id: If provided, only clear entries for this dataset.
                       If None, clear all cached data.

        Returns:
            Number of entries removed.
        """
        removed = 0
        keys_to_remove = []

        for key, entry in self._index.items():
            if dataset_id and entry.get("dataset_id") != dataset_id:
                continue
            path = Path(entry["path"])
            if path.exists():
                path.unlink()
            keys_to_remove.append(key)
            removed += 1

        for key in keys_to_remove:
            del self._index[key]
        self._save_index()

        return removed

    def stats(self) -> dict:
        """Get cache statistics."""
        total_size = 0
        total_entries = len(self._index)
        datasets = set()

        for entry in self._index.values():
            total_size += entry.get("size_bytes", 0)
            datasets.add(entry.get("dataset_id", "unknown"))

        return {
            "total_entries": total_entries,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "unique_datasets": len(datasets),
            "cache_dir": str(self._cache_dir),
        }
