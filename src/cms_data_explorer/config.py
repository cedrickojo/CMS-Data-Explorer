"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from platformdirs import user_cache_dir


@dataclass
class Config:
    """Configuration for CMS Data Explorer."""

    socrata_app_token: str = field(
        default_factory=lambda: os.environ.get("SOCRATA_APP_TOKEN", "")
    )
    cache_dir: str = field(
        default_factory=lambda: os.environ.get(
            "CMS_CACHE_DIR", user_cache_dir("cms-data-explorer")
        )
    )
    default_limit: int = 1000
    max_records_per_fetch: int = 50000
    cache_ttl_seconds: int = 86400 * 7  # 7 days
    catalog_ttl_seconds: int = 86400  # 1 day

    def __post_init__(self):
        Path(self.cache_dir).mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_env(cls) -> Config:
        return cls()
