"""Abstract base client for all API clients."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from cms_data_explorer.registry.models import Column, Dataset


class BaseClient(ABC):
    """Abstract base for all API clients."""

    @abstractmethod
    def fetch(
        self,
        dataset: Dataset,
        params: dict | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> pd.DataFrame:
        """Fetch data from the dataset. Returns a DataFrame."""
        ...

    @abstractmethod
    def fetch_all(
        self,
        dataset: Dataset,
        params: dict | None = None,
        max_records: int = 100000,
    ) -> pd.DataFrame:
        """Fetch all records (handles pagination). Returns a DataFrame."""
        ...

    def get_sample(self, dataset: Dataset, n: int = 5) -> pd.DataFrame:
        """Fetch a small sample for inspection."""
        return self.fetch(dataset, limit=n)

    def get_schema(self, dataset: Dataset) -> list[Column]:
        """Get column schema, preferring metadata then falling back to sampling."""
        if dataset.columns:
            return dataset.columns
        sample = self.get_sample(dataset, n=2)
        return [
            Column(name=col, data_type=str(sample[col].dtype))
            for col in sample.columns
        ]
