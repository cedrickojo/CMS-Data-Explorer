"""Dataset catalog for discovering and searching CMS datasets."""

from __future__ import annotations

import json
from pathlib import Path

from cms_data_explorer.registry.models import ApiPlatform, Column, DataDomain, Dataset


class DatasetCatalog:
    """Registry of known CMS datasets with search capability."""

    def __init__(self) -> None:
        self._datasets: dict[str, Dataset] = {}
        self._load_seed_catalog()

    def _load_seed_catalog(self) -> None:
        """Load the pre-built catalog from seed_catalog.json."""
        seed_path = Path(__file__).parent / "seed_catalog.json"
        with open(seed_path) as f:
            raw = json.load(f)
        for entry in raw:
            columns = [Column(**c) for c in entry.pop("columns", [])]
            ds = Dataset(columns=columns, **entry)
            self._datasets[ds.id] = ds

    def search(
        self,
        query: str = "",
        domain: str = "",
        limit: int = 20,
    ) -> list[Dataset]:
        """Search datasets by keyword, domain, or platform.

        Args:
            query: Free-text search matching title, description, keywords.
            domain: Filter by DataDomain value (e.g. 'hospital_compare').
            limit: Max results to return.
        """
        results = []
        query_lower = query.lower()

        for ds in self._datasets.values():
            # Domain filter
            if domain:
                try:
                    target_domain = DataDomain(domain)
                except ValueError:
                    target_domain = None
                if target_domain and ds.data_domain != target_domain:
                    continue
                if not target_domain and domain.lower() not in ds.domain.lower():
                    continue

            # Keyword search
            if query_lower:
                searchable = " ".join(
                    [
                        ds.title.lower(),
                        ds.description.lower(),
                        " ".join(k.lower() for k in ds.keywords),
                        ds.data_domain.value.lower(),
                        ds.notes.lower(),
                    ]
                )
                if query_lower not in searchable:
                    words = query_lower.split()
                    if not all(w in searchable for w in words):
                        continue

            results.append(ds)
            if len(results) >= limit:
                break

        return results

    def get(self, dataset_id: str) -> Dataset | None:
        """Get a specific dataset by ID."""
        return self._datasets.get(dataset_id)

    def list_all(self) -> list[Dataset]:
        """List all known datasets."""
        return list(self._datasets.values())

    def get_joinable(self, dataset_id: str) -> list[tuple[Dataset, str]]:
        """Find datasets that can be joined with the given dataset.

        Returns list of (dataset, join_key) tuples.
        """
        source = self._datasets.get(dataset_id)
        if not source:
            return []

        joinable = []
        for ds in self._datasets.values():
            if ds.id == dataset_id:
                continue
            for key in source.join_keys:
                if key in ds.join_keys:
                    joinable.append((ds, key))
                    break
                ds_col_names = [c.name for c in ds.columns]
                if any(key in col_name for col_name in ds_col_names):
                    joinable.append((ds, key))
                    break

        return joinable
