"""NPI Registry API client."""

from __future__ import annotations

import logging

import pandas as pd
import requests

from cms_data_explorer.clients.base import BaseClient
from cms_data_explorer.registry.models import Column, Dataset

logger = logging.getLogger(__name__)


class NPIClient(BaseClient):
    """Client for the NPPES NPI Registry API.

    API docs: https://npiregistry.cms.hhs.gov/api-page
    Max 200 results per query. No authentication required.
    """

    BASE_URL = "https://npiregistry.cms.hhs.gov/api/"
    API_VERSION = "2.1"

    def __init__(self) -> None:
        self._session = requests.Session()

    def search(
        self,
        number: str = "",
        first_name: str = "",
        last_name: str = "",
        organization_name: str = "",
        city: str = "",
        state: str = "",
        postal_code: str = "",
        taxonomy_description: str = "",
        enumeration_type: str = "",
        limit: int = 200,
    ) -> pd.DataFrame:
        """Search the NPI Registry.

        Args:
            number: NPI number (10 digits) for exact lookup.
            first_name: Provider first name.
            last_name: Provider last name.
            organization_name: Organization name (Type 2 NPI).
            city: City name.
            state: State abbreviation (e.g., 'CA').
            postal_code: ZIP code (5 or 9 digits).
            taxonomy_description: Specialty (e.g., 'Internal Medicine').
            enumeration_type: 'NPI-1' (individual) or 'NPI-2' (organization).
            limit: Max results (API max is 200).

        Returns:
            Flattened DataFrame of provider information.
        """
        params: dict = {"version": self.API_VERSION, "limit": min(limit, 200)}

        param_map = {
            "number": number,
            "first_name": first_name,
            "last_name": last_name,
            "organization_name": organization_name,
            "city": city,
            "state": state,
            "postal_code": postal_code,
            "taxonomy_description": taxonomy_description,
            "enumeration_type": enumeration_type,
        }
        for key, value in param_map.items():
            if value:
                params[key] = value

        resp = self._session.get(self.BASE_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if "results" in data and data["results"]:
            return self._flatten_results(data["results"])
        return pd.DataFrame()

    def lookup(self, npi: str) -> pd.DataFrame:
        """Look up a single NPI number."""
        return self.search(number=npi)

    def fetch(
        self,
        dataset: Dataset,
        params: dict | None = None,
        limit: int = 200,
        offset: int = 0,
    ) -> pd.DataFrame:
        """Fetch from NPI registry using generic params dict."""
        if not params:
            return pd.DataFrame()
        return self.search(**params, limit=limit)

    def fetch_all(
        self,
        dataset: Dataset,
        params: dict | None = None,
        max_records: int = 200,
    ) -> pd.DataFrame:
        """NPI API doesn't support pagination beyond 200 results."""
        return self.fetch(dataset, params=params, limit=min(max_records, 200))

    def _flatten_results(self, results: list[dict]) -> pd.DataFrame:
        """Flatten nested NPI response into a flat DataFrame."""
        rows = []
        for r in results:
            row: dict = {"npi": r.get("number", "")}

            basic = r.get("basic", {})
            for k, v in basic.items():
                row[f"basic_{k}"] = v

            addresses = r.get("addresses", [])
            for addr in addresses:
                if addr.get("address_purpose") == "LOCATION":
                    for k, v in addr.items():
                        row[f"practice_{k}"] = v
                    break

            taxonomies = r.get("taxonomies", [])
            if taxonomies:
                for k, v in taxonomies[0].items():
                    row[f"taxonomy_{k}"] = v

            row["enumeration_type"] = r.get("enumeration_type", "")
            rows.append(row)

        return pd.DataFrame(rows)

    def get_schema(self, dataset: Dataset) -> list[Column]:
        """Return schema for NPI data."""
        return [
            Column(name="npi", description="NPI number", data_type="text"),
            Column(name="basic_first_name", description="First name", data_type="text"),
            Column(name="basic_last_name", description="Last name", data_type="text"),
            Column(name="basic_organization_name", description="Organization name", data_type="text"),
            Column(name="basic_credential", description="Credentials", data_type="text"),
            Column(name="basic_status", description="Status (A=Active)", data_type="text"),
            Column(name="practice_state", description="Practice state", data_type="text"),
            Column(name="practice_city", description="Practice city", data_type="text"),
            Column(name="taxonomy_desc", description="Specialty/taxonomy", data_type="text"),
            Column(name="enumeration_type", description="NPI-1 or NPI-2", data_type="text"),
        ]
