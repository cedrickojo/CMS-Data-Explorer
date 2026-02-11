"""Data models for the dataset registry."""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class ApiPlatform(str, Enum):
    """API platform that hosts the dataset."""

    SODA = "soda"  # Socrata/SODA (data.medicare.gov, data.medicaid.gov, etc.)
    CMS_DATA_API = "cms_data_api"  # data.cms.gov data-api/v1
    NPI = "npi"  # NPI Registry API
    BULK_DOWNLOAD = "bulk"  # Direct CSV/ZIP download


class DataDomain(str, Enum):
    """CMS program area the dataset belongs to."""

    HOSPITAL_COMPARE = "hospital_compare"
    NURSING_HOME = "nursing_home"
    PHYSICIAN_COMPARE = "physician_compare"
    MEDICARE_PROVIDER = "medicare_provider"
    MEDICARE_PART_D = "medicare_part_d"
    PROGRAM_STATISTICS = "program_statistics"
    OPEN_PAYMENTS = "open_payments"
    MEDICAID = "medicaid"
    NPI_REGISTRY = "npi_registry"
    COST_REPORTS = "cost_reports"
    HOSPITAL_READMISSIONS = "hospital_readmissions"
    QUALITY_MEASURES = "quality_measures"
    SPENDING = "spending"


class Column(BaseModel):
    """Metadata for a dataset column."""

    name: str
    description: str = ""
    data_type: str = "text"  # text, number, date, boolean
    example: str = ""


class Dataset(BaseModel):
    """Metadata for a single CMS dataset."""

    id: str  # Four-by-four for SODA, UUID for CMS Data API, or slug
    title: str
    description: str
    domain: str  # e.g., "data.medicare.gov"
    platform: ApiPlatform
    data_domain: DataDomain
    api_endpoint: str  # Full URL to query data
    columns: list[Column] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    modified: str = ""  # ISO date string
    temporal: str = ""  # Date range description
    record_count: Optional[int] = None
    join_keys: list[str] = Field(default_factory=list)  # e.g., ["npi", "provider_id"]
    notes: str = ""

    @property
    def slug(self) -> str:
        """URL-friendly name derived from title."""
        return self.title.lower().replace(" ", "-").replace("&", "and")[:60]
