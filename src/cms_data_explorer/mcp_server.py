"""MCP server exposing CMS data exploration tools to Claude Code."""

from __future__ import annotations

import json
import logging
import traceback
from typing import Any

from mcp.server.fastmcp import FastMCP

from cms_data_explorer.cache import CacheManager
from cms_data_explorer.clients.cms_api import CMSDataApiClient
from cms_data_explorer.clients.npi import NPIClient
from cms_data_explorer.clients.soda import SodaClient
from cms_data_explorer.config import Config
from cms_data_explorer.engine.duckdb_engine import QueryEngine
from cms_data_explorer.registry.catalog import DatasetCatalog
from cms_data_explorer.registry.models import ApiPlatform

logger = logging.getLogger(__name__)

# Initialize global state
config = Config.from_env()
catalog = DatasetCatalog()
cache = CacheManager(config.cache_dir)
engine = QueryEngine()
soda_client = SodaClient(app_token=config.socrata_app_token)
cms_client = CMSDataApiClient()
npi_client = NPIClient()

mcp = FastMCP(
    "CMS Data Explorer",
    instructions="""You have access to tools for querying publicly available CMS
(Centers for Medicare & Medicaid Services) healthcare datasets. Use these tools to
answer questions about hospitals, physicians, nursing homes, Medicare spending,
drug prescriptions, and more.

Typical workflow:
1. search_datasets() to find relevant datasets
2. describe_dataset() to understand the schema
3. query_dataset() for simple queries, or load_dataset() + run_sql() for complex analysis
4. lookup_provider() to resolve NPI numbers to provider details

Key join relationships:
- facility_id links Hospital General Info ↔ Quality Measures ↔ Spending ↔ Readmissions
- NPI links NPI Registry ↔ Medicare Physician Utilization ↔ Part D Prescribers ↔ Open Payments
- rndrng_prvdr_ccn links IPPS Provider Summary ↔ Hospital General Info (via facility_id)
""",
)


def _get_client(platform: ApiPlatform):
    """Return the appropriate client for a dataset's platform."""
    match platform:
        case ApiPlatform.SODA:
            return soda_client
        case ApiPlatform.CMS_DATA_API:
            return cms_client
        case ApiPlatform.NPI:
            return npi_client
        case _:
            return soda_client  # Default fallback


def _df_to_result(df, max_rows: int = 100) -> dict[str, Any]:
    """Convert a DataFrame to a serializable result dict."""
    truncated = len(df) > max_rows
    display_df = df.head(max_rows)

    return {
        "total_rows": len(df),
        "displayed_rows": len(display_df),
        "truncated": truncated,
        "columns": list(df.columns),
        "data": display_df.to_dict(orient="records"),
    }


@mcp.tool()
def search_datasets(
    query: str = "",
    domain: str = "",
    limit: int = 10,
) -> str:
    """Search available CMS healthcare datasets by keyword or domain.

    Returns dataset metadata including title, description, and available
    columns. Use this to discover what data is available before querying.

    Args:
        query: Search term (e.g., "hospital ratings", "Part D prescriber",
               "nursing home staffing", "opioid", "spending").
        domain: Filter by data domain. Options: hospital_compare, nursing_home,
                medicare_provider, medicare_part_d, open_payments, medicaid,
                npi_registry, quality_measures, spending, hospital_readmissions.
        limit: Max results to return (default 10).
    """
    results = catalog.search(query=query, domain=domain, limit=limit)

    if not results:
        return json.dumps({
            "message": "No datasets found. Try broader search terms.",
            "available_domains": [d.value for d in catalog.list_all()[0].data_domain.__class__],
            "tip": "Try: 'hospital', 'nursing home', 'Medicare', 'drug', 'provider', 'spending'",
        })

    output = []
    for ds in results:
        output.append({
            "id": ds.id,
            "title": ds.title,
            "description": ds.description[:200],
            "domain": ds.domain,
            "platform": ds.platform.value,
            "data_domain": ds.data_domain.value,
            "key_columns": [c.name for c in ds.columns[:8]],
            "join_keys": ds.join_keys,
            "notes": ds.notes[:150] if ds.notes else "",
        })

    return json.dumps({"count": len(output), "datasets": output}, indent=2)


@mcp.tool()
def describe_dataset(dataset_id: str) -> str:
    """Get detailed metadata for a specific dataset.

    Shows all columns with types, descriptions, and examples. Also shows
    which other datasets can be joined with this one.

    Args:
        dataset_id: The dataset identifier (e.g., 'xubh-q36u' for Hospital
                    General Information, 'npi_registry' for NPI lookup).
    """
    ds = catalog.get(dataset_id)
    if not ds:
        available = [d.id for d in catalog.list_all()]
        return json.dumps({
            "error": f"Dataset '{dataset_id}' not found.",
            "available_ids": available,
        })

    joinable = catalog.get_joinable(dataset_id)

    return json.dumps({
        "id": ds.id,
        "title": ds.title,
        "description": ds.description,
        "domain": ds.domain,
        "platform": ds.platform.value,
        "api_endpoint": ds.api_endpoint,
        "temporal": ds.temporal,
        "notes": ds.notes,
        "columns": [
            {
                "name": c.name,
                "description": c.description,
                "type": c.data_type,
                "example": c.example,
            }
            for c in ds.columns
        ],
        "join_keys": ds.join_keys,
        "joinable_datasets": [
            {"id": jds.id, "title": jds.title, "join_key": jkey}
            for jds, jkey in joinable
        ],
    }, indent=2)


@mcp.tool()
def query_dataset(
    dataset_id: str,
    filters: str = "{}",
    columns: str = "[]",
    limit: int = 100,
    offset: int = 0,
    order_by: str = "",
    where: str = "",
) -> str:
    """Fetch data from a CMS dataset with optional filtering.

    For SODA datasets, you can use SoQL syntax in the 'where' parameter
    for complex queries. For simple equality filters, use the 'filters' param.

    Args:
        dataset_id: Dataset identifier (e.g., 'xubh-q36u').
        filters: JSON string of key-value filter pairs.
                 Example: '{"state": "CA"}' or '{"hospital_type": "Acute Care Hospitals"}'.
        columns: JSON string of column names to return.
                 Example: '["facility_name", "state", "hospital_overall_rating"]'.
                 Empty list returns all columns.
        limit: Max records to return (default 100, max 50000).
        offset: Starting record for pagination.
        order_by: Column to sort by. Prefix with '-' for descending.
                  Example: 'hospital_overall_rating' or '-total_amount_reimbursed'.
        where: SoQL WHERE clause for complex filtering (SODA datasets only).
               Example: "state='CA' AND hospital_overall_rating > '3'".
    """
    ds = catalog.get(dataset_id)
    if not ds:
        return json.dumps({"error": f"Dataset '{dataset_id}' not found."})

    try:
        filter_dict = json.loads(filters) if isinstance(filters, str) else filters
        column_list = json.loads(columns) if isinstance(columns, str) else columns
    except json.JSONDecodeError as e:
        return json.dumps({"error": f"Invalid JSON: {e}"})

    client = _get_client(ds.platform)
    params = dict(filter_dict) if filter_dict else {}

    # Build SoQL params for SODA datasets
    if ds.platform == ApiPlatform.SODA:
        if column_list:
            params["$select"] = ",".join(column_list)
        if where:
            params["$where"] = where
        if order_by:
            if order_by.startswith("-"):
                params["$order"] = f"{order_by[1:]} DESC"
            else:
                params["$order"] = f"{order_by} ASC"

    # Check cache first
    cache_params = {**params, "_limit": limit, "_offset": offset}
    cached = cache.get_cached_df(dataset_id, cache_params)
    if cached is not None:
        return json.dumps(_df_to_result(cached, max_rows=limit))

    try:
        df = client.fetch(ds, params=params, limit=limit, offset=offset)
    except Exception as e:
        return json.dumps({"error": f"API request failed: {e}", "traceback": traceback.format_exc()})

    if not df.empty:
        # Apply column selection for non-SODA datasets
        if column_list and ds.platform != ApiPlatform.SODA:
            available = [c for c in column_list if c in df.columns]
            if available:
                df = df[available]

        # Cache the result
        cache.cache_df(dataset_id, df, cache_params)

    return json.dumps(_df_to_result(df, max_rows=limit))


@mcp.tool()
def load_dataset(
    dataset_id: str,
    table_name: str = "",
    filters: str = "{}",
    max_records: int = 50000,
) -> str:
    """Download a dataset and register it as a SQL table for run_sql() queries.

    Use this before run_sql() to make datasets available for complex SQL
    analysis including JOINs, GROUP BY, and window functions. Results are
    cached locally as Parquet files for fast re-use.

    Args:
        dataset_id: Dataset identifier (e.g., 'xubh-q36u').
        table_name: Name for the SQL table. Auto-generated if empty.
        filters: JSON string of pre-filters to apply during download.
                 Example: '{"state": "CA"}'.
        max_records: Maximum records to download (default 50000).
    """
    ds = catalog.get(dataset_id)
    if not ds:
        return json.dumps({"error": f"Dataset '{dataset_id}' not found."})

    try:
        filter_dict = json.loads(filters) if isinstance(filters, str) else filters
    except json.JSONDecodeError as e:
        return json.dumps({"error": f"Invalid JSON: {e}"})

    if not table_name:
        table_name = ds.title.lower().replace(" ", "_").replace("-", "_")[:40]
        # Clean to valid SQL identifier
        table_name = "".join(c if c.isalnum() or c == "_" else "_" for c in table_name)

    # Check cache
    cache_params = {**(filter_dict or {}), "_max_records": max_records}
    cached = cache.get_cached_df(dataset_id, cache_params)

    if cached is not None:
        df = cached
    else:
        client = _get_client(ds.platform)
        params = dict(filter_dict) if filter_dict else {}

        try:
            df = client.fetch_all(ds, params=params, max_records=max_records)
        except Exception as e:
            return json.dumps({"error": f"Failed to fetch data: {e}", "traceback": traceback.format_exc()})

        if not df.empty:
            cache.cache_df(dataset_id, df, cache_params)

    if df.empty:
        return json.dumps({"error": "No data returned. Try different filters."})

    engine.register_dataframe(table_name, df)

    return json.dumps({
        "table_name": table_name,
        "rows": len(df),
        "columns": list(df.columns),
        "sample": df.head(3).to_dict(orient="records"),
        "tip": f"Use run_sql('SELECT * FROM {table_name} LIMIT 10') to query this table.",
    }, indent=2, default=str)


@mcp.tool()
def run_sql(sql: str) -> str:
    """Execute a SQL query against loaded datasets using DuckDB.

    Datasets must be loaded first with load_dataset(). Use list_loaded_tables()
    to see available tables.

    Supports full DuckDB SQL: JOINs, GROUP BY, window functions, CTEs,
    subqueries, CASE expressions, string functions, date functions, etc.

    Args:
        sql: SQL query to execute. Examples:
             - SELECT * FROM hospitals WHERE state = 'CA' LIMIT 10
             - SELECT state, AVG(CAST(hospital_overall_rating AS INT)) as avg_rating
               FROM hospitals GROUP BY state ORDER BY avg_rating DESC
             - SELECT h.facility_name, s.score FROM hospitals h
               JOIN spending s ON h.facility_id = s.facility_id
    """
    tables = engine.list_tables()
    if not tables:
        return json.dumps({
            "error": "No tables loaded. Use load_dataset() first to load data.",
            "tip": "Example: load_dataset('xubh-q36u', table_name='hospitals')",
        })

    try:
        df = engine.query(sql)
        return json.dumps(_df_to_result(df, max_rows=500), default=str)
    except Exception as e:
        return json.dumps({
            "error": f"SQL error: {e}",
            "available_tables": {
                name: {"rows": info["rows"], "columns": info["columns"]}
                for name, info in tables.items()
            },
            "tip": "Check column names with list_loaded_tables() or describe a specific table.",
        })


@mcp.tool()
def list_loaded_tables() -> str:
    """List all datasets currently loaded as SQL tables.

    Returns table names, row counts, column names, and source info.
    Use this to check what's available before running SQL queries.
    """
    tables = engine.list_tables()

    if not tables:
        return json.dumps({
            "message": "No tables loaded yet.",
            "tip": "Use load_dataset() to load a dataset as a SQL table.",
            "example": "load_dataset('xubh-q36u', table_name='hospitals')",
        })

    result = {}
    for name, info in tables.items():
        result[name] = {
            "rows": info["rows"],
            "columns": info["columns"],
            "source": info["source"],
        }

    return json.dumps(result, indent=2, default=str)


@mcp.tool()
def lookup_provider(
    npi: str = "",
    first_name: str = "",
    last_name: str = "",
    state: str = "",
    city: str = "",
    specialty: str = "",
    organization_name: str = "",
    limit: int = 10,
) -> str:
    """Look up a healthcare provider in the NPI Registry.

    Search by NPI number for exact lookup, or by name, location, and
    specialty for broader searches. Returns provider details including
    name, credentials, address, and taxonomy/specialty.

    Args:
        npi: NPI number (10 digits) for exact lookup.
        first_name: Provider first name.
        last_name: Provider last name.
        state: State abbreviation (e.g., 'CA', 'NY', 'TX').
        city: City name.
        specialty: Taxonomy/specialty (e.g., 'Internal Medicine',
                   'Family Medicine', 'Cardiology').
        organization_name: Organization name (for Type 2 NPIs).
        limit: Max results (API max is 200, default 10).
    """
    if not any([npi, first_name, last_name, state, city, specialty, organization_name]):
        return json.dumps({
            "error": "At least one search parameter required.",
            "params": ["npi", "first_name", "last_name", "state", "city", "specialty", "organization_name"],
        })

    try:
        df = npi_client.search(
            number=npi,
            first_name=first_name,
            last_name=last_name,
            state=state,
            city=city,
            taxonomy_description=specialty,
            organization_name=organization_name,
            limit=limit,
        )
    except Exception as e:
        return json.dumps({"error": f"NPI lookup failed: {e}"})

    if df.empty:
        return json.dumps({"message": "No providers found matching your criteria."})

    # Select key columns for display
    display_cols = [
        "npi", "basic_first_name", "basic_last_name",
        "basic_organization_name", "basic_credential",
        "practice_city", "practice_state",
        "taxonomy_desc", "enumeration_type",
    ]
    available_cols = [c for c in display_cols if c in df.columns]
    display_df = df[available_cols] if available_cols else df

    return json.dumps(_df_to_result(display_df, max_rows=limit), default=str)


@mcp.tool()
def manage_cache(action: str = "stats") -> str:
    """Manage the local data cache.

    Args:
        action: One of:
                - 'stats': Show cache size, entry count, and directory.
                - 'list': Show all cached datasets with metadata.
                - 'clear': Remove all cached data.
    """
    if action == "stats":
        return json.dumps(cache.stats())
    elif action == "list":
        entries = cache.list_cached()
        return json.dumps(entries, indent=2, default=str)
    elif action == "clear":
        removed = cache.clear()
        return json.dumps({"message": f"Cleared {removed} cache entries."})
    else:
        return json.dumps({"error": f"Unknown action '{action}'. Use 'stats', 'list', or 'clear'."})


if __name__ == "__main__":
    mcp.run()
