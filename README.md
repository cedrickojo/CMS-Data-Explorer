# CMS Data Explorer

Query and analyze publicly available CMS (Centers for Medicare & Medicaid Services) healthcare datasets through an MCP server for Claude Code, a CLI, or as a Python library.

## Quick Start

### Install

```bash
pip install -e .
```

### Use with Claude Code (MCP Server)

The `.mcp.json` file in the project root configures the MCP server automatically. Claude Code gets 8 tools:

- **search_datasets** — Find datasets by keyword (e.g., "hospital ratings", "opioid prescribing")
- **describe_dataset** — Get column schemas, join keys, and related datasets
- **query_dataset** — Fetch data with filters, column selection, and SoQL queries
- **load_dataset** — Download a full dataset and register it as a SQL table
- **run_sql** — Execute DuckDB SQL across loaded tables (JOINs, GROUP BY, window functions)
- **list_loaded_tables** — See what's loaded and available for SQL queries
- **lookup_provider** — Search the NPI Registry by name, state, specialty, or NPI number
- **manage_cache** — View/clear the local Parquet cache

### Use from CLI

```bash
# Search for datasets
cms-explorer search "hospital ratings"

# Describe a dataset
cms-explorer describe xubh-q36u

# Fetch data
cms-explorer fetch xubh-q36u -f state=CA -l 10

# Look up a provider
cms-explorer provider --state CA --specialty "Internal Medicine"

# List all available datasets
cms-explorer datasets
```

## Available Datasets

| Dataset | ID | Domain | Join Key |
|---------|-----|--------|----------|
| Hospital General Information | `xubh-q36u` | data.medicare.gov | `facility_id` |
| Nursing Home Provider Info | `4pq5-n9py` | data.medicare.gov | `federal_provider_number` |
| Timely and Effective Care | `9n3s-kdb3` | data.medicare.gov | `facility_id` |
| Medicare Spending/Beneficiary | `nrth-mfg3` | data.medicare.gov | `facility_id` |
| Hospital Readmissions | `yq43-i98g` | data.medicare.gov | `facility_id` |
| NPI Registry | `npi_registry` | npiregistry.cms.hhs.gov | `npi` |
| State Drug Utilization | `e5ds-i36p` | data.medicaid.gov | `ndc`, `state` |
| Medicare Physician Utilization | `mj5m-pzi6` | data.cms.gov | `rndrng_npi` |
| Part D Prescribers by Drug | `fykj-qjee` | data.cms.gov | `prscrbr_npi` |
| Open Payments | `77hc-bjwt` | openpaymentsdata.cms.gov | `covered_recipient_npi` |
| IPPS Provider Summary | `97k6-zzrs` | data.cms.gov | `rndrng_prvdr_ccn` |
| Part D Spending by Drug | `jx8g-mn6j` | data.cms.gov | `brnd_name` |

## Join Relationships

```
Hospital General Info (facility_id)
  ├── Timely & Effective Care
  ├── Medicare Spending/Beneficiary
  ├── Hospital Readmissions
  └── IPPS Provider Summary (via rndrng_prvdr_ccn)

NPI Registry (npi)
  ├── Medicare Physician Utilization (rndrng_npi)
  ├── Part D Prescribers (prscrbr_npi)
  └── Open Payments (covered_recipient_npi)
```

## Architecture

```
MCP Server (8 tools)   ←  Claude Code interface
       ↓
Query Engine (DuckDB)  ←  SQL joins, aggregations
       ↓
API Clients            ←  SODA API, CMS Data API, NPI API
       ↓
Dataset Registry       ←  Curated catalog of 12 datasets
       ↓
Cache (Parquet)        ←  Local cache (~/.cache/cms-data-explorer/)
```

## Configuration

Optional environment variables:

```bash
# Socrata app token for higher rate limits (10k vs 1k requests/hour)
export SOCRATA_APP_TOKEN=your_token_here

# Override cache directory (default: ~/.cache/cms-data-explorer/)
export CMS_CACHE_DIR=/path/to/cache
```

## Example Workflows

**"What are the highest-rated hospitals in California?"**
1. `search_datasets("hospital ratings")` → finds Hospital General Info
2. `query_dataset("xubh-q36u", filters={"state": "CA"}, order_by="-hospital_overall_rating")`

**"Which providers prescribe the most opioids in New York?"**
1. `load_dataset("fykj-qjee", table_name="prescribers", filters={"prscrbr_state_abrvtn": "NY"})`
2. `run_sql("SELECT prscrbr_npi, prscrbr_last_org_name, SUM(CAST(tot_clms AS INT)) as total_claims FROM prescribers WHERE opioid_drug_flag='Y' GROUP BY 1,2 ORDER BY total_claims DESC LIMIT 20")`
3. `lookup_provider(npi="<top NPI>")` for provider details

**"Compare hospital spending vs. readmission rates"**
1. `load_dataset("nrth-mfg3", table_name="spending")`
2. `load_dataset("yq43-i98g", table_name="readmissions")`
3. `run_sql("SELECT s.facility_name, s.score as spending_score, r.score as readmission_rate FROM spending s JOIN readmissions r ON s.facility_id = r.facility_id WHERE s.measure_id='MSPB_1'")`
