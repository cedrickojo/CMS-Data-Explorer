# CMS Data Explorer - Claude Code Guide

## What This Is

A tool for querying publicly available CMS healthcare datasets. It runs as an MCP server that gives Claude Code 8 tools for searching, querying, and analyzing healthcare data.

## MCP Server

The `.mcp.json` in the project root auto-registers the MCP server. Once connected, you have these tools:

1. `search_datasets(query, domain)` — Find relevant datasets
2. `describe_dataset(dataset_id)` — Get schema, columns, join info
3. `query_dataset(dataset_id, filters, columns, limit, where)` — Fetch data with filtering
4. `load_dataset(dataset_id, table_name, filters)` — Load into DuckDB for SQL
5. `run_sql(sql)` — Execute SQL across loaded tables
6. `list_loaded_tables()` — See available SQL tables
7. `lookup_provider(npi, name, state, specialty)` — NPI Registry search
8. `manage_cache(action)` — Cache management

## Typical Workflow

1. Search for relevant datasets with `search_datasets()`
2. Understand the schema with `describe_dataset()`
3. For simple queries: use `query_dataset()` directly
4. For complex analysis: `load_dataset()` then `run_sql()` with DuckDB SQL
5. Use `lookup_provider()` to resolve NPI numbers to names/details

## Key Join Keys

- **facility_id**: Links Hospital Info ↔ Quality Measures ↔ Spending ↔ Readmissions
- **npi**: Links NPI Registry ↔ Physician Utilization ↔ Part D Prescribers ↔ Open Payments
- **rndrng_prvdr_ccn**: Links IPPS Summary ↔ Hospital Info (= facility_id)

## Development

```bash
pip install -e .                    # Install
PYTHONPATH=src python -m pytest     # Run tests
cms-explorer search "hospital"      # CLI test
```

### Project Structure

```
src/cms_data_explorer/
  mcp_server.py          # MCP server (8 tools) - main entry point
  config.py              # Environment-based configuration
  cache.py               # Parquet-based cache manager
  registry/
    catalog.py           # Dataset search and discovery
    models.py            # Dataset, Column, ApiPlatform models
    seed_catalog.json    # Curated metadata for 12 CMS datasets
  clients/
    soda.py              # Socrata/SODA API (data.medicare.gov, etc.)
    cms_api.py           # CMS Data API (data.cms.gov)
    npi.py               # NPI Registry API
    bulk.py              # Bulk CSV/ZIP downloads
  engine/
    duckdb_engine.py     # DuckDB query engine for SQL analytics
  cli.py                 # Click-based CLI
```

### Adding New Datasets

Edit `src/cms_data_explorer/registry/seed_catalog.json`. Each entry needs:
- `id`: Four-by-four ID (SODA) or UUID (CMS Data API)
- `title`, `description`
- `domain`: API hostname (e.g., `data.medicare.gov`)
- `platform`: `soda`, `cms_data_api`, `npi`, or `bulk`
- `api_endpoint`: Full URL
- `columns`: Array of `{name, description, data_type, example}`
- `join_keys`: Column names used for cross-dataset joins
