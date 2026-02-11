"""CLI interface for CMS Data Explorer."""

from __future__ import annotations

import json

import click
from rich.console import Console
from rich.table import Table

from cms_data_explorer.cache import CacheManager
from cms_data_explorer.clients.cms_api import CMSDataApiClient
from cms_data_explorer.clients.npi import NPIClient
from cms_data_explorer.clients.soda import SodaClient
from cms_data_explorer.config import Config
from cms_data_explorer.engine.duckdb_engine import QueryEngine
from cms_data_explorer.registry.catalog import DatasetCatalog
from cms_data_explorer.registry.models import ApiPlatform

console = Console()


def _get_config():
    return Config.from_env()


def _get_catalog():
    return DatasetCatalog()


def _get_client(platform: ApiPlatform, config: Config):
    match platform:
        case ApiPlatform.SODA:
            return SodaClient(app_token=config.socrata_app_token)
        case ApiPlatform.CMS_DATA_API:
            return CMSDataApiClient()
        case ApiPlatform.NPI:
            return NPIClient()
        case _:
            return SodaClient(app_token=config.socrata_app_token)


@click.group()
def main():
    """CMS Data Explorer - Query public healthcare datasets from the command line."""
    pass


@main.command()
@click.argument("query", default="")
@click.option("--domain", "-d", default="", help="Filter by data domain")
@click.option("--limit", "-l", default=10, help="Max results")
def search(query: str, domain: str, limit: int):
    """Search available datasets by keyword or domain."""
    catalog = _get_catalog()
    results = catalog.search(query=query, domain=domain, limit=limit)

    if not results:
        console.print("[yellow]No datasets found.[/yellow] Try broader search terms.")
        return

    table = Table(title=f"Datasets matching '{query}'")
    table.add_column("ID", style="cyan")
    table.add_column("Title", style="bold")
    table.add_column("Domain")
    table.add_column("Platform")
    table.add_column("Join Keys")

    for ds in results:
        table.add_row(
            ds.id[:15],
            ds.title[:50],
            ds.data_domain.value,
            ds.platform.value,
            ", ".join(ds.join_keys),
        )

    console.print(table)


@main.command()
@click.argument("dataset_id")
def describe(dataset_id: str):
    """Show detailed info about a dataset."""
    catalog = _get_catalog()
    ds = catalog.get(dataset_id)

    if not ds:
        console.print(f"[red]Dataset '{dataset_id}' not found.[/red]")
        console.print("Available IDs:", ", ".join(d.id for d in catalog.list_all()))
        return

    console.print(f"\n[bold]{ds.title}[/bold]")
    console.print(f"[dim]{ds.description}[/dim]\n")
    console.print(f"ID: {ds.id}")
    console.print(f"Domain: {ds.domain}")
    console.print(f"Platform: {ds.platform.value}")
    console.print(f"Endpoint: {ds.api_endpoint}")
    if ds.notes:
        console.print(f"Notes: {ds.notes}")

    if ds.columns:
        table = Table(title="Columns")
        table.add_column("Name", style="cyan")
        table.add_column("Type")
        table.add_column("Description")
        table.add_column("Example", style="dim")

        for col in ds.columns:
            table.add_row(col.name, col.data_type, col.description[:60], col.example)

        console.print(table)

    joinable = catalog.get_joinable(dataset_id)
    if joinable:
        console.print("\n[bold]Joinable Datasets:[/bold]")
        for jds, key in joinable:
            console.print(f"  - {jds.title} (join on: {key})")


@main.command(name="fetch")
@click.argument("dataset_id")
@click.option("--filter", "-f", "filters", multiple=True, help="Filters as key=value")
@click.option("--limit", "-l", default=20, help="Max records")
@click.option("--format", "-o", "output_format", type=click.Choice(["table", "csv", "json"]), default="table")
def fetch_data(dataset_id: str, filters: tuple, limit: int, output_format: str):
    """Fetch data from a dataset."""
    config = _get_config()
    catalog = _get_catalog()
    ds = catalog.get(dataset_id)

    if not ds:
        console.print(f"[red]Dataset '{dataset_id}' not found.[/red]")
        return

    client = _get_client(ds.platform, config)

    params = {}
    for f in filters:
        if "=" in f:
            key, value = f.split("=", 1)
            params[key] = value

    try:
        df = client.fetch(ds, params=params, limit=limit)
    except Exception as e:
        console.print(f"[red]Error fetching data: {e}[/red]")
        return

    if df.empty:
        console.print("[yellow]No data returned.[/yellow]")
        return

    if output_format == "json":
        console.print(df.to_json(orient="records", indent=2))
    elif output_format == "csv":
        console.print(df.to_csv(index=False))
    else:
        table = Table(title=f"{ds.title} ({len(df)} rows)")
        for col in df.columns[:10]:  # Limit columns for readability
            table.add_column(str(col), max_width=30)
        for _, row in df.head(limit).iterrows():
            table.add_row(*[str(v)[:30] for v in row.values[:10]])
        console.print(table)


@main.command()
@click.option("--npi", default="", help="NPI number (10 digits)")
@click.option("--last-name", default="", help="Provider last name")
@click.option("--first-name", default="", help="Provider first name")
@click.option("--state", default="", help="State abbreviation")
@click.option("--specialty", default="", help="Specialty/taxonomy")
@click.option("--limit", "-l", default=10, help="Max results")
def provider(npi: str, last_name: str, first_name: str, state: str, specialty: str, limit: int):
    """Look up a healthcare provider in the NPI Registry."""
    client = NPIClient()

    try:
        df = client.search(
            number=npi,
            last_name=last_name,
            first_name=first_name,
            state=state,
            taxonomy_description=specialty,
            limit=limit,
        )
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        return

    if df.empty:
        console.print("[yellow]No providers found.[/yellow]")
        return

    display_cols = ["npi", "basic_first_name", "basic_last_name", "basic_credential",
                    "practice_city", "practice_state", "taxonomy_desc"]
    available = [c for c in display_cols if c in df.columns]

    table = Table(title=f"Providers ({len(df)} results)")
    for col in available:
        table.add_column(col.replace("basic_", "").replace("practice_", "").replace("taxonomy_", ""))
    for _, row in df[available].iterrows():
        table.add_row(*[str(v) for v in row.values])

    console.print(table)


@main.command()
def cache():
    """Show cache statistics."""
    config = _get_config()
    cm = CacheManager(config.cache_dir)
    stats = cm.stats()

    console.print(f"Cache directory: {stats['cache_dir']}")
    console.print(f"Total entries: {stats['total_entries']}")
    console.print(f"Total size: {stats['total_size_mb']} MB")
    console.print(f"Unique datasets: {stats['unique_datasets']}")


@main.command()
def serve():
    """Start the MCP server for Claude Code integration."""
    from cms_data_explorer.mcp_server import mcp as mcp_server
    mcp_server.run()


@main.command()
def datasets():
    """List all available datasets."""
    catalog = _get_catalog()
    all_ds = catalog.list_all()

    table = Table(title=f"All Available Datasets ({len(all_ds)})")
    table.add_column("ID", style="cyan")
    table.add_column("Title", style="bold")
    table.add_column("Domain")
    table.add_column("Platform")

    for ds in all_ds:
        table.add_row(ds.id[:20], ds.title[:50], ds.data_domain.value, ds.platform.value)

    console.print(table)


if __name__ == "__main__":
    main()
