"""DuckDB-based query engine for analytical SQL on CMS data."""

from __future__ import annotations

import logging

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


class QueryEngine:
    """DuckDB-based query engine for SQL analytics on CMS data.

    Supports registering DataFrames and Parquet/CSV files as virtual tables,
    then querying them with full SQL (JOINs, GROUP BY, window functions, CTEs).
    """

    def __init__(self) -> None:
        self._conn = duckdb.connect()
        self._registered_tables: dict[str, dict] = {}

    def register_dataframe(self, name: str, df: pd.DataFrame) -> str:
        """Register a pandas DataFrame as a queryable table.

        Args:
            name: Table name for SQL queries.
            df: DataFrame to register.

        Returns:
            The table name.
        """
        # DuckDB can query DataFrames directly when registered
        self._conn.register(name, df)
        self._registered_tables[name] = {
            "source": "dataframe",
            "rows": len(df),
            "columns": list(df.columns),
        }
        logger.info(f"Registered table '{name}' ({len(df)} rows, {len(df.columns)} columns)")
        return name

    def register_parquet(self, name: str, parquet_path: str) -> str:
        """Register a Parquet file as a virtual table (lazy, no memory load).

        Args:
            name: Table name for SQL queries.
            parquet_path: Path to the Parquet file.

        Returns:
            The table name.
        """
        self._conn.execute(
            f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{parquet_path}')"
        )
        # Get row count and columns
        info = self._conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()
        cols = self._conn.execute(f"DESCRIBE {name}").fetchdf()
        self._registered_tables[name] = {
            "source": f"parquet:{parquet_path}",
            "rows": info[0] if info else 0,
            "columns": list(cols["column_name"]) if not cols.empty else [],
        }
        logger.info(f"Registered Parquet table '{name}' from {parquet_path}")
        return name

    def register_csv(self, name: str, csv_path: str) -> str:
        """Register a CSV file as a virtual table.

        Args:
            name: Table name for SQL queries.
            csv_path: Path to the CSV file.

        Returns:
            The table name.
        """
        self._conn.execute(
            f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_csv_auto('{csv_path}')"
        )
        info = self._conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()
        cols = self._conn.execute(f"DESCRIBE {name}").fetchdf()
        self._registered_tables[name] = {
            "source": f"csv:{csv_path}",
            "rows": info[0] if info else 0,
            "columns": list(cols["column_name"]) if not cols.empty else [],
        }
        logger.info(f"Registered CSV table '{name}' from {csv_path}")
        return name

    def query(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame.

        Args:
            sql: SQL query (DuckDB SQL dialect). Supports JOINs, GROUP BY,
                 window functions, CTEs, subqueries, etc.

        Returns:
            Query results as a pandas DataFrame.
        """
        logger.info(f"Executing SQL: {sql[:200]}...")
        result = self._conn.execute(sql)
        return result.fetchdf()

    def describe_table(self, name: str) -> pd.DataFrame:
        """Get column names and types for a registered table.

        Args:
            name: Table name.

        Returns:
            DataFrame with column_name, column_type, null, key, default, extra.
        """
        return self._conn.execute(f"DESCRIBE {name}").fetchdf()

    def list_tables(self) -> dict[str, dict]:
        """List all registered tables with metadata.

        Returns:
            Dict mapping table name to {source, rows, columns}.
        """
        return dict(self._registered_tables)

    def sample(self, name: str, n: int = 5) -> pd.DataFrame:
        """Get a sample of rows from a registered table.

        Args:
            name: Table name.
            n: Number of rows to sample.

        Returns:
            Sample rows as a DataFrame.
        """
        return self._conn.execute(f"SELECT * FROM {name} LIMIT {n}").fetchdf()

    def count(self, name: str) -> int:
        """Get the row count of a registered table."""
        result = self._conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()
        return result[0] if result else 0

    def close(self) -> None:
        """Close the DuckDB connection."""
        self._conn.close()
