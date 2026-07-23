"""Command-line interface: `delta2ducklake bootstrap|copy|sync|refresh-stats`."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from delta2ducklake.convert import copy_table, sync_table
from delta2ducklake.ducklake.bootstrap import bootstrap_catalog
from delta2ducklake.ducklake.catalog import (
    CatalogConfig,
    DuckDBCatalogConfig,
    PostgresCatalogConfig,
    QuackCatalogConfig,
    SQLiteCatalogConfig,
)
from delta2ducklake.ducklake.stats_refresh import refresh_stats


def _parse_catalog(spec: str) -> CatalogConfig:
    if spec.startswith("duckdb:"):
        return DuckDBCatalogConfig(spec.removeprefix("duckdb:"))
    if spec.startswith("sqlite:"):
        return SQLiteCatalogConfig(spec.removeprefix("sqlite:"))
    if spec.startswith("postgres:"):
        return PostgresCatalogConfig(spec.removeprefix("postgres:"))
    if spec.startswith("quack:"):
        return QuackCatalogConfig(spec.removeprefix("quack:"))
    raise argparse.ArgumentTypeError(
        f"--catalog must start with 'duckdb:', 'sqlite:', 'postgres:', or 'quack:', got {spec!r}"
    )


def _add_catalog_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--catalog",
        required=True,
        type=_parse_catalog,
        metavar="duckdb:<path>|sqlite:<path>|postgres:<libpq DSN>|quack:<host:port>",
        help="DuckLake catalog location, e.g. 'duckdb:./catalog.ducklake', 'sqlite:./catalog.db', "
        "'postgres:host=localhost dbname=mydb user=me password=...', or 'quack:localhost:9494' "
        "(quack: is experimental -- see README for its current limitations)",
    )
    parser.add_argument(
        "--entra-user",
        default=None,
        metavar="USER",
        help="Postgres catalog only: authenticate to Azure Database for PostgreSQL as this user "
        "with an Entra ID token instead of a static password (requires delta2ducklake[azure])",
    )
    parser.add_argument(
        "--quack-token",
        default=None,
        metavar="TOKEN",
        help="Quack catalog only: shared secret token configured on the quack_serve(...) side",
    )
    parser.add_argument(
        "--managed-identity",
        action="store_true",
        help="With --entra-user, fetch the Entra ID token via ManagedIdentityCredential instead "
        "of DefaultAzureCredential",
    )


def _resolve_catalog(args: argparse.Namespace) -> CatalogConfig:
    catalog = args.catalog
    if args.entra_user is not None:
        if not isinstance(catalog, PostgresCatalogConfig):
            raise argparse.ArgumentTypeError(
                "--entra-user only applies to a 'postgres:' --catalog"
            )
        catalog = PostgresCatalogConfig(
            dsn=catalog.dsn, entra_user=args.entra_user, managed_identity=args.managed_identity
        )
    if args.quack_token is not None:
        if not isinstance(catalog, QuackCatalogConfig):
            raise argparse.ArgumentTypeError("--quack-token only applies to a 'quack:' --catalog")
        catalog = QuackCatalogConfig(endpoint=catalog.endpoint, token=args.quack_token)
    return catalog


def _add_table_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--table", required=True, help="DuckLake table name")
    parser.add_argument("--schema", default="main", help="DuckLake schema name (default: main)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="delta2ducklake",
        description="Register Delta Lake tables into a DuckLake catalog without copying data.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    bootstrap_parser = subparsers.add_parser(
        "bootstrap", help="Initialize a DuckLake catalog (one-time setup, safe to re-run)"
    )
    _add_catalog_arg(bootstrap_parser)
    bootstrap_parser.add_argument(
        "--data-path", required=True, help="DuckLake-managed data directory (local path or URL)"
    )

    copy_parser = subparsers.add_parser(
        "copy", help="Register a Delta table's current state as a brand-new DuckLake table"
    )
    copy_parser.add_argument(
        "delta_table_root", help="Path/URL to the Delta table's root directory"
    )
    _add_catalog_arg(copy_parser)
    _add_table_args(copy_parser)
    copy_parser.add_argument(
        "--version", type=int, default=None, metavar="N",
        help="Delta version to copy (default: latest)",
    )

    sync_parser = subparsers.add_parser(
        "sync",
        help="Update a DuckLake table (from a previous 'copy') to the Delta table's current state",
    )
    sync_parser.add_argument(
        "delta_table_root", help="Path/URL to the Delta table's root directory"
    )
    _add_catalog_arg(sync_parser)
    _add_table_args(sync_parser)
    sync_parser.add_argument(
        "--version", type=int, default=None, metavar="N",
        help="Delta version to sync to (default: latest)",
    )

    stats_parser = subparsers.add_parser(
        "refresh-stats", help="(Re)compute stats for some or all columns of a DuckLake table"
    )
    _add_catalog_arg(stats_parser)
    _add_table_args(stats_parser)
    stats_parser.add_argument(
        "--columns", default=None, metavar="a,b,c",
        help="Comma-separated top-level column names (default: every non-container column)",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        catalog = _resolve_catalog(args)
        if args.command == "bootstrap":
            bootstrap_catalog(catalog, args.data_path)
            print(f"Bootstrapped DuckLake catalog (data_path={args.data_path!r})")
        elif args.command == "copy":
            table_id = copy_table(
                args.delta_table_root, catalog, args.table,
                schema_name=args.schema, end_version=args.version,
            )
            print(f"Registered {args.schema}.{args.table} (table_id={table_id})")
        elif args.command == "sync":
            table_id = sync_table(
                args.delta_table_root, catalog, args.table,
                schema_name=args.schema, end_version=args.version,
            )
            print(f"Synced {args.schema}.{args.table} (table_id={table_id})")
        elif args.command == "refresh-stats":
            columns = args.columns.split(",") if args.columns else None
            refresh_stats(catalog, args.table, schema_name=args.schema, columns=columns)
            print(f"Refreshed stats for {args.schema}.{args.table}")
    except (ValueError, NotImplementedError, RuntimeError, argparse.ArgumentTypeError) as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
