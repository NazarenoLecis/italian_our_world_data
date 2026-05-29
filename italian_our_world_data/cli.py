"""Command-line interface for source discovery and retrieval."""

from __future__ import annotations

import argparse
import json
from ast import literal_eval
from typing import Any, Sequence

import pandas as pd

from .gateway import discover_data, fetch_data, list_indicators, list_sources, source_info


def _parse_value(value: str) -> Any:
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered in {"none", "null"}:
        return None
    try:
        return json.loads(value)
    except ValueError:
        pass
    try:
        parsed = literal_eval(value)
    except (ValueError, SyntaxError):
        return value
    return parsed if isinstance(parsed, (str, int, float, bool, list, tuple, dict)) else value


def _parse_params(values: Sequence[str]) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for item in values:
        if "=" not in item:
            raise SystemExit(f"Parameters must use key=value syntax: {item!r}")
        key, value = item.split("=", 1)
        if not key:
            raise SystemExit(f"Parameter key cannot be empty: {item!r}")
        parsed[key] = _parse_value(value)
    return parsed


def _print_frame(frame: pd.DataFrame, output: str, *, head: int | None = None) -> None:
    if head is not None:
        frame = frame.head(head)
    if output == "json":
        print(frame.to_json(orient="records", force_ascii=False, indent=2))
    elif output == "csv":
        print(frame.to_csv(index=False), end="")
    else:
        print(frame.to_string(index=False))


def _print_info(value: Any, output: str) -> None:
    if isinstance(value, pd.DataFrame):
        _print_frame(value, output)
    elif output == "json":
        print(json.dumps(value, ensure_ascii=False, indent=2))
    else:
        rows = [{"field": key, "value": value} for key, value in value.items()]
        print(pd.DataFrame(rows).to_string(index=False))


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""
    parser = argparse.ArgumentParser(
        prog="italian-our-world-data",
        description="Discover and retrieve public data sources relevant to Italy.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    sources = subparsers.add_parser("sources", help="List supported source IDs")
    sources.add_argument("--category", help="Filter by source category")
    sources.add_argument("--format", choices=("table", "csv", "json"), default="table")

    info = subparsers.add_parser("info", help="Show source usage information")
    info.add_argument("source", nargs="?", help="Optional source ID or alias")
    info.add_argument("--format", choices=("table", "json"), default="table")

    indicators = subparsers.add_parser(
        "indicators",
        aliases=["items"],
        help="List usable indicators/items for a source",
    )
    indicators.add_argument(
        "source",
        nargs="?",
        help="Optional source ID or alias. Omit it to see identifier columns for all sources.",
    )
    indicators.add_argument(
        "-p",
        "--param",
        action="append",
        default=[],
        help="Discovery parameter as key=value; repeat for multiple parameters",
    )
    indicators.add_argument("--head", type=int, default=20, help="Rows to print")
    indicators.add_argument("--format", choices=("table", "csv", "json"), default="table")

    discover = subparsers.add_parser("discover", help="List available objects in a source")
    discover.add_argument("source", help="Source ID or alias")
    discover.add_argument(
        "-p",
        "--param",
        action="append",
        default=[],
        help="Discovery parameter as key=value; repeat for multiple parameters",
    )
    discover.add_argument("--head", type=int, default=20, help="Rows to print")
    discover.add_argument("--format", choices=("table", "csv", "json"), default="table")

    fetch = subparsers.add_parser("fetch", help="Fetch data from a source")
    fetch.add_argument("source", help="Source ID or alias")
    fetch.add_argument(
        "-p",
        "--param",
        action="append",
        default=[],
        help="Fetch parameter as key=value; repeat for multiple parameters",
    )
    fetch.add_argument("--head", type=int, default=20, help="Rows to print")
    fetch.add_argument("--format", choices=("table", "csv", "json"), default="table")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command-line interface."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "sources":
        _print_frame(list_sources(category=args.category), args.format)
        return 0
    if args.command == "info":
        _print_info(source_info(args.source), args.format)
        return 0
    if args.command in {"indicators", "items"}:
        frame = list_indicators(args.source, **_parse_params(args.param))
        _print_frame(frame, args.format, head=args.head if args.source else None)
        return 0
    if args.command == "discover":
        frame = discover_data(args.source, **_parse_params(args.param))
        _print_frame(frame, args.format, head=args.head)
        return 0
    if args.command == "fetch":
        frame = fetch_data(args.source, **_parse_params(args.param))
        _print_frame(frame, args.format, head=args.head)
        return 0

    parser.error(f"Unknown command {args.command!r}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
