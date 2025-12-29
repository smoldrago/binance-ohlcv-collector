"""CLI interface using typer."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.columns import Columns
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from binance_ohlcv_collector import (
    __version__,
    download,
    download_all,
    filter_symbols,
    list_symbols,
)
from binance_ohlcv_collector.config import (
    DEFAULT_CONCURRENCY,
    DEFAULT_OUTPUT_FORMAT,
    DEFAULT_RETRIES,
    DEFAULT_TIMEFRAME,
    DEFAULT_TIMEOUT,
    VALID_OUTPUT_FORMATS,
    VALID_TIMEFRAMES,
    MarketType,
)

app = typer.Typer(
    name="binance-ohlcv-collector",
    help="Download and collect historical OHLCV candlestick data from Binance Vision.",
    add_completion=False,
)
console = Console()


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        console.print(f"binance-ohlcv-collector {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-V",
            help="Show version and exit.",
            callback=version_callback,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """Download historical cryptocurrency data from Binance Vision."""
    pass


@app.command("download")
def download_cmd(
    symbols: Annotated[
        list[str] | None,
        typer.Argument(help="Symbol(s) to download (e.g., ETHUSDT BTCUSDT)"),
    ] = None,
    all_symbols: Annotated[
        bool,
        typer.Option("--all", "-a", help="Download all available symbols"),
    ] = False,
    timeframe: Annotated[
        str,
        typer.Option(
            "--timeframe",
            "-t",
            help=f"Kline interval. Choices: {', '.join(VALID_TIMEFRAMES)}",
        ),
    ] = DEFAULT_TIMEFRAME,
    months: Annotated[
        int | None,
        typer.Option("--months", "-m", help="Number of months to download"),
    ] = None,
    days: Annotated[
        int | None,
        typer.Option("--days", "-d", help="Number of days to download"),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option("--start", "-s", help="Start date (YYYY-MM-DD)"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", "-e", help="End date (YYYY-MM-DD)"),
    ] = None,
    market_type: Annotated[
        str,
        typer.Option(
            "--market-type",
            "-M",
            help="Market type: spot, futures-usdt, futures-coin",
        ),
    ] = "futures-usdt",
    output: Annotated[
        Path,
        typer.Option(
            "--output",
            "-o",
            help="Output directory",
        ),
    ] = Path("./data"),
    output_format: Annotated[
        str,
        typer.Option(
            "--format",
            "-f",
            help=f"Output format. Choices: {', '.join(VALID_OUTPUT_FORMATS)}",
        ),
    ] = DEFAULT_OUTPUT_FORMAT,
    no_keep_raw: Annotated[
        bool,
        typer.Option("--no-keep-raw", help="Delete raw ZIP files after processing"),
    ] = False,
    no_verify: Annotated[
        bool,
        typer.Option("--no-verify", help="Skip checksum verification"),
    ] = False,
    no_validate: Annotated[
        bool,
        typer.Option("--no-validate", help="Skip data validation (gap detection)"),
    ] = False,
    force: Annotated[
        bool,
        typer.Option("--force", help="Force re-download existing files"),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Skip confirmation prompt for large downloads"),
    ] = False,
    concurrency: Annotated[
        int,
        typer.Option("--concurrency", "-c", help="Max concurrent downloads"),
    ] = DEFAULT_CONCURRENCY,
    retries: Annotated[
        int,
        typer.Option("--retries", "-r", help="Number of retries per file"),
    ] = DEFAULT_RETRIES,
    timeout: Annotated[
        int,
        typer.Option("--timeout", help="Timeout per file in seconds"),
    ] = DEFAULT_TIMEOUT,
    quiet: Annotated[
        bool,
        typer.Option("--quiet", "-q", help="Minimal output"),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Verbose output"),
    ] = False,
) -> None:
    """Download kline data for one or more symbols."""
    # Validate inputs
    if timeframe not in VALID_TIMEFRAMES:
        console.print(f"[red]Error: Invalid timeframe '{timeframe}'[/red]")
        console.print(f"Valid options: {', '.join(VALID_TIMEFRAMES)}")
        raise typer.Exit(1)

    if output_format not in VALID_OUTPUT_FORMATS:
        console.print(f"[red]Error: Invalid format '{output_format}'[/red]")
        console.print(f"Valid options: {', '.join(VALID_OUTPUT_FORMATS)}")
        raise typer.Exit(1)

    if not months and not days and not (start and end):
        console.print("[red]Error: Must specify --months, --days, or --start/--end[/red]")
        raise typer.Exit(1)

    try:
        mt = MarketType(market_type)
    except ValueError:
        console.print(f"[red]Error: Invalid market type '{market_type}'[/red]")
        console.print("Valid options: spot, futures-usdt, futures-coin")
        raise typer.Exit(1) from None

    # Handle --all flag
    if all_symbols:
        symbols_data = list_symbols(mt)
        actual_symbols = [s["symbol"] for s in symbols_data]
        if not quiet:
            console.print(f"\n[bold]Found {len(actual_symbols)} symbols for {market_type}[/bold]")
        # Confirm for large downloads
        if not yes and not typer.confirm(f"Download all {len(actual_symbols)} symbols?"):
            raise typer.Exit(0)
    elif symbols:
        actual_symbols = symbols
    else:
        console.print("[red]Error: Must specify symbols or use --all[/red]")
        raise typer.Exit(1)

    # Confirm for large downloads (>100 symbols)
    if (
        len(actual_symbols) > 100
        and not all_symbols
        and not yes
        and not typer.confirm(f"Download {len(actual_symbols)} symbols?")
    ):
        raise typer.Exit(0)

    # Download
    if not quiet:
        console.print(f"\n[bold]Downloading {len(actual_symbols)} symbol(s)...[/bold]\n")

    try:
        if len(actual_symbols) == 1:
            result = download(
                actual_symbols[0],
                timeframe=timeframe,
                months=months,
                days=days,
                start=start,
                end=end,
                market_type=mt,
                output_dir=output,
                format=output_format,  # type: ignore[arg-type]
                keep_raw=not no_keep_raw,
                verify=not no_verify,
                validate=not no_validate,
                force=force,
                concurrency=concurrency,
                retries=retries,
                timeout=timeout,
            )

            if not quiet:
                console.print(f"[green]Saved to {result}[/green]")

        else:
            results = download(
                actual_symbols,
                timeframe=timeframe,
                months=months,
                days=days,
                start=start,
                end=end,
                market_type=mt,
                output_dir=output,
                format=output_format,  # type: ignore[arg-type]
                keep_raw=not no_keep_raw,
                verify=not no_verify,
                validate=not no_validate,
                force=force,
                concurrency=concurrency,
                retries=retries,
                timeout=timeout,
            )

            if not quiet and isinstance(results, dict):
                console.print(f"\n[green]Downloaded {len(results)} symbol(s)[/green]")
                for sym, path in results.items():
                    console.print(f"  {sym}: {path}")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        if verbose:
            console.print_exception()
        raise typer.Exit(1) from None


@app.command("download-all")
def download_all_cmd(
    market_type: Annotated[
        str,
        typer.Option(
            "--market-type",
            "-M",
            help="Market type: spot, futures-usdt, futures-coin",
        ),
    ] = "futures-usdt",
    timeframe: Annotated[
        str,
        typer.Option(
            "--timeframe",
            "-t",
            help=f"Kline interval. Choices: {', '.join(VALID_TIMEFRAMES)}",
        ),
    ] = DEFAULT_TIMEFRAME,
    months: Annotated[
        int | None,
        typer.Option("--months", "-m", help="Number of months to download"),
    ] = None,
    days: Annotated[
        int | None,
        typer.Option("--days", "-d", help="Number of days to download"),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option("--start", "-s", help="Start date (YYYY-MM-DD)"),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", "-e", help="End date (YYYY-MM-DD)"),
    ] = None,
    output: Annotated[
        Path,
        typer.Option(
            "--output",
            "-o",
            help="Output directory",
        ),
    ] = Path("./data"),
    output_format: Annotated[
        str,
        typer.Option(
            "--format",
            "-f",
            help=f"Output format. Choices: {', '.join(VALID_OUTPUT_FORMATS)}",
        ),
    ] = DEFAULT_OUTPUT_FORMAT,
    no_keep_raw: Annotated[
        bool,
        typer.Option("--no-keep-raw", help="Delete raw ZIP files after processing"),
    ] = False,
    no_verify: Annotated[
        bool,
        typer.Option("--no-verify", help="Skip checksum verification"),
    ] = False,
    no_validate: Annotated[
        bool,
        typer.Option("--no-validate", help="Skip data validation (gap detection)"),
    ] = False,
    force: Annotated[
        bool,
        typer.Option("--force", help="Force re-download existing files"),
    ] = False,
    concurrency: Annotated[
        int,
        typer.Option("--concurrency", "-c", help="Max concurrent downloads"),
    ] = DEFAULT_CONCURRENCY,
    retries: Annotated[
        int,
        typer.Option("--retries", "-r", help="Number of retries per file"),
    ] = DEFAULT_RETRIES,
    timeout: Annotated[
        int,
        typer.Option("--timeout", help="Timeout per file in seconds"),
    ] = DEFAULT_TIMEOUT,
    yes: Annotated[
        bool,
        typer.Option("--yes", "-y", help="Skip confirmation prompt"),
    ] = False,
    quiet: Annotated[
        bool,
        typer.Option("--quiet", "-q", help="Minimal output"),
    ] = False,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Verbose output"),
    ] = False,
) -> None:
    """Download kline data for all available symbols."""
    try:
        mt = MarketType(market_type)
    except ValueError:
        console.print(f"[red]Error: Invalid market type '{market_type}'[/red]")
        raise typer.Exit(1) from None

    if not months and not days and not (start and end):
        console.print("[red]Error: Must specify --months, --days, or --start/--end[/red]")
        raise typer.Exit(1)

    # Get available symbols
    symbols_data = list_symbols(mt)
    symbols = [s["symbol"] for s in symbols_data]

    if not quiet:
        console.print(f"\n[bold]Found {len(symbols)} symbols for {market_type}[/bold]")

    # Confirm
    if not yes and not typer.confirm(f"Download all {len(symbols)} symbols?"):
        raise typer.Exit(0)

    # Download
    try:
        results = download_all(
            market_type=mt,
            timeframe=timeframe,
            months=months,
            days=days,
            start=start,
            end=end,
            output_dir=output,
            format=output_format,  # type: ignore[arg-type]
            keep_raw=not no_keep_raw,
            verify=not no_verify,
            validate=not no_validate,
            force=force,
            concurrency=concurrency,
            retries=retries,
            timeout=timeout,
        )

        if not quiet:
            console.print(f"\n[green]Downloaded {len(results)} symbol(s)[/green]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        if verbose:
            console.print_exception()
        raise typer.Exit(1) from None


@app.command("list")
def list_cmd(
    market_type: Annotated[
        str,
        typer.Option(
            "--market-type",
            "-M",
            help="Market type: spot, futures-usdt, futures-coin",
        ),
    ] = "futures-usdt",
    search: Annotated[
        str | None,
        typer.Option("--search", "-s", help="Search symbols containing text"),
    ] = None,
    base: Annotated[
        str | None,
        typer.Option("--base", "-b", help="Filter by base asset (e.g., BTC, ETH)"),
    ] = None,
    quote: Annotated[
        str | None,
        typer.Option("--quote", "-q", help="Filter by quote asset (e.g., USDT, BUSD)"),
    ] = None,
    limit: Annotated[
        int | None,
        typer.Option("--limit", "-l", help="Limit number of results"),
    ] = None,
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: grid, table, json, plain"),
    ] = "grid",
    no_cache: Annotated[
        bool,
        typer.Option("--no-cache", help="Refresh symbols from API"),
    ] = False,
) -> None:
    """List available symbols."""
    try:
        mt = MarketType(market_type)
    except ValueError:
        console.print(f"[red]Error: Invalid market type '{market_type}'[/red]")
        raise typer.Exit(1) from None

    # Validate format
    valid_formats = ["grid", "table", "json", "plain"]
    if format not in valid_formats:
        console.print(f"[red]Error: Invalid format '{format}'[/red]")
        console.print(f"Valid options: {', '.join(valid_formats)}")
        raise typer.Exit(1)

    # Fetch symbols
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        progress.add_task("Fetching symbols...", total=None)
        symbols = list_symbols(mt, use_cache=not no_cache)

    total_count = len(symbols)

    # Apply filters
    filtered = filter_symbols(symbols, search=search, base_asset=base, quote_asset=quote)

    # Apply limit
    if limit and limit > 0:
        filtered = filtered[:limit]

    # Render based on format
    if format == "grid":
        _render_grid(filtered, total_count)
    elif format == "table":
        _render_table(filtered, total_count, market_type)
    elif format == "json":
        _render_json(filtered)
    elif format == "plain":
        _render_plain(filtered)


def _render_grid(symbols: list[dict[str, str]], total_count: int) -> None:
    """Render symbols in a compact grid format."""
    if not symbols:
        console.print("[yellow]No symbols found[/yellow]")
        return

    symbol_names = [s["symbol"] for s in symbols]
    columns = Columns(symbol_names, equal=True, expand=False, padding=(0, 2))

    console.print()
    console.print(columns)
    console.print()

    if len(symbols) < total_count:
        console.print(
            f"[bold]Total: {len(symbols):,} symbols (filtered from {total_count:,})[/bold]"
        )
    else:
        console.print(f"[bold]Total: {len(symbols):,} symbols[/bold]")


def _render_table(symbols: list[dict[str, str]], total_count: int, market_type: str) -> None:
    """Render symbols in a table format with metadata."""
    table = Table(title=f"Available Symbols ({market_type})")
    table.add_column("Symbol", style="cyan")
    table.add_column("Base Asset", style="green")
    table.add_column("Quote Asset", style="yellow")

    for symbol in symbols:
        table.add_row(symbol["symbol"], symbol["baseAsset"], symbol["quoteAsset"])

    console.print(table)

    if len(symbols) < total_count:
        console.print(
            f"\n[bold]Total: {len(symbols):,} symbols (filtered from {total_count:,})[/bold]"
        )
    else:
        console.print(f"\n[bold]Total: {len(symbols):,} symbols[/bold]")


def _render_json(symbols: list[dict[str, str]]) -> None:
    """Render symbols in JSON format."""
    import json

    console.print(json.dumps(symbols, indent=2))


def _render_plain(symbols: list[dict[str, str]]) -> None:
    """Render symbols in plain text format (one per line)."""
    for symbol in symbols:
        console.print(symbol["symbol"])


if __name__ == "__main__":
    app()
