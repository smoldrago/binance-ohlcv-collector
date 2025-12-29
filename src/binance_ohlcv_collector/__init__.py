"""Download and collect historical OHLCV candlestick data from Binance Vision.

Example usage:

    from binance_ohlcv_collector import download, download_all, list_symbols

    # Download single market
    df = download("ETHUSDT", timeframe="15m", months=12)

    # Download with date range
    df = download("ETHUSDT", start="2024-01-01", end="2024-12-31")

    # Download multiple markets
    result = download(["ETHUSDT", "BTCUSDT"], months=12)

    # Save to disk
    path = download("ETHUSDT", months=12, output_dir="./data")

    # List available symbols (returns list of dicts)
    symbols = list_symbols(market_type="futures-usdt")
    # Returns: [{"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT"}, ...]

    # Filter symbols
    from binance_ohlcv_collector import filter_symbols
    filtered = filter_symbols(symbols, search="BTC", quote_asset="USDT")

"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from binance_ohlcv_collector.config import (
    DEFAULT_CONCURRENCY,
    DEFAULT_OUTPUT_FORMAT,
    DEFAULT_RETRIES,
    DEFAULT_TIMEFRAME,
    DEFAULT_TIMEOUT,
    MarketType,
    OutputFormat,
    detect_market_type,
    validate_output_format,
    validate_timeframe,
)
from binance_ohlcv_collector.downloader import (
    DownloadResult,
    create_download_tasks,
    download_files_async,
    get_download_date_range,
)
from binance_ohlcv_collector.exceptions import NoDataAvailableError
from binance_ohlcv_collector.processor import (
    process_symbol,
)
from binance_ohlcv_collector.symbols import (
    fetch_symbols,
    filter_symbols,
    validate_symbol,
)
from binance_ohlcv_collector.validation import (
    format_validation_report,
    validate_dataframe,
)

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

__version__ = "0.1.0"
__all__ = [
    "download",
    "download_all",
    "list_symbols",
    "filter_symbols",
    "MarketType",
    "DownloadResult",
]


def _parse_date(date_str: str) -> date:
    """Parse date string to date object."""
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def download(
    symbols: str | list[str],
    *,
    timeframe: str = DEFAULT_TIMEFRAME,
    months: int | None = None,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
    market_type: str | MarketType | None = None,
    output_dir: str | Path | None = None,
    format: OutputFormat = DEFAULT_OUTPUT_FORMAT,
    keep_raw: bool = True,
    verify: bool = True,
    validate: bool = True,
    force: bool = False,
    concurrency: int = DEFAULT_CONCURRENCY,
    retries: int = DEFAULT_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
    on_progress: Callable[[DownloadResult], None] | None = None,
) -> pd.DataFrame | Path | dict[str, pd.DataFrame] | dict[str, Path]:
    """Download historical kline data from Binance Vision.

    Parameters
    ----------
    symbols : str | list[str]
        Symbol(s) to download (e.g., "ETHUSDT" or ["ETHUSDT", "BTCUSDT"]).
    timeframe : str
        Kline interval (default: "15m").
    months : int | None
        Number of months to download (relative).
    days : int | None
        Number of days to download (relative).
    start : str | None
        Start date in YYYY-MM-DD format (absolute).
    end : str | None
        End date in YYYY-MM-DD format (absolute).
    market_type : str | MarketType | None
        Market type. Auto-detected if None.
    output_dir : str | Path | None
        Directory to save files. If None, returns DataFrame(s).
    format : OutputFormat
        Output format: "parquet" or "csv" (default: "parquet").
    keep_raw : bool
        Keep raw ZIP files after processing (default: True).
    verify : bool
        Verify checksums (default: True).
    validate : bool
        Validate data for gaps (default: True).
    force : bool
        Force re-download even if files exist (default: False).
    concurrency : int
        Max concurrent downloads (default: 4).
    retries : int
        Number of retries per file (default: 3).
    timeout : int
        Timeout per file in seconds (default: 30).
    on_progress : Callable[[DownloadResult], None] | None
        Optional callback for progress updates. Called after each file download.

    Returns
    -------
    pd.DataFrame | Path | dict[str, pd.DataFrame] | dict[str, Path]
        - Single symbol, no output_dir: DataFrame
        - Single symbol, with output_dir: Path to saved file
        - Multiple symbols, no output_dir: Dict of symbol -> DataFrame
        - Multiple symbols, with output_dir: Dict of symbol -> Path

    """
    # Normalize inputs
    timeframe = validate_timeframe(timeframe)
    format = validate_output_format(format)

    # Handle single vs multiple symbols
    if isinstance(symbols, str):
        return _download_single(
            symbol=symbols,
            timeframe=timeframe,
            months=months,
            days=days,
            start=start,
            end=end,
            market_type=market_type,
            output_dir=output_dir,
            format=format,
            keep_raw=keep_raw,
            verify=verify,
            validate=validate,
            force=force,
            concurrency=concurrency,
            retries=retries,
            timeout=timeout,
            on_progress=on_progress,
        )
    else:
        return _download_multiple(
            symbols=symbols,
            timeframe=timeframe,
            months=months,
            days=days,
            start=start,
            end=end,
            market_type=market_type,
            output_dir=output_dir,
            format=format,
            keep_raw=keep_raw,
            verify=verify,
            validate=validate,
            force=force,
            concurrency=concurrency,
            retries=retries,
            timeout=timeout,
            on_progress=on_progress,
        )


def _download_single(
    symbol: str,
    timeframe: str,
    months: int | None,
    days: int | None,
    start: str | None,
    end: str | None,
    market_type: str | MarketType | None,
    output_dir: str | Path | None,
    format: OutputFormat,
    keep_raw: bool,
    verify: bool,
    validate: bool,
    force: bool,
    concurrency: int,
    retries: int,
    timeout: int,
    on_progress: Callable[[DownloadResult], None] | None = None,
) -> pd.DataFrame | Path:
    """Download data for a single symbol."""
    # Determine market type
    if market_type is None:
        mt = detect_market_type(symbol)
    elif isinstance(market_type, str):
        mt = MarketType(market_type)
    else:
        mt = market_type

    # Validate symbol
    symbol = validate_symbol(symbol, mt)

    # Determine output directory
    if output_dir is not None:
        out_path = Path(output_dir)
    else:
        # Use temp directory for in-memory processing
        import tempfile

        out_path = Path(tempfile.mkdtemp())

    # Parse dates if provided
    start_date = _parse_date(start) if start else None
    end_date = _parse_date(end) if end else None

    # Create download tasks
    tasks, granularity = create_download_tasks(
        symbol=symbol,
        timeframe=timeframe,
        market_type=mt,
        output_dir=out_path,
        start_date=start_date,
        end_date=end_date,
        months=months,
        days=days,
    )

    # Run downloads
    results = asyncio.run(
        download_files_async(
            tasks,
            verify=verify,
            concurrency=concurrency,
            retries=retries,
            timeout=timeout,
            force=force,
            progress_callback=on_progress,
        )
    )

    # Check for failures and warn about missing data
    failed = [r for r in results if not r.success]

    if failed and len(failed) == len(results):
        start_date_str, end_date_str = get_download_date_range(tasks)
        raise NoDataAvailableError(symbol, start_date_str, end_date_str)

    if failed:
        # Warn about partial data availability
        logger.warning(
            "%s - %d of %d files not available. Actual data range may be less than requested.",
            symbol,
            len(failed),
            len(results),
        )

    # Process the data
    df, saved_path = process_symbol(
        output_dir=out_path,
        symbol=symbol,
        timeframe=timeframe,
        market_type=mt,
        output_format=format,
    )

    # Validate if requested
    if validate and not df.empty:
        validation = validate_dataframe(df, timeframe)
        if not validation.is_valid:
            logger.warning("%s\n%s", symbol, format_validation_report(validation))

    # Clean up raw files if requested
    if not keep_raw and output_dir is not None:
        raw_dir = out_path / "raw" / mt.value / f"{symbol}_{timeframe}"
        if raw_dir.exists():
            import shutil

            shutil.rmtree(raw_dir)

    # Return based on output_dir
    if output_dir is None:
        # Clean up temp directory
        import shutil

        shutil.rmtree(out_path)
        return df
    else:
        return saved_path if saved_path else out_path


def _download_multiple(
    symbols: list[str],
    timeframe: str,
    months: int | None,
    days: int | None,
    start: str | None,
    end: str | None,
    market_type: str | MarketType | None,
    output_dir: str | Path | None,
    format: OutputFormat,
    keep_raw: bool,
    verify: bool,
    validate: bool,
    force: bool,
    concurrency: int,
    retries: int,
    timeout: int,
    on_progress: Callable[[DownloadResult], None] | None = None,
) -> dict[str, pd.DataFrame] | dict[str, Path]:
    """Download data for multiple symbols."""
    results: dict[str, pd.DataFrame | Path] = {}

    for symbol in symbols:
        try:
            result = _download_single(
                symbol=symbol,
                timeframe=timeframe,
                months=months,
                days=days,
                start=start,
                end=end,
                market_type=market_type,
                output_dir=output_dir,
                format=format,
                keep_raw=keep_raw,
                verify=verify,
                validate=validate,
                force=force,
                concurrency=concurrency,
                retries=retries,
                timeout=timeout,
                on_progress=on_progress,
            )
            results[symbol] = result
        except Exception as e:
            logger.error("Error downloading %s: %s", symbol, e)
            continue

    return results  # type: ignore[return-value]


def download_all(
    *,
    market_type: str | MarketType = MarketType.FUTURES_USDT,
    timeframe: str = DEFAULT_TIMEFRAME,
    months: int | None = None,
    days: int | None = None,
    start: str | None = None,
    end: str | None = None,
    output_dir: str | Path | None = None,
    format: OutputFormat = DEFAULT_OUTPUT_FORMAT,
    keep_raw: bool = True,
    verify: bool = True,
    validate: bool = True,
    force: bool = False,
    concurrency: int = DEFAULT_CONCURRENCY,
    retries: int = DEFAULT_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
    on_progress: Callable[[DownloadResult], None] | None = None,
) -> dict[str, pd.DataFrame] | dict[str, Path]:
    """Download data for all available symbols in a market type.

    Parameters
    ----------
    market_type : str | MarketType
        Market type (default: "futures-usdt").
    timeframe : str
        Kline interval (default: "15m").
    months : int | None
        Number of months to download.
    days : int | None
        Number of days to download.
    start : str | None
        Start date in YYYY-MM-DD format.
    end : str | None
        End date in YYYY-MM-DD format.
    output_dir : str | Path | None
        Directory to save files.
    format : OutputFormat
        Output format: "parquet" or "csv".
    keep_raw : bool
        Keep raw ZIP files.
    verify : bool
        Verify checksums.
    validate : bool
        Validate data for gaps.
    force : bool
        Force re-download even if files exist.
    concurrency : int
        Max concurrent downloads.
    retries : int
        Number of retries per file.
    timeout : int
        Timeout per file in seconds.
    on_progress : Callable[[DownloadResult], None] | None
        Optional callback for progress updates. Called after each file download.

    Returns
    -------
    dict[str, pd.DataFrame] | dict[str, Path]
        Dict mapping symbol to DataFrame or Path.

    """
    mt = MarketType(market_type) if isinstance(market_type, str) else market_type
    symbols_data = fetch_symbols(mt)
    symbols = [s["symbol"] for s in symbols_data]

    return download(  # type: ignore[return-value]
        symbols=symbols,
        timeframe=timeframe,
        months=months,
        days=days,
        start=start,
        end=end,
        market_type=mt,
        output_dir=output_dir,
        format=format,
        keep_raw=keep_raw,
        verify=verify,
        validate=validate,
        force=force,
        concurrency=concurrency,
        retries=retries,
        timeout=timeout,
        on_progress=on_progress,
    )


def list_symbols(
    market_type: str | MarketType = MarketType.FUTURES_USDT,
    use_cache: bool = True,
) -> list[dict[str, str]]:
    """List available symbols for a market type.

    Parameters
    ----------
    market_type : str | MarketType
        Market type (default: "futures-usdt").
    use_cache : bool
        Use cached symbols if available (default: True).

    Returns
    -------
    list[dict[str, str]]
        List of available symbols with metadata (symbol, baseAsset, quoteAsset).

    """
    mt = MarketType(market_type) if isinstance(market_type, str) else market_type
    return fetch_symbols(mt, use_cache=use_cache)
