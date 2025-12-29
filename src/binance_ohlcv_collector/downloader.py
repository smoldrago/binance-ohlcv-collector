"""Download logic with retries, concurrency, and checksum verification."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import httpx

from binance_ohlcv_collector.config import (
    BINANCE_VISION_BASE_URL,
    DEFAULT_CONCURRENCY,
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT,
    MARKET_TYPE_PATHS,
    Granularity,
    MarketType,
)
from binance_ohlcv_collector.exceptions import ChecksumError, DownloadError, RateLimitError

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass
class DownloadResult:
    """Result of a file download."""

    path: Path
    success: bool
    error: str | None = None
    already_existed: bool = False


@dataclass
class DownloadTask:
    """A single file download task."""

    url: str
    checksum_url: str
    output_path: Path
    date_str: str


def _generate_month_strings(end_date: date, num_months: int) -> list[str]:
    """Generate list of YYYY-MM strings for N months backwards.

    Parameters
    ----------
    end_date : date
        The ending date (most recent month to include).
    num_months : int
        Number of months to generate.

    Returns
    -------
    list[str]
        List of month strings in YYYY-MM format, newest first.

    """
    months = []
    current = end_date.replace(day=1)

    for _ in range(num_months):
        months.append(current.strftime("%Y-%m"))
        # Move to previous month
        current = (current - timedelta(days=1)).replace(day=1)

    return months


def _generate_day_strings(end_date: date, num_days: int) -> list[str]:
    """Generate list of YYYY-MM-DD strings for N days backwards.

    Parameters
    ----------
    end_date : date
        The ending date (most recent day to include).
    num_days : int
        Number of days to generate.

    Returns
    -------
    list[str]
        List of date strings in YYYY-MM-DD format, newest first.

    """
    days = []
    current = end_date

    for _ in range(num_days):
        days.append(current.strftime("%Y-%m-%d"))
        current = current - timedelta(days=1)

    return days


def _generate_date_range(
    start_date: date,
    end_date: date,
    granularity: Granularity,
) -> list[str]:
    """Generate date strings for a range.

    Parameters
    ----------
    start_date : date
        Start date (earliest).
    end_date : date
        End date (most recent).
    granularity : Granularity
        Whether to generate daily or monthly dates.

    Returns
    -------
    list[str]
        List of date strings, newest first.

    """
    dates = []

    if granularity == "monthly":
        current = end_date.replace(day=1)
        start = start_date.replace(day=1)
        while current >= start:
            dates.append(current.strftime("%Y-%m"))
            current = (current - timedelta(days=1)).replace(day=1)
    else:  # daily
        current = end_date
        while current >= start_date:
            dates.append(current.strftime("%Y-%m-%d"))
            current = current - timedelta(days=1)

    return dates


def _construct_url(
    symbol: str,
    timeframe: str,
    date_str: str,
    market_type: MarketType,
    granularity: Granularity,
) -> str:
    """Construct Binance Vision URL for a specific file.

    Parameters
    ----------
    symbol : str
        Trading pair symbol (e.g., "ETHUSDT").
    timeframe : str
        Kline interval (e.g., "15m").
    date_str : str
        Date string (YYYY-MM or YYYY-MM-DD).
    market_type : MarketType
        The market type.
    granularity : Granularity
        Daily or monthly granularity.

    Returns
    -------
    str
        Full URL to the ZIP file.

    """
    market_path = MARKET_TYPE_PATHS[market_type]
    filename = f"{symbol}-{timeframe}-{date_str}.zip"

    return (
        f"{BINANCE_VISION_BASE_URL}/{market_path}/{granularity}/klines/"
        f"{symbol}/{timeframe}/{filename}"
    )


def _construct_checksum_url(file_url: str) -> str:
    """Construct checksum URL from file URL.

    Parameters
    ----------
    file_url : str
        URL to the ZIP file.

    Returns
    -------
    str
        URL to the checksum file.

    """
    return f"{file_url}.CHECKSUM"


def _verify_checksum(file_path: Path, expected_checksum: str) -> bool:
    """Verify file checksum.

    Parameters
    ----------
    file_path : Path
        Path to the file to verify.
    expected_checksum : str
        Expected SHA256 checksum.

    Returns
    -------
    bool
        True if checksum matches.

    """
    sha256_hash = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256_hash.update(chunk)

    actual = sha256_hash.hexdigest()
    return actual.lower() == expected_checksum.lower()


def _parse_checksum_file(content: str) -> str:
    """Parse checksum from Binance checksum file content.

    The format is: <checksum>  <filename>

    Parameters
    ----------
    content : str
        Content of the checksum file.

    Returns
    -------
    str
        The checksum hash.

    """
    # Format: "abc123def456  filename.zip"
    parts = content.strip().split()
    if len(parts) >= 1:
        return parts[0]
    raise ValueError("Invalid checksum file format")


async def _download_file_with_retry(
    client: httpx.AsyncClient,
    url: str,
    output_path: Path,
    retries: int = DEFAULT_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
) -> bytes:
    """Download a file with retry logic.

    Parameters
    ----------
    client : httpx.AsyncClient
        HTTP client to use.
    url : str
        URL to download from.
    output_path : Path
        Path to save the file.
    retries : int
        Number of retries.
    timeout : int
        Timeout in seconds.

    Returns
    -------
    bytes
        File content.

    Raises
    ------
    DownloadError
        If download fails after all retries.

    """
    last_error: Exception | None = None

    for attempt in range(retries + 1):
        try:
            response = await client.get(url, timeout=timeout)
            response.raise_for_status()
            content = response.content

            # Write to file
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(content)

            return content

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                # Not found - no point retrying
                raise DownloadError(f"File not found: {url}") from e
            if e.response.status_code == 429:
                # Rate limit exceeded - raise specific error
                raise RateLimitError(f"Rate limit exceeded for {url}") from e
            last_error = e

        except (httpx.TimeoutException, httpx.ConnectError) as e:
            last_error = e

        # Exponential backoff
        if attempt < retries:
            await asyncio.sleep(2**attempt)

    raise DownloadError(f"Failed to download {url} after {retries + 1} attempts: {last_error}")


async def _download_single_file(
    client: httpx.AsyncClient,
    task: DownloadTask,
    verify: bool = True,
    retries: int = DEFAULT_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
    force: bool = False,
) -> DownloadResult:
    """Download a single file with optional checksum verification.

    Parameters
    ----------
    client : httpx.AsyncClient
        HTTP client to use.
    task : DownloadTask
        The download task.
    verify : bool
        Whether to verify checksum.
    retries : int
        Number of retries.
    timeout : int
        Timeout in seconds.
    force : bool
        Force re-download even if file exists.

    Returns
    -------
    DownloadResult
        Result of the download.

    """
    # Skip if already exists (unless force is True)
    if task.output_path.exists() and not force:
        return DownloadResult(
            path=task.output_path,
            success=True,
            already_existed=True,
        )

    try:
        # Download the file
        await _download_file_with_retry(client, task.url, task.output_path, retries, timeout)

        # Verify checksum if requested
        if verify:
            try:
                checksum_response = await client.get(task.checksum_url, timeout=timeout)
                checksum_response.raise_for_status()
                expected_checksum = _parse_checksum_file(checksum_response.text)

                if not _verify_checksum(task.output_path, expected_checksum):
                    # Delete corrupted file
                    task.output_path.unlink(missing_ok=True)
                    raise ChecksumError(f"Checksum mismatch for {task.output_path.name}")
            except httpx.HTTPStatusError:
                # Checksum file not found - skip verification
                pass

        return DownloadResult(path=task.output_path, success=True)

    except (DownloadError, ChecksumError, RateLimitError) as e:
        return DownloadResult(
            path=task.output_path,
            success=False,
            error=str(e),
        )


async def download_files_async(
    tasks: list[DownloadTask],
    verify: bool = True,
    concurrency: int = DEFAULT_CONCURRENCY,
    retries: int = DEFAULT_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
    force: bool = False,
    progress_callback: Callable[[DownloadResult], None] | None = None,
) -> list[DownloadResult]:
    """Download multiple files with concurrency control.

    Parameters
    ----------
    tasks : list[DownloadTask]
        List of download tasks.
    verify : bool
        Whether to verify checksums.
    concurrency : int
        Maximum concurrent downloads.
    retries : int
        Number of retries per file.
    timeout : int
        Timeout per file in seconds.
    force : bool
        Force re-download even if files exist.
    progress_callback : Callable[[DownloadResult], None] | None
        Optional callback for progress updates.

    Returns
    -------
    list[DownloadResult]
        Results for all downloads.

    """
    semaphore = asyncio.Semaphore(concurrency)

    async def download_with_semaphore(
        client: httpx.AsyncClient, task: DownloadTask
    ) -> DownloadResult:
        async with semaphore:
            result = await _download_single_file(client, task, verify, retries, timeout, force)
            if progress_callback:
                progress_callback(result)
            return result

    # Share one client across all downloads
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[download_with_semaphore(client, task) for task in tasks])

    return list(results)


def create_download_tasks(
    symbol: str,
    timeframe: str,
    market_type: MarketType,
    output_dir: Path,
    start_date: date | None = None,
    end_date: date | None = None,
    months: int | None = None,
    days: int | None = None,
) -> tuple[list[DownloadTask], Granularity]:
    """Create download tasks for a symbol.

    Parameters
    ----------
    symbol : str
        Trading pair symbol.
    timeframe : str
        Kline interval.
    market_type : MarketType
        The market type.
    output_dir : Path
        Base output directory.
    start_date : date | None
        Start date for range download.
    end_date : date | None
        End date for range download.
    months : int | None
        Number of months to download.
    days : int | None
        Number of days to download.

    Returns
    -------
    tuple[list[DownloadTask], Granularity]
        List of download tasks and the granularity used.

    """
    today = datetime.now().date()

    # Determine granularity and date strings
    if start_date is not None and end_date is not None:
        # Absolute date range
        # Use monthly for complete months, daily for partial/recent
        if end_date >= today.replace(day=1):
            # End date is in current month - use daily for recent data
            granularity: Granularity = "daily"
            date_strings = _generate_date_range(start_date, end_date, "daily")
        else:
            # All complete months - use monthly
            granularity = "monthly"
            date_strings = _generate_date_range(start_date, end_date, "monthly")

    elif days is not None:
        # Download N days
        granularity = "daily"
        # Start from yesterday (today's data usually not available)
        ref_date = today - timedelta(days=1)
        date_strings = _generate_day_strings(ref_date, days)

    elif months is not None:
        # Download N months
        # Use last complete month as reference
        last_complete_month = today.replace(day=1) - timedelta(days=1)
        granularity = "monthly"
        date_strings = _generate_month_strings(last_complete_month, months)

    else:
        raise ValueError("Must specify either start_date/end_date, months, or days")

    # Determine output subdirectory
    raw_dir = output_dir / "raw" / market_type.value / f"{symbol}_{timeframe}" / granularity

    # Create tasks
    tasks = []
    for date_str in date_strings:
        url = _construct_url(symbol, timeframe, date_str, market_type, granularity)
        checksum_url = _construct_checksum_url(url)
        filename = f"{symbol}-{timeframe}-{date_str}.zip"
        output_path = raw_dir / filename

        tasks.append(
            DownloadTask(
                url=url,
                checksum_url=checksum_url,
                output_path=output_path,
                date_str=date_str,
            )
        )

    return tasks, granularity


def get_download_date_range(tasks: list[DownloadTask]) -> tuple[str, str]:
    """Get the date range covered by download tasks.

    Parameters
    ----------
    tasks : list[DownloadTask]
        List of download tasks.

    Returns
    -------
    tuple[str, str]
        (start_date, end_date) as strings.

    """
    if not tasks:
        return ("", "")

    dates = [t.date_str for t in tasks]
    return (min(dates), max(dates))
