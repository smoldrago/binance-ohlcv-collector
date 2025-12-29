"""Process ZIP files to DataFrame, Parquet, or CSV."""

from __future__ import annotations

import logging
import zipfile
from io import BytesIO
from pathlib import Path

import pandas as pd

from binance_ohlcv_collector.config import (
    KLINE_COLUMNS,
    OUTPUT_COLUMNS,
    MarketType,
    OutputFormat,
)

logger = logging.getLogger(__name__)


def process_zip_file(zip_path: Path) -> pd.DataFrame:
    """Extract and parse CSV from a ZIP file.

    Parameters
    ----------
    zip_path : Path
        Path to the ZIP file.

    Returns
    -------
    pd.DataFrame
        DataFrame with kline data.

    """
    with zipfile.ZipFile(zip_path, "r") as zf:
        # Each ZIP contains one CSV with the same name
        csv_name = zip_path.stem + ".csv"
        with zf.open(csv_name) as f:
            content = BytesIO(f.read())

            # Read first line to check for header
            first_line = content.readline().decode("utf-8")
            content.seek(0)

            # If first field is not numeric, skip header
            has_header = not first_line.split(",")[0].isdigit()

            df = pd.read_csv(
                content,
                header=0 if has_header else None,
                names=None if has_header else KLINE_COLUMNS,
            )

            # Normalize column names if header was present
            if has_header:
                df.columns = pd.Index(KLINE_COLUMNS[: len(df.columns)])

    return df


def process_zip_files(zip_paths: list[Path]) -> pd.DataFrame:
    """Process multiple ZIP files into a single DataFrame.

    Parameters
    ----------
    zip_paths : list[Path]
        List of paths to ZIP files.

    Returns
    -------
    pd.DataFrame
        Combined DataFrame with all data, sorted and deduplicated.

    """
    dfs: list[pd.DataFrame] = []

    for zip_path in zip_paths:
        try:
            df = process_zip_file(zip_path)
            dfs.append(df)
        except Exception as e:
            # Log but continue processing
            logger.warning("Failed to process %s: %s", zip_path.name, e)

    if not dfs:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    # Combine all data
    combined = pd.concat(dfs, ignore_index=True)

    # Convert timestamps to datetime
    combined["timestamp"] = pd.to_datetime(combined["open_time"], unit="ms", utc=True)

    # Sort by timestamp and remove duplicates
    combined = combined.sort_values("timestamp").drop_duplicates(subset=["timestamp"])

    # Select output columns
    result = combined[OUTPUT_COLUMNS].copy()

    return result


def find_zip_files(
    output_dir: Path,
    symbol: str,
    timeframe: str,
    market_type: MarketType,
) -> list[Path]:
    """Find all ZIP files for a symbol.

    Parameters
    ----------
    output_dir : Path
        Base output directory.
    symbol : str
        Trading pair symbol.
    timeframe : str
        Kline interval.
    market_type : MarketType
        The market type.

    Returns
    -------
    list[Path]
        List of ZIP file paths, sorted by name.

    """
    raw_dir = output_dir / "raw" / market_type.value / f"{symbol}_{timeframe}"
    zip_pattern = f"{symbol}-{timeframe}-*.zip"

    zip_files: list[Path] = []

    # Check monthly subdirectory
    monthly_dir = raw_dir / "monthly"
    if monthly_dir.exists():
        zip_files.extend(monthly_dir.glob(zip_pattern))

    # Check daily subdirectory
    daily_dir = raw_dir / "daily"
    if daily_dir.exists():
        zip_files.extend(daily_dir.glob(zip_pattern))

    return sorted(zip_files)


def get_output_path(
    output_dir: Path,
    symbol: str,
    timeframe: str,
    market_type: MarketType,
    start_date: str,
    end_date: str,
    output_format: OutputFormat,
) -> Path:
    """Get the output file path for processed data.

    Parameters
    ----------
    output_dir : Path
        Base output directory.
    symbol : str
        Trading pair symbol.
    timeframe : str
        Kline interval.
    market_type : MarketType
        The market type.
    start_date : str
        Start date string.
    end_date : str
        End date string.
    output_format : OutputFormat
        Output format (parquet or csv).

    Returns
    -------
    Path
        Path to the output file.

    """
    processed_dir = output_dir / "processed" / market_type.value
    processed_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{symbol}-{timeframe}-{start_date}-to-{end_date}.{output_format}"
    return processed_dir / filename


def save_dataframe(
    df: pd.DataFrame,
    output_path: Path,
    output_format: OutputFormat,
) -> None:
    """Save DataFrame to file.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to save.
    output_path : Path
        Path to save to.
    output_format : OutputFormat
        Output format (parquet or csv).

    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_format == "parquet":
        df.to_parquet(output_path, index=False)
    else:  # csv
        df.to_csv(output_path, index=False)


def process_symbol(
    output_dir: Path,
    symbol: str,
    timeframe: str,
    market_type: MarketType,
    output_format: OutputFormat = "parquet",
) -> tuple[pd.DataFrame, Path | None]:
    """Process all downloaded data for a symbol.

    Parameters
    ----------
    output_dir : Path
        Base output directory.
    symbol : str
        Trading pair symbol.
    timeframe : str
        Kline interval.
    market_type : MarketType
        The market type.
    output_format : OutputFormat
        Output format (parquet or csv).

    Returns
    -------
    tuple[pd.DataFrame, Path | None]
        Tuple of (DataFrame, output_path). Output path is None if no data.

    """
    # Find all ZIP files
    zip_files = find_zip_files(output_dir, symbol, timeframe, market_type)

    if not zip_files:
        return pd.DataFrame(columns=OUTPUT_COLUMNS), None

    # Process all files
    df = process_zip_files(zip_files)

    if df.empty:
        return df, None

    # Determine date range from data
    start_date = df["timestamp"].min().strftime("%Y-%m-%d")
    end_date = df["timestamp"].max().strftime("%Y-%m-%d")

    # Get output path
    output_path = get_output_path(
        output_dir, symbol, timeframe, market_type, start_date, end_date, output_format
    )

    # Save to file
    save_dataframe(df, output_path, output_format)

    return df, output_path


def get_dataframe_stats(df: pd.DataFrame) -> dict[str, str | int]:
    """Get statistics about a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to analyze.

    Returns
    -------
    dict[str, str | int]
        Dictionary with stats: rows, start_date, end_date.

    """
    if df.empty:
        return {
            "rows": 0,
            "start_date": "",
            "end_date": "",
        }

    return {
        "rows": len(df),
        "start_date": str(df["timestamp"].min()),
        "end_date": str(df["timestamp"].max()),
    }
