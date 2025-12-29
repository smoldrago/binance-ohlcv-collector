"""Data validation: gap detection, data integrity checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import pandas as pd

from binance_ohlcv_collector.config import VALID_TIMEFRAMES
from binance_ohlcv_collector.exceptions import ValidationError

# Map timeframe string to expected interval in minutes
TIMEFRAME_MINUTES: dict[str, int] = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "2h": 120,
    "4h": 240,
    "6h": 360,
    "8h": 480,
    "12h": 720,
    "1d": 1440,
    "3d": 4320,
    "1w": 10080,
    "1mo": 43200,  # Approximate for 30 days
}


@dataclass
class Gap:
    """Represents a gap in the data."""

    start: pd.Timestamp
    end: pd.Timestamp
    missing_bars: int


@dataclass
class ValidationResult:
    """Result of data validation."""

    is_valid: bool
    total_bars: int
    expected_bars: int
    gaps: list[Gap]
    warnings: list[str]

    @property
    def gap_count(self) -> int:
        """Number of gaps found."""
        return len(self.gaps)

    @property
    def missing_bars(self) -> int:
        """Total number of missing bars."""
        return sum(g.missing_bars for g in self.gaps)


def get_expected_interval(timeframe: str) -> timedelta:
    """Get the expected time interval for a timeframe.

    Parameters
    ----------
    timeframe : str
        Kline interval (e.g., "15m", "1h").

    Returns
    -------
    timedelta
        Expected interval between bars.

    Raises
    ------
    ValueError
        If timeframe is not valid.

    """
    if timeframe not in VALID_TIMEFRAMES:
        raise ValueError(f"Invalid timeframe: {timeframe}")

    minutes = TIMEFRAME_MINUTES[timeframe]
    return timedelta(minutes=minutes)


def detect_gaps(
    df: pd.DataFrame,
    timeframe: str,
    tolerance_factor: float = 1.5,
) -> list[Gap]:
    """Detect gaps in time series data.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with a 'timestamp' column.
    timeframe : str
        Expected timeframe (e.g., "15m", "1h").
    tolerance_factor : float
        Factor to multiply expected interval for gap detection.
        Default 1.5 means gaps > 1.5x expected interval are flagged.

    Returns
    -------
    list[Gap]
        List of detected gaps.

    """
    if df.empty or len(df) < 2:
        return []

    expected_interval = get_expected_interval(timeframe)
    tolerance = expected_interval * tolerance_factor

    # Ensure sorted
    sorted_df = df.sort_values("timestamp")

    # Calculate time differences
    timestamps = sorted_df["timestamp"]
    diffs = timestamps.diff()

    # Convert tolerance to pandas Timedelta for comparison
    tolerance_td = pd.Timedelta(tolerance)
    expected_td = pd.Timedelta(expected_interval)

    gaps = []
    for i in range(1, len(diffs)):
        diff_val = diffs.iloc[i]
        if pd.notna(diff_val):
            diff = pd.Timedelta(diff_val)
            if diff > tolerance_td:
                gap_start = timestamps.iloc[i - 1]
                gap_end = timestamps.iloc[i]

                # Calculate missing bars (approximate)
                missing = int(diff / expected_td) - 1

                gaps.append(
                    Gap(
                        start=gap_start,
                        end=gap_end,
                        missing_bars=missing,
                    )
                )

    return gaps


def calculate_expected_bars(
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    timeframe: str,
) -> int:
    """Calculate expected number of bars for a time range.

    Parameters
    ----------
    start_time : pd.Timestamp
        Start of time range.
    end_time : pd.Timestamp
        End of time range.
    timeframe : str
        Kline interval.

    Returns
    -------
    int
        Expected number of bars.

    """
    interval = pd.Timedelta(get_expected_interval(timeframe))
    duration = end_time - start_time

    # Add 1 because both endpoints are inclusive
    return int(duration / interval) + 1


def validate_dataframe(
    df: pd.DataFrame,
    timeframe: str,
    check_gaps: bool = True,
) -> ValidationResult:
    """Validate a DataFrame of kline data.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with 'timestamp' column.
    timeframe : str
        Expected timeframe.
    check_gaps : bool
        Whether to check for gaps.

    Returns
    -------
    ValidationResult
        Validation result with gaps and warnings.

    """
    warnings: list[str] = []
    gaps: list[Gap] = []

    if df.empty:
        return ValidationResult(
            is_valid=True,
            total_bars=0,
            expected_bars=0,
            gaps=[],
            warnings=["DataFrame is empty"],
        )

    # Check for required columns
    if "timestamp" not in df.columns:
        raise ValidationError(
            "DataFrame is missing required 'timestamp' column. "
            f"Available columns: {list(df.columns)}"
        )

    total_bars = len(df)

    # Calculate expected bars
    start_time = df["timestamp"].min()
    end_time = df["timestamp"].max()
    expected_bars = calculate_expected_bars(start_time, end_time, timeframe)

    # Check for gaps
    if check_gaps:
        gaps = detect_gaps(df, timeframe)

    if gaps:
        warnings.append(f"Found {len(gaps)} gap(s) in data")

    # Check if we have fewer bars than expected
    if total_bars < expected_bars:
        diff = expected_bars - total_bars
        warnings.append(f"Missing {diff} bars (have {total_bars}, expected {expected_bars})")

    is_valid = len(gaps) == 0 and total_bars >= expected_bars * 0.99  # 99% threshold

    return ValidationResult(
        is_valid=is_valid,
        total_bars=total_bars,
        expected_bars=expected_bars,
        gaps=gaps,
        warnings=warnings,
    )


def format_validation_report(result: ValidationResult) -> str:
    """Format validation result as a human-readable report.

    Parameters
    ----------
    result : ValidationResult
        Validation result to format.

    Returns
    -------
    str
        Formatted report string.

    """
    lines = []

    status = "VALID" if result.is_valid else "ISSUES FOUND"
    lines.append(f"Validation: {status}")
    lines.append(f"Total bars: {result.total_bars:,}")
    lines.append(f"Expected bars: {result.expected_bars:,}")

    if result.gaps:
        lines.append(f"Gaps found: {result.gap_count}")
        lines.append(f"Missing bars: {result.missing_bars:,}")

        # Show first few gaps
        for i, gap in enumerate(result.gaps[:5]):
            lines.append(f"  Gap {i + 1}: {gap.start} to {gap.end} ({gap.missing_bars} bars)")

        if len(result.gaps) > 5:
            lines.append(f"  ... and {len(result.gaps) - 5} more gaps")

    if result.warnings:
        lines.append("Warnings:")
        for warning in result.warnings:
            lines.append(f"  - {warning}")

    return "\n".join(lines)
