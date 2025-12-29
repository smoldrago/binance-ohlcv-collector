"""Tests for validation module."""

from __future__ import annotations

from datetime import timedelta

import pandas as pd
import pytest

from binance_ohlcv_collector.validation import (
    Gap,
    ValidationResult,
    calculate_expected_bars,
    detect_gaps,
    format_validation_report,
    get_expected_interval,
    validate_dataframe,
)


class TestGetExpectedInterval:
    """Tests for get_expected_interval."""

    def test_minute_intervals(self) -> None:
        """Test minute-based timeframes."""
        assert get_expected_interval("1m") == timedelta(minutes=1)
        assert get_expected_interval("5m") == timedelta(minutes=5)
        assert get_expected_interval("15m") == timedelta(minutes=15)
        assert get_expected_interval("30m") == timedelta(minutes=30)

    def test_hour_intervals(self) -> None:
        """Test hour-based timeframes."""
        assert get_expected_interval("1h") == timedelta(hours=1)
        assert get_expected_interval("4h") == timedelta(hours=4)
        assert get_expected_interval("12h") == timedelta(hours=12)

    def test_day_intervals(self) -> None:
        """Test day-based timeframes."""
        assert get_expected_interval("1d") == timedelta(days=1)
        assert get_expected_interval("3d") == timedelta(days=3)
        assert get_expected_interval("1w") == timedelta(weeks=1)

    def test_invalid_timeframe(self) -> None:
        """Test invalid timeframe raises error."""
        with pytest.raises(ValueError, match="Invalid timeframe"):
            get_expected_interval("invalid")


class TestDetectGaps:
    """Tests for detect_gaps."""

    def test_no_gaps(self) -> None:
        """Test detection with no gaps."""
        timestamps = pd.date_range("2024-01-01", periods=10, freq="15min")
        df = pd.DataFrame({"timestamp": timestamps})

        gaps = detect_gaps(df, "15m")

        assert gaps == []

    def test_single_gap(self) -> None:
        """Test detection of a single gap."""
        # Create timestamps with a gap
        timestamps = list(pd.date_range("2024-01-01", periods=5, freq="15min"))
        # Add a gap (skip 2 periods)
        timestamps.extend(pd.date_range("2024-01-01 02:00:00", periods=5, freq="15min"))

        df = pd.DataFrame({"timestamp": timestamps})
        gaps = detect_gaps(df, "15m")

        assert len(gaps) == 1
        assert gaps[0].missing_bars > 0

    def test_multiple_gaps(self) -> None:
        """Test detection of multiple gaps."""
        timestamps = (
            list(pd.date_range("2024-01-01 00:00", periods=3, freq="15min"))
            + list(pd.date_range("2024-01-01 02:00", periods=3, freq="15min"))
            + list(pd.date_range("2024-01-01 05:00", periods=3, freq="15min"))
        )

        df = pd.DataFrame({"timestamp": timestamps})
        gaps = detect_gaps(df, "15m")

        assert len(gaps) == 2

    def test_empty_dataframe(self) -> None:
        """Test with empty DataFrame."""
        df = pd.DataFrame(columns=["timestamp"])

        gaps = detect_gaps(df, "15m")

        assert gaps == []

    def test_single_row(self) -> None:
        """Test with single row."""
        df = pd.DataFrame({"timestamp": [pd.Timestamp("2024-01-01")]})

        gaps = detect_gaps(df, "15m")

        assert gaps == []

    def test_different_timeframes(self) -> None:
        """Test gap detection with different timeframes."""
        # Create hourly data with a gap
        timestamps = list(pd.date_range("2024-01-01", periods=5, freq="1h"))
        # Add a 3-hour gap
        timestamps.extend(pd.date_range("2024-01-01 08:00:00", periods=5, freq="1h"))

        df = pd.DataFrame({"timestamp": timestamps})
        gaps = detect_gaps(df, "1h")

        assert len(gaps) == 1


class TestCalculateExpectedBars:
    """Tests for calculate_expected_bars."""

    def test_calculate_15m_bars(self) -> None:
        """Test calculating expected bars for 15m timeframe."""
        start = pd.Timestamp("2024-01-01 00:00:00")
        end = pd.Timestamp("2024-01-01 01:00:00")

        expected = calculate_expected_bars(start, end, "15m")

        # 00:00, 00:15, 00:30, 00:45, 01:00 = 5 bars
        assert expected == 5

    def test_calculate_1h_bars(self) -> None:
        """Test calculating expected bars for 1h timeframe."""
        start = pd.Timestamp("2024-01-01 00:00:00")
        end = pd.Timestamp("2024-01-01 03:00:00")

        expected = calculate_expected_bars(start, end, "1h")

        # 00:00, 01:00, 02:00, 03:00 = 4 bars
        assert expected == 4

    def test_calculate_1d_bars(self) -> None:
        """Test calculating expected bars for 1d timeframe."""
        start = pd.Timestamp("2024-01-01")
        end = pd.Timestamp("2024-01-10")

        expected = calculate_expected_bars(start, end, "1d")

        # 10 days inclusive
        assert expected == 10


class TestValidateDataframe:
    """Tests for validate_dataframe."""

    def test_valid_data(self) -> None:
        """Test validation of complete data."""
        timestamps = pd.date_range("2024-01-01", periods=96, freq="15min")
        df = pd.DataFrame({"timestamp": timestamps})

        result = validate_dataframe(df, "15m")

        assert result.is_valid
        assert result.total_bars == 96
        assert len(result.gaps) == 0

    def test_data_with_gaps(self) -> None:
        """Test validation of data with gaps."""
        timestamps = list(pd.date_range("2024-01-01 00:00", periods=10, freq="15min"))
        timestamps.extend(pd.date_range("2024-01-01 05:00", periods=10, freq="15min"))

        df = pd.DataFrame({"timestamp": timestamps})
        result = validate_dataframe(df, "15m")

        assert not result.is_valid
        assert len(result.gaps) > 0
        assert "gap" in result.warnings[0].lower()

    def test_empty_dataframe(self) -> None:
        """Test validation of empty DataFrame."""
        df = pd.DataFrame(columns=["timestamp"])

        result = validate_dataframe(df, "15m")

        assert result.is_valid
        assert result.total_bars == 0
        assert "empty" in result.warnings[0].lower()

    def test_missing_timestamp_column(self) -> None:
        """Test validation without timestamp column raises ValidationError."""
        import pytest

        from binance_ohlcv_collector.exceptions import ValidationError

        df = pd.DataFrame({"other": [1, 2, 3]})

        with pytest.raises(ValidationError) as excinfo:
            validate_dataframe(df, "15m")

        assert "timestamp" in str(excinfo.value).lower()

    def test_skip_gap_check(self) -> None:
        """Test skipping gap check."""
        timestamps = list(pd.date_range("2024-01-01 00:00", periods=10, freq="15min"))
        timestamps.extend(pd.date_range("2024-01-01 05:00", periods=10, freq="15min"))

        df = pd.DataFrame({"timestamp": timestamps})
        result = validate_dataframe(df, "15m", check_gaps=False)

        assert len(result.gaps) == 0


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_gap_count(self) -> None:
        """Test gap_count property."""
        result = ValidationResult(
            is_valid=False,
            total_bars=100,
            expected_bars=110,
            gaps=[
                Gap(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02"), 10),
                Gap(pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-04"), 5),
            ],
            warnings=[],
        )

        assert result.gap_count == 2

    def test_missing_bars(self) -> None:
        """Test missing_bars property."""
        result = ValidationResult(
            is_valid=False,
            total_bars=100,
            expected_bars=115,
            gaps=[
                Gap(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02"), 10),
                Gap(pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-04"), 5),
            ],
            warnings=[],
        )

        assert result.missing_bars == 15


class TestFormatValidationReport:
    """Tests for format_validation_report."""

    def test_format_valid_result(self) -> None:
        """Test formatting valid result."""
        result = ValidationResult(
            is_valid=True,
            total_bars=1000,
            expected_bars=1000,
            gaps=[],
            warnings=[],
        )

        report = format_validation_report(result)

        assert "VALID" in report
        assert "1,000" in report

    def test_format_invalid_result(self) -> None:
        """Test formatting invalid result."""
        result = ValidationResult(
            is_valid=False,
            total_bars=90,
            expected_bars=100,
            gaps=[
                Gap(pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-02"), 10),
            ],
            warnings=["Missing 10 bars"],
        )

        report = format_validation_report(result)

        assert "ISSUES FOUND" in report
        assert "Gap" in report
        assert "Warning" in report

    def test_format_many_gaps(self) -> None:
        """Test formatting result with many gaps (shows first 5)."""
        gaps = [
            Gap(pd.Timestamp(f"2024-01-{i:02d}"), pd.Timestamp(f"2024-01-{i + 1:02d}"), 1)
            for i in range(1, 11)
        ]

        result = ValidationResult(
            is_valid=False,
            total_bars=90,
            expected_bars=100,
            gaps=gaps,
            warnings=[],
        )

        report = format_validation_report(result)

        assert "Gap 5:" in report
        assert "5 more gaps" in report
