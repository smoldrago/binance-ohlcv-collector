"""Tests for processor module."""

from __future__ import annotations

import zipfile
from pathlib import Path

import pandas as pd

from binance_ohlcv_collector.config import KLINE_COLUMNS, OUTPUT_COLUMNS, MarketType
from binance_ohlcv_collector.processor import (
    find_zip_files,
    get_dataframe_stats,
    get_output_path,
    process_symbol,
    process_zip_file,
    process_zip_files,
    save_dataframe,
)


def create_test_zip(path: Path, data: list[list[str]], with_header: bool = False) -> None:
    """Create a test ZIP file with CSV data.

    Parameters
    ----------
    path : Path
        Path to create the ZIP file.
    data : list[list[str]]
        CSV data rows.
    with_header : bool
        Whether to include header row.

    """
    csv_name = path.stem + ".csv"
    csv_content = ""

    if with_header:
        csv_content = ",".join(KLINE_COLUMNS) + "\n"

    for row in data:
        csv_content += ",".join(row) + "\n"

    path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr(csv_name, csv_content)


# Sample kline data (open_time, open, high, low, close, volume, close_time, ...)
SAMPLE_KLINE_1 = [
    "1704067200000",  # 2024-01-01 00:00:00 UTC
    "100.0",
    "105.0",
    "99.0",
    "102.0",
    "1000.0",
    "1704067799999",
    "102000.0",
    "50",
    "500.0",
    "51000.0",
    "0",
]

SAMPLE_KLINE_2 = [
    "1704067500000",  # 2024-01-01 00:05:00 UTC
    "102.0",
    "108.0",
    "101.0",
    "106.0",
    "1500.0",
    "1704068099999",
    "159000.0",
    "75",
    "750.0",
    "79500.0",
    "0",
]


class TestProcessZipFile:
    """Tests for process_zip_file."""

    def test_process_zip_without_header(self, tmp_path: Path) -> None:
        """Test processing ZIP file without header."""
        zip_path = tmp_path / "test.zip"
        create_test_zip(zip_path, [SAMPLE_KLINE_1, SAMPLE_KLINE_2], with_header=False)

        df = process_zip_file(zip_path)

        assert len(df) == 2
        assert list(df.columns) == KLINE_COLUMNS
        assert df["open"].iloc[0] == 100.0
        assert df["close"].iloc[1] == 106.0

    def test_process_zip_with_header(self, tmp_path: Path) -> None:
        """Test processing ZIP file with header."""
        zip_path = tmp_path / "test.zip"
        create_test_zip(zip_path, [SAMPLE_KLINE_1, SAMPLE_KLINE_2], with_header=True)

        df = process_zip_file(zip_path)

        assert len(df) == 2
        assert "open" in df.columns
        assert "close" in df.columns


class TestProcessZipFiles:
    """Tests for process_zip_files."""

    def test_process_multiple_files(self, tmp_path: Path) -> None:
        """Test processing multiple ZIP files."""
        zip1 = tmp_path / "file1.zip"
        zip2 = tmp_path / "file2.zip"

        create_test_zip(zip1, [SAMPLE_KLINE_1])
        create_test_zip(zip2, [SAMPLE_KLINE_2])

        df = process_zip_files([zip1, zip2])

        assert len(df) == 2
        assert list(df.columns) == OUTPUT_COLUMNS
        assert "timestamp" in df.columns

    def test_removes_duplicates(self, tmp_path: Path) -> None:
        """Test that duplicate timestamps are removed."""
        zip1 = tmp_path / "file1.zip"
        zip2 = tmp_path / "file2.zip"

        # Same data in both files
        create_test_zip(zip1, [SAMPLE_KLINE_1])
        create_test_zip(zip2, [SAMPLE_KLINE_1])

        df = process_zip_files([zip1, zip2])

        assert len(df) == 1

    def test_sorts_by_timestamp(self, tmp_path: Path) -> None:
        """Test that result is sorted by timestamp."""
        zip1 = tmp_path / "file1.zip"
        zip2 = tmp_path / "file2.zip"

        # Add in reverse order
        create_test_zip(zip1, [SAMPLE_KLINE_2])
        create_test_zip(zip2, [SAMPLE_KLINE_1])

        df = process_zip_files([zip1, zip2])

        assert df["timestamp"].is_monotonic_increasing

    def test_empty_list(self) -> None:
        """Test processing empty list."""
        df = process_zip_files([])

        assert df.empty
        assert list(df.columns) == OUTPUT_COLUMNS


class TestFindZipFiles:
    """Tests for find_zip_files."""

    def test_finds_monthly_files(self, tmp_path: Path) -> None:
        """Test finding monthly ZIP files."""
        monthly_dir = tmp_path / "raw" / "futures-usdt" / "ETHUSDT_15m" / "monthly"
        monthly_dir.mkdir(parents=True)

        (monthly_dir / "ETHUSDT-15m-2024-01.zip").touch()
        (monthly_dir / "ETHUSDT-15m-2024-02.zip").touch()

        files = find_zip_files(tmp_path, "ETHUSDT", "15m", MarketType.FUTURES_USDT)

        assert len(files) == 2

    def test_finds_daily_files(self, tmp_path: Path) -> None:
        """Test finding daily ZIP files."""
        daily_dir = tmp_path / "raw" / "futures-usdt" / "ETHUSDT_15m" / "daily"
        daily_dir.mkdir(parents=True)

        (daily_dir / "ETHUSDT-15m-2024-01-01.zip").touch()
        (daily_dir / "ETHUSDT-15m-2024-01-02.zip").touch()

        files = find_zip_files(tmp_path, "ETHUSDT", "15m", MarketType.FUTURES_USDT)

        assert len(files) == 2

    def test_finds_both_monthly_and_daily(self, tmp_path: Path) -> None:
        """Test finding both monthly and daily files."""
        base = tmp_path / "raw" / "futures-usdt" / "ETHUSDT_15m"
        (base / "monthly").mkdir(parents=True)
        (base / "daily").mkdir(parents=True)

        (base / "monthly" / "ETHUSDT-15m-2024-01.zip").touch()
        (base / "daily" / "ETHUSDT-15m-2024-02-01.zip").touch()

        files = find_zip_files(tmp_path, "ETHUSDT", "15m", MarketType.FUTURES_USDT)

        assert len(files) == 2

    def test_returns_sorted(self, tmp_path: Path) -> None:
        """Test that files are returned sorted."""
        monthly_dir = tmp_path / "raw" / "futures-usdt" / "ETHUSDT_15m" / "monthly"
        monthly_dir.mkdir(parents=True)

        (monthly_dir / "ETHUSDT-15m-2024-03.zip").touch()
        (monthly_dir / "ETHUSDT-15m-2024-01.zip").touch()
        (monthly_dir / "ETHUSDT-15m-2024-02.zip").touch()

        files = find_zip_files(tmp_path, "ETHUSDT", "15m", MarketType.FUTURES_USDT)

        assert files[0].name == "ETHUSDT-15m-2024-01.zip"
        assert files[1].name == "ETHUSDT-15m-2024-02.zip"
        assert files[2].name == "ETHUSDT-15m-2024-03.zip"

    def test_no_files(self, tmp_path: Path) -> None:
        """Test when no files exist."""
        files = find_zip_files(tmp_path, "ETHUSDT", "15m", MarketType.FUTURES_USDT)

        assert files == []


class TestGetOutputPath:
    """Tests for get_output_path."""

    def test_parquet_path(self, tmp_path: Path) -> None:
        """Test output path for parquet format."""
        path = get_output_path(
            tmp_path,
            "ETHUSDT",
            "15m",
            MarketType.FUTURES_USDT,
            "2024-01-01",
            "2024-12-31",
            "parquet",
        )

        assert path.suffix == ".parquet"
        assert "ETHUSDT-15m-2024-01-01-to-2024-12-31" in path.name
        assert "futures-usdt" in str(path)
        assert "processed" in str(path)

    def test_csv_path(self, tmp_path: Path) -> None:
        """Test output path for CSV format."""
        path = get_output_path(
            tmp_path,
            "ETHUSDT",
            "15m",
            MarketType.FUTURES_USDT,
            "2024-01-01",
            "2024-12-31",
            "csv",
        )

        assert path.suffix == ".csv"

    def test_creates_directory(self, tmp_path: Path) -> None:
        """Test that processed directory is created."""
        path = get_output_path(
            tmp_path,
            "ETHUSDT",
            "15m",
            MarketType.FUTURES_USDT,
            "2024-01-01",
            "2024-12-31",
            "parquet",
        )

        assert path.parent.exists()


class TestSaveDataframe:
    """Tests for save_dataframe."""

    def test_save_parquet(self, tmp_path: Path) -> None:
        """Test saving DataFrame to parquet."""
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        output_path = tmp_path / "test.parquet"

        save_dataframe(df, output_path, "parquet")

        assert output_path.exists()
        loaded = pd.read_parquet(output_path)
        pd.testing.assert_frame_equal(df, loaded)

    def test_save_csv(self, tmp_path: Path) -> None:
        """Test saving DataFrame to CSV."""
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        output_path = tmp_path / "test.csv"

        save_dataframe(df, output_path, "csv")

        assert output_path.exists()
        loaded = pd.read_csv(output_path)
        pd.testing.assert_frame_equal(df, loaded)

    def test_creates_parent_directory(self, tmp_path: Path) -> None:
        """Test that parent directory is created."""
        df = pd.DataFrame({"a": [1]})
        output_path = tmp_path / "subdir" / "nested" / "test.parquet"

        save_dataframe(df, output_path, "parquet")

        assert output_path.exists()


class TestProcessSymbol:
    """Tests for process_symbol."""

    def test_process_symbol(self, tmp_path: Path) -> None:
        """Test full symbol processing pipeline."""
        # Create raw directory structure
        monthly_dir = tmp_path / "raw" / "futures-usdt" / "ETHUSDT_15m" / "monthly"
        monthly_dir.mkdir(parents=True)

        # Create test ZIP
        zip_path = monthly_dir / "ETHUSDT-15m-2024-01.zip"
        create_test_zip(zip_path, [SAMPLE_KLINE_1, SAMPLE_KLINE_2])

        df, output_path = process_symbol(
            tmp_path, "ETHUSDT", "15m", MarketType.FUTURES_USDT, "parquet"
        )

        assert len(df) == 2
        assert output_path is not None
        assert output_path.exists()
        assert output_path.suffix == ".parquet"

    def test_process_symbol_no_files(self, tmp_path: Path) -> None:
        """Test processing when no files exist."""
        df, output_path = process_symbol(
            tmp_path, "ETHUSDT", "15m", MarketType.FUTURES_USDT, "parquet"
        )

        assert df.empty
        assert output_path is None


class TestGetDataframeStats:
    """Tests for get_dataframe_stats."""

    def test_stats_with_data(self) -> None:
        """Test getting stats from DataFrame with data."""
        df = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
                "open": [1, 2, 3],
            }
        )

        stats = get_dataframe_stats(df)

        assert stats["rows"] == 3
        assert "2024-01-01" in stats["start_date"]
        assert "2024-01-03" in stats["end_date"]

    def test_stats_empty_dataframe(self) -> None:
        """Test getting stats from empty DataFrame."""
        df = pd.DataFrame(columns=OUTPUT_COLUMNS)

        stats = get_dataframe_stats(df)

        assert stats["rows"] == 0
        assert stats["start_date"] == ""
        assert stats["end_date"] == ""
