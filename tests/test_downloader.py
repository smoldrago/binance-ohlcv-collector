"""Tests for downloader module."""

from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path

import pytest
import respx
from httpx import Response

from binance_ohlcv_collector.config import MarketType
from binance_ohlcv_collector.downloader import (
    DownloadResult,
    DownloadTask,
    _construct_checksum_url,
    _construct_url,
    _generate_date_range,
    _generate_day_strings,
    _generate_month_strings,
    _parse_checksum_file,
    _verify_checksum,
    create_download_tasks,
    download_files_async,
    get_download_date_range,
)


class TestGenerateMonthStrings:
    """Tests for _generate_month_strings."""

    def test_generate_12_months(self) -> None:
        """Test generating 12 months of strings."""
        end = date(2024, 12, 15)
        months = _generate_month_strings(end, 12)

        assert len(months) == 12
        assert months[0] == "2024-12"  # Most recent first
        assert months[-1] == "2024-01"  # Oldest last

    def test_generate_crosses_year(self) -> None:
        """Test generation across year boundary."""
        end = date(2024, 2, 15)
        months = _generate_month_strings(end, 4)

        assert months == ["2024-02", "2024-01", "2023-12", "2023-11"]

    def test_generate_single_month(self) -> None:
        """Test generating single month."""
        end = date(2024, 6, 1)
        months = _generate_month_strings(end, 1)

        assert months == ["2024-06"]


class TestGenerateDayStrings:
    """Tests for _generate_day_strings."""

    def test_generate_30_days(self) -> None:
        """Test generating 30 days of strings."""
        end = date(2024, 12, 31)
        days = _generate_day_strings(end, 30)

        assert len(days) == 30
        assert days[0] == "2024-12-31"  # Most recent first
        assert days[-1] == "2024-12-02"  # Oldest last

    def test_generate_crosses_month(self) -> None:
        """Test generation across month boundary."""
        end = date(2024, 3, 2)
        days = _generate_day_strings(end, 5)

        assert days == [
            "2024-03-02",
            "2024-03-01",
            "2024-02-29",  # Leap year
            "2024-02-28",
            "2024-02-27",
        ]

    def test_generate_single_day(self) -> None:
        """Test generating single day."""
        end = date(2024, 6, 15)
        days = _generate_day_strings(end, 1)

        assert days == ["2024-06-15"]


class TestGenerateDateRange:
    """Tests for _generate_date_range."""

    def test_monthly_range(self) -> None:
        """Test monthly date range generation."""
        start = date(2024, 1, 1)
        end = date(2024, 3, 31)
        dates = _generate_date_range(start, end, "monthly")

        assert dates == ["2024-03", "2024-02", "2024-01"]

    def test_daily_range(self) -> None:
        """Test daily date range generation."""
        start = date(2024, 1, 1)
        end = date(2024, 1, 3)
        dates = _generate_date_range(start, end, "daily")

        assert dates == ["2024-01-03", "2024-01-02", "2024-01-01"]


class TestConstructUrl:
    """Tests for _construct_url."""

    def test_futures_usdt_monthly(self) -> None:
        """Test URL construction for USDT-M futures monthly."""
        url = _construct_url("ETHUSDT", "15m", "2024-01", MarketType.FUTURES_USDT, "monthly")

        assert url == (
            "https://data.binance.vision/data/futures/um/monthly/klines/"
            "ETHUSDT/15m/ETHUSDT-15m-2024-01.zip"
        )

    def test_futures_usdt_daily(self) -> None:
        """Test URL construction for USDT-M futures daily."""
        url = _construct_url("ETHUSDT", "15m", "2024-01-15", MarketType.FUTURES_USDT, "daily")

        assert url == (
            "https://data.binance.vision/data/futures/um/daily/klines/"
            "ETHUSDT/15m/ETHUSDT-15m-2024-01-15.zip"
        )

    def test_futures_coin(self) -> None:
        """Test URL construction for COIN-M futures."""
        url = _construct_url("ETHUSD_PERP", "1h", "2024-01", MarketType.FUTURES_COIN, "monthly")

        assert url == (
            "https://data.binance.vision/data/futures/cm/monthly/klines/"
            "ETHUSD_PERP/1h/ETHUSD_PERP-1h-2024-01.zip"
        )

    def test_spot(self) -> None:
        """Test URL construction for spot."""
        url = _construct_url("ETHUSDT", "1d", "2024-01", MarketType.SPOT, "monthly")

        assert url == (
            "https://data.binance.vision/data/spot/monthly/klines/ETHUSDT/1d/ETHUSDT-1d-2024-01.zip"
        )


class TestConstructChecksumUrl:
    """Tests for _construct_checksum_url."""

    def test_appends_checksum_extension(self) -> None:
        """Test that .CHECKSUM is appended."""
        file_url = "https://example.com/file.zip"
        checksum_url = _construct_checksum_url(file_url)

        assert checksum_url == "https://example.com/file.zip.CHECKSUM"


class TestParseChecksumFile:
    """Tests for _parse_checksum_file."""

    def test_parse_valid_checksum(self) -> None:
        """Test parsing valid checksum file."""
        content = "abc123def456  ETHUSDT-15m-2024-01.zip"
        checksum = _parse_checksum_file(content)

        assert checksum == "abc123def456"

    def test_parse_checksum_with_newline(self) -> None:
        """Test parsing checksum file with trailing newline."""
        content = "abc123def456  ETHUSDT-15m-2024-01.zip\n"
        checksum = _parse_checksum_file(content)

        assert checksum == "abc123def456"

    def test_parse_invalid_raises(self) -> None:
        """Test parsing empty content raises."""
        with pytest.raises(ValueError):
            _parse_checksum_file("")


class TestVerifyChecksum:
    """Tests for _verify_checksum."""

    def test_valid_checksum(self, tmp_path: Path) -> None:
        """Test verification with correct checksum."""
        # Create test file
        test_file = tmp_path / "test.zip"
        test_content = b"test content"
        test_file.write_bytes(test_content)

        # Calculate expected checksum
        expected = hashlib.sha256(test_content).hexdigest()

        assert _verify_checksum(test_file, expected) is True

    def test_invalid_checksum(self, tmp_path: Path) -> None:
        """Test verification with incorrect checksum."""
        test_file = tmp_path / "test.zip"
        test_file.write_bytes(b"test content")

        assert _verify_checksum(test_file, "wrongchecksum") is False

    def test_case_insensitive(self, tmp_path: Path) -> None:
        """Test checksum comparison is case-insensitive."""
        test_file = tmp_path / "test.zip"
        test_content = b"test content"
        test_file.write_bytes(test_content)

        expected = hashlib.sha256(test_content).hexdigest().upper()

        assert _verify_checksum(test_file, expected) is True


class TestCreateDownloadTasks:
    """Tests for create_download_tasks."""

    def test_create_tasks_with_months(self, tmp_path: Path) -> None:
        """Test creating tasks for N months."""
        tasks, granularity = create_download_tasks(
            symbol="ETHUSDT",
            timeframe="15m",
            market_type=MarketType.FUTURES_USDT,
            output_dir=tmp_path,
            months=3,
        )

        assert len(tasks) == 3
        assert granularity == "monthly"
        assert all(isinstance(t, DownloadTask) for t in tasks)
        assert all("monthly" in str(t.output_path) for t in tasks)

    def test_create_tasks_with_days(self, tmp_path: Path) -> None:
        """Test creating tasks for N days."""
        tasks, granularity = create_download_tasks(
            symbol="ETHUSDT",
            timeframe="15m",
            market_type=MarketType.FUTURES_USDT,
            output_dir=tmp_path,
            days=10,
        )

        assert len(tasks) == 10
        assert granularity == "daily"
        assert all("daily" in str(t.output_path) for t in tasks)

    def test_output_path_structure(self, tmp_path: Path) -> None:
        """Test output path has correct structure."""
        tasks, _ = create_download_tasks(
            symbol="ETHUSDT",
            timeframe="15m",
            market_type=MarketType.FUTURES_USDT,
            output_dir=tmp_path,
            months=1,
        )

        task = tasks[0]
        path_parts = task.output_path.parts

        # Should contain: raw / futures-usdt / ETHUSDT_15m / monthly / filename.zip
        assert "raw" in path_parts
        assert "futures-usdt" in path_parts
        assert "ETHUSDT_15m" in path_parts
        assert "monthly" in path_parts

    def test_no_params_raises(self, tmp_path: Path) -> None:
        """Test that missing date params raises error."""
        with pytest.raises(ValueError, match="Must specify"):
            create_download_tasks(
                symbol="ETHUSDT",
                timeframe="15m",
                market_type=MarketType.FUTURES_USDT,
                output_dir=tmp_path,
            )


class TestGetDownloadDateRange:
    """Tests for get_download_date_range."""

    def test_get_range(self) -> None:
        """Test getting date range from tasks."""
        tasks = [
            DownloadTask("url1", "cs1", Path("p1"), "2024-01"),
            DownloadTask("url2", "cs2", Path("p2"), "2024-03"),
            DownloadTask("url3", "cs3", Path("p3"), "2024-02"),
        ]

        start, end = get_download_date_range(tasks)

        assert start == "2024-01"
        assert end == "2024-03"

    def test_empty_tasks(self) -> None:
        """Test with empty task list."""
        start, end = get_download_date_range([])

        assert start == ""
        assert end == ""


class TestDownloadFilesAsync:
    """Tests for download_files_async."""

    @respx.mock
    async def test_download_single_file(self, tmp_path: Path) -> None:
        """Test downloading a single file."""
        file_url = "https://data.binance.vision/test.zip"
        file_content = b"test zip content"

        respx.get(file_url).mock(return_value=Response(200, content=file_content))
        respx.get(f"{file_url}.CHECKSUM").mock(return_value=Response(404))

        task = DownloadTask(
            url=file_url,
            checksum_url=f"{file_url}.CHECKSUM",
            output_path=tmp_path / "test.zip",
            date_str="2024-01",
        )

        results = await download_files_async([task], verify=False)

        assert len(results) == 1
        assert results[0].success is True
        assert results[0].path.exists()
        assert results[0].path.read_bytes() == file_content

    @respx.mock
    async def test_download_with_checksum(self, tmp_path: Path) -> None:
        """Test downloading with checksum verification."""
        file_url = "https://data.binance.vision/test.zip"
        file_content = b"test zip content"
        checksum = hashlib.sha256(file_content).hexdigest()

        respx.get(file_url).mock(return_value=Response(200, content=file_content))
        respx.get(f"{file_url}.CHECKSUM").mock(
            return_value=Response(200, text=f"{checksum}  test.zip")
        )

        task = DownloadTask(
            url=file_url,
            checksum_url=f"{file_url}.CHECKSUM",
            output_path=tmp_path / "test.zip",
            date_str="2024-01",
        )

        results = await download_files_async([task], verify=True)

        assert results[0].success is True

    @respx.mock
    async def test_download_skips_existing(self, tmp_path: Path) -> None:
        """Test that existing files are skipped."""
        file_url = "https://data.binance.vision/test.zip"
        output_path = tmp_path / "test.zip"
        output_path.write_bytes(b"existing content")

        # No mock needed - should not make request

        task = DownloadTask(
            url=file_url,
            checksum_url=f"{file_url}.CHECKSUM",
            output_path=output_path,
            date_str="2024-01",
        )

        results = await download_files_async([task])

        assert results[0].success is True
        assert results[0].already_existed is True

    @respx.mock
    async def test_download_force_redownloads_existing(self, tmp_path: Path) -> None:
        """Test that force=True re-downloads existing files."""
        file_url = "https://data.binance.vision/test.zip"
        output_path = tmp_path / "test.zip"
        output_path.write_bytes(b"old content")

        # Mock the new download
        respx.get(file_url).mock(return_value=Response(200, content=b"new content"))
        respx.get(f"{file_url}.CHECKSUM").mock(return_value=Response(404))

        task = DownloadTask(
            url=file_url,
            checksum_url=f"{file_url}.CHECKSUM",
            output_path=output_path,
            date_str="2024-01",
        )

        results = await download_files_async([task], force=True, verify=False)

        assert results[0].success is True
        assert results[0].already_existed is False
        assert output_path.read_bytes() == b"new content"

    @respx.mock
    async def test_download_404_error(self, tmp_path: Path) -> None:
        """Test handling of 404 error."""
        file_url = "https://data.binance.vision/notfound.zip"

        respx.get(file_url).mock(return_value=Response(404))

        task = DownloadTask(
            url=file_url,
            checksum_url=f"{file_url}.CHECKSUM",
            output_path=tmp_path / "test.zip",
            date_str="2024-01",
        )

        results = await download_files_async([task])

        assert results[0].success is False
        assert results[0].error is not None
        assert "not found" in results[0].error.lower()

    @respx.mock
    async def test_download_multiple_concurrent(self, tmp_path: Path) -> None:
        """Test concurrent download of multiple files."""
        tasks = []
        for i in range(5):
            file_url = f"https://data.binance.vision/test{i}.zip"
            respx.get(file_url).mock(return_value=Response(200, content=f"content{i}".encode()))
            respx.get(f"{file_url}.CHECKSUM").mock(return_value=Response(404))

            tasks.append(
                DownloadTask(
                    url=file_url,
                    checksum_url=f"{file_url}.CHECKSUM",
                    output_path=tmp_path / f"test{i}.zip",
                    date_str=f"2024-0{i + 1}",
                )
            )

        results = await download_files_async(tasks, verify=False, concurrency=3)

        assert len(results) == 5
        assert all(r.success for r in results)

    @respx.mock
    async def test_progress_callback(self, tmp_path: Path) -> None:
        """Test progress callback is called."""
        file_url = "https://data.binance.vision/test.zip"
        respx.get(file_url).mock(return_value=Response(200, content=b"content"))
        respx.get(f"{file_url}.CHECKSUM").mock(return_value=Response(404))

        task = DownloadTask(
            url=file_url,
            checksum_url=f"{file_url}.CHECKSUM",
            output_path=tmp_path / "test.zip",
            date_str="2024-01",
        )

        callback_results = []

        def callback(result: DownloadResult) -> None:
            callback_results.append(result)

        await download_files_async([task], verify=False, progress_callback=callback)

        assert len(callback_results) == 1
        assert callback_results[0].success is True
