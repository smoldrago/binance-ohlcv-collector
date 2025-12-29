"""Tests for CLI module using real Binance API."""

from pathlib import Path

from typer.testing import CliRunner

from binance_ohlcv_collector.cli import app

runner = CliRunner()


class TestListCommand:
    """Tests for the list command."""

    def test_list_futures_usdt(self) -> None:
        """Test listing futures-usdt symbols."""
        result = runner.invoke(app, ["list", "--market-type", "futures-usdt"])
        assert result.exit_code == 0
        assert "BTCUSDT" in result.stdout
        assert "Total:" in result.stdout

    def test_list_spot(self) -> None:
        """Test listing spot symbols."""
        result = runner.invoke(app, ["list", "--market-type", "spot"])
        assert result.exit_code == 0
        assert "Total:" in result.stdout

    def test_list_futures_coin(self) -> None:
        """Test listing futures-coin symbols."""
        result = runner.invoke(app, ["list", "--market-type", "futures-coin"])
        assert result.exit_code == 0
        assert "BTCUSD_PERP" in result.stdout

    def test_list_invalid_market_type(self) -> None:
        """Test error for invalid market type."""
        result = runner.invoke(app, ["list", "--market-type", "invalid"])
        assert result.exit_code == 1
        assert "Invalid market type" in result.stdout


class TestDownloadCommand:
    """Tests for the download command."""

    def test_download_single_symbol(self, tmp_path: Path) -> None:
        """Test downloading single symbol."""
        result = runner.invoke(
            app,
            [
                "download",
                "BTCUSDT",
                "--days",
                "1",
                "--output",
                str(tmp_path),
                "--quiet",
            ],
        )
        assert result.exit_code == 0
        # Verify file was created
        processed_dir = tmp_path / "processed" / "futures-usdt"
        assert processed_dir.exists()
        parquet_files = list(processed_dir.glob("*.parquet"))
        assert len(parquet_files) == 1

    def test_download_multiple_symbols(self, tmp_path: Path) -> None:
        """Test downloading multiple symbols."""
        result = runner.invoke(
            app,
            [
                "download",
                "BTCUSDT",
                "ETHUSDT",
                "--days",
                "1",
                "--output",
                str(tmp_path),
                "--quiet",
            ],
        )
        assert result.exit_code == 0

    def test_download_missing_date_params(self) -> None:
        """Test error when no date parameters provided."""
        result = runner.invoke(app, ["download", "BTCUSDT"])
        assert result.exit_code == 1
        assert "Must specify --months, --days, or --start/--end" in result.stdout

    def test_download_invalid_timeframe(self) -> None:
        """Test error for invalid timeframe."""
        result = runner.invoke(
            app,
            [
                "download",
                "BTCUSDT",
                "--days",
                "1",
                "--timeframe",
                "invalid",
            ],
        )
        assert result.exit_code == 1
        assert "Invalid timeframe" in result.stdout

    def test_download_invalid_format(self) -> None:
        """Test error for invalid output format."""
        result = runner.invoke(
            app,
            [
                "download",
                "BTCUSDT",
                "--days",
                "1",
                "--format",
                "invalid",
            ],
        )
        assert result.exit_code == 1
        assert "Invalid format" in result.stdout

    def test_download_csv_format(self, tmp_path: Path) -> None:
        """Test downloading as CSV."""
        result = runner.invoke(
            app,
            [
                "download",
                "BTCUSDT",
                "--days",
                "1",
                "--output",
                str(tmp_path),
                "--format",
                "csv",
                "--quiet",
            ],
        )
        assert result.exit_code == 0
        csv_files = list((tmp_path / "processed" / "futures-usdt").glob("*.csv"))
        assert len(csv_files) == 1

    def test_download_spot_market(self, tmp_path: Path) -> None:
        """Test downloading spot market data."""
        result = runner.invoke(
            app,
            [
                "download",
                "BTCUSDT",
                "--start",
                "2024-12-01",
                "--end",
                "2024-12-02",
                "--market-type",
                "spot",
                "--output",
                str(tmp_path),
                "--quiet",
            ],
        )
        assert result.exit_code == 0

    def test_download_with_date_range(self, tmp_path: Path) -> None:
        """Test downloading with start/end dates."""
        result = runner.invoke(
            app,
            [
                "download",
                "BTCUSDT",
                "--start",
                "2024-12-01",
                "--end",
                "2024-12-02",
                "--output",
                str(tmp_path),
                "--quiet",
            ],
        )
        assert result.exit_code == 0

    def test_download_no_symbols_no_all(self) -> None:
        """Test error when no symbols and no --all flag."""
        result = runner.invoke(app, ["download", "--days", "1"])
        assert result.exit_code == 1
        assert "Must specify symbols or use --all" in result.stdout


class TestDownloadAllCommand:
    """Tests for the download-all command."""

    def test_download_all_requires_date_params(self) -> None:
        """Test error when no date parameters."""
        result = runner.invoke(app, ["download-all"])
        assert result.exit_code == 1
        assert "Must specify --months, --days, or --start/--end" in result.stdout

    def test_download_all_with_confirmation_no(self) -> None:
        """Test cancellation via confirmation prompt."""
        result = runner.invoke(app, ["download-all", "--days", "1"], input="n\n")
        assert result.exit_code == 0  # Clean exit after "no"


class TestVersionFlag:
    """Tests for version flag."""

    def test_version_short(self) -> None:
        """Test -V flag."""
        result = runner.invoke(app, ["-V"])
        assert result.exit_code == 0
        assert "binance-ohlcv-collector" in result.stdout

    def test_version_long(self) -> None:
        """Test --version flag."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0


class TestHelpFlags:
    """Tests for help flags."""

    def test_main_help(self) -> None:
        """Test main --help."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "download" in result.stdout
        assert "list" in result.stdout

    def test_download_help(self) -> None:
        """Test download --help."""
        result = runner.invoke(app, ["download", "--help"])
        assert result.exit_code == 0
        assert "--timeframe" in result.stdout
        assert "--months" in result.stdout
