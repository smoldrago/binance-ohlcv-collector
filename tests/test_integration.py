"""Integration tests for public API using real Binance data."""

from pathlib import Path
from typing import cast

import pandas as pd
import pytest

from binance_ohlcv_collector import MarketType, download, download_all, list_symbols


class TestListSymbols:
    """Tests for list_symbols function."""

    def test_list_futures_usdt(self) -> None:
        """Test listing USDT-M futures symbols."""
        symbols = list_symbols(market_type="futures-usdt")
        assert isinstance(symbols, list)
        assert len(symbols) > 0
        symbol_names = [s["symbol"] for s in symbols]
        assert "BTCUSDT" in symbol_names
        assert "ETHUSDT" in symbol_names

    def test_list_spot(self) -> None:
        """Test listing spot symbols."""
        symbols = list_symbols(market_type="spot")
        assert isinstance(symbols, list)
        assert len(symbols) > 0

    def test_list_futures_coin(self) -> None:
        """Test listing COIN-M futures symbols."""
        symbols = list_symbols(market_type="futures-coin")
        assert isinstance(symbols, list)
        assert len(symbols) > 0
        symbol_names = [s["symbol"] for s in symbols]
        assert any("_PERP" in s for s in symbol_names)

    def test_list_with_market_type_enum(self) -> None:
        """Test using MarketType enum directly."""
        symbols = list_symbols(market_type=MarketType.FUTURES_USDT)
        assert isinstance(symbols, list)
        assert len(symbols) > 0


class TestDownloadSingle:
    """Tests for download() with single symbol."""

    def test_download_returns_dataframe(self) -> None:
        """Test that download returns DataFrame when no output_dir."""
        result = download("BTCUSDT", days=1)
        assert isinstance(result, pd.DataFrame)
        df = cast("pd.DataFrame", result)
        assert not df.empty
        assert "timestamp" in df.columns
        assert "open" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns

    def test_download_with_output_dir(self, tmp_path: Path) -> None:
        """Test that download returns Path when output_dir provided."""
        result = download("BTCUSDT", days=1, output_dir=tmp_path)
        assert isinstance(result, Path)
        assert result.exists()
        assert result.suffix == ".parquet"

    def test_download_csv_format(self, tmp_path: Path) -> None:
        """Test CSV output format."""
        result = download("BTCUSDT", days=1, output_dir=tmp_path, format="csv")
        assert isinstance(result, Path)
        assert result.suffix == ".csv"

    def test_download_spot_market(self) -> None:
        """Test downloading spot market data."""
        result = download("BTCUSDT", start="2024-12-01", end="2024-12-02", market_type="spot")
        assert isinstance(result, pd.DataFrame)
        df = cast("pd.DataFrame", result)
        assert not df.empty

    def test_download_futures_coin(self) -> None:
        """Test downloading COIN-M futures data."""
        result = download("BTCUSD_PERP", days=1, market_type="futures-coin")
        assert isinstance(result, pd.DataFrame)
        df = cast("pd.DataFrame", result)
        assert not df.empty

    def test_download_different_timeframes(self) -> None:
        """Test different timeframes."""
        for tf in ["1h", "4h", "1d"]:
            result = download("BTCUSDT", days=1, timeframe=tf)
            assert isinstance(result, pd.DataFrame)

    def test_download_with_date_range(self) -> None:
        """Test downloading with start/end dates."""
        result = download("BTCUSDT", start="2024-12-01", end="2024-12-02")
        assert isinstance(result, pd.DataFrame)
        df = cast("pd.DataFrame", result)
        assert not df.empty

    def test_download_with_months(self) -> None:
        """Test downloading with months parameter."""
        result = download("BTCUSDT", months=1, timeframe="1d")
        assert isinstance(result, pd.DataFrame)
        df = cast("pd.DataFrame", result)
        assert not df.empty


class TestDownloadMultiple:
    """Tests for download() with multiple symbols."""

    def test_download_multiple_returns_dict(self) -> None:
        """Test that download returns dict for multiple symbols."""
        result = download(["BTCUSDT", "ETHUSDT"], days=1)
        assert isinstance(result, dict)
        assert "BTCUSDT" in result
        assert "ETHUSDT" in result
        assert isinstance(result["BTCUSDT"], pd.DataFrame)
        assert isinstance(result["ETHUSDT"], pd.DataFrame)

    def test_download_multiple_with_output_dir(self, tmp_path: Path) -> None:
        """Test multiple symbols with output_dir."""
        result = download(["BTCUSDT", "ETHUSDT"], days=1, output_dir=tmp_path)
        assert isinstance(result, dict)
        assert all(isinstance(v, Path) for v in result.values())


class TestDownloadAll:
    """Tests for download_all function."""

    @pytest.mark.skip(reason="download_all is too slow for regular testing")
    def test_download_all_returns_dict(self, tmp_path: Path) -> None:
        """Test download_all returns dict of paths."""
        # Use futures-usdt which has fewer symbols in test environment
        result = download_all(
            market_type="futures-usdt",
            days=1,
            output_dir=tmp_path,
        )
        assert isinstance(result, dict)
        assert len(result) > 0


class TestDataQuality:
    """Tests for data quality."""

    def test_dataframe_columns(self) -> None:
        """Test DataFrame has correct columns."""
        result = download("BTCUSDT", days=1)
        assert isinstance(result, pd.DataFrame)
        df = cast("pd.DataFrame", result)
        expected_columns = ["timestamp", "open", "high", "low", "close", "volume"]
        assert list(df.columns) == expected_columns

    def test_timestamp_is_datetime(self) -> None:
        """Test timestamp column is datetime type."""
        result = download("BTCUSDT", days=1)
        assert isinstance(result, pd.DataFrame)
        df = cast("pd.DataFrame", result)
        assert pd.api.types.is_datetime64_any_dtype(df["timestamp"])

    def test_data_is_sorted(self) -> None:
        """Test data is sorted by timestamp."""
        result = download("BTCUSDT", days=1)
        assert isinstance(result, pd.DataFrame)
        df = cast("pd.DataFrame", result)
        timestamps = df["timestamp"]
        assert timestamps.is_monotonic_increasing

    def test_no_duplicate_timestamps(self) -> None:
        """Test no duplicate timestamps."""
        result = download("BTCUSDT", days=1)
        assert isinstance(result, pd.DataFrame)
        df = cast("pd.DataFrame", result)
        timestamps = df["timestamp"]
        assert timestamps.is_unique
