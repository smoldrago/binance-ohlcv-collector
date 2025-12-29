"""Tests for config module."""

import pytest

from binance_ohlcv_collector.config import (
    DEFAULT_MARKET_TYPE,
    VALID_OUTPUT_FORMATS,
    VALID_TIMEFRAMES,
    MarketType,
    detect_market_type,
    validate_output_format,
    validate_timeframe,
)


class TestMarketType:
    """Tests for MarketType enum."""

    def test_market_type_values(self) -> None:
        """Test that market types have correct values."""
        assert MarketType.SPOT.value == "spot"
        assert MarketType.FUTURES_USDT.value == "futures-usdt"
        assert MarketType.FUTURES_COIN.value == "futures-coin"

    def test_market_type_is_string(self) -> None:
        """Test that MarketType is a string enum."""
        assert isinstance(MarketType.SPOT, str)
        assert MarketType.SPOT == "spot"


class TestDetectMarketType:
    """Tests for detect_market_type function."""

    def test_detect_coin_m_perp(self) -> None:
        """Test detection of COIN-M perpetual symbols."""
        assert detect_market_type("ETHUSD_PERP") == MarketType.FUTURES_COIN
        assert detect_market_type("BTCUSD_PERP") == MarketType.FUTURES_COIN
        assert detect_market_type("ethusd_perp") == MarketType.FUTURES_COIN

    def test_detect_default_usdt_futures(self) -> None:
        """Test that regular symbols default to USDT-M futures."""
        assert detect_market_type("ETHUSDT") == DEFAULT_MARKET_TYPE
        assert detect_market_type("BTCUSDT") == DEFAULT_MARKET_TYPE
        assert detect_market_type("SOLUSDT") == DEFAULT_MARKET_TYPE

    def test_default_is_futures_usdt(self) -> None:
        """Test that default market type is USDT-M futures."""
        assert DEFAULT_MARKET_TYPE == MarketType.FUTURES_USDT


class TestValidateTimeframe:
    """Tests for validate_timeframe function."""

    def test_valid_timeframes(self) -> None:
        """Test validation of all valid timeframes."""
        for tf in VALID_TIMEFRAMES:
            assert validate_timeframe(tf) == tf

    def test_invalid_timeframe_raises(self) -> None:
        """Test that invalid timeframes raise ValueError."""
        with pytest.raises(ValueError, match="Invalid timeframe"):
            validate_timeframe("invalid")

        with pytest.raises(ValueError, match="Invalid timeframe"):
            validate_timeframe("10m")

        with pytest.raises(ValueError, match="Invalid timeframe"):
            validate_timeframe("")

    def test_error_message_includes_valid_options(self) -> None:
        """Test that error message includes valid options."""
        with pytest.raises(ValueError, match="15m"):
            validate_timeframe("invalid")


class TestValidateOutputFormat:
    """Tests for validate_output_format function."""

    def test_valid_formats(self) -> None:
        """Test validation of all valid output formats."""
        for fmt in VALID_OUTPUT_FORMATS:
            assert validate_output_format(fmt) == fmt

    def test_invalid_format_raises(self) -> None:
        """Test that invalid formats raise ValueError."""
        with pytest.raises(ValueError, match="Invalid format"):
            validate_output_format("json")

        with pytest.raises(ValueError, match="Invalid format"):
            validate_output_format("excel")

    def test_error_message_includes_valid_options(self) -> None:
        """Test that error message includes valid options."""
        with pytest.raises(ValueError, match="parquet"):
            validate_output_format("invalid")
