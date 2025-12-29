"""Tests for exceptions module."""

from binance_ohlcv_collector.exceptions import (
    BinanceVisionError,
    ChecksumError,
    DownloadError,
    NoDataAvailableError,
    RateLimitError,
    SymbolNotFoundError,
    ValidationError,
)


class TestExceptionHierarchy:
    """Tests for exception hierarchy."""

    def test_all_exceptions_inherit_from_base(self) -> None:
        """Test that all exceptions inherit from BinanceVisionError."""
        assert issubclass(DownloadError, BinanceVisionError)
        assert issubclass(ChecksumError, BinanceVisionError)
        assert issubclass(ValidationError, BinanceVisionError)
        assert issubclass(SymbolNotFoundError, BinanceVisionError)
        assert issubclass(NoDataAvailableError, BinanceVisionError)
        assert issubclass(RateLimitError, BinanceVisionError)

    def test_base_inherits_from_exception(self) -> None:
        """Test that base exception inherits from Exception."""
        assert issubclass(BinanceVisionError, Exception)


class TestSymbolNotFoundError:
    """Tests for SymbolNotFoundError."""

    def test_basic_message(self) -> None:
        """Test basic error message without suggestions."""
        error = SymbolNotFoundError("INVALID", "futures-usdt")
        assert "INVALID" in str(error)
        assert "futures-usdt" in str(error)
        assert error.symbol == "INVALID"
        assert error.market_type == "futures-usdt"

    def test_with_suggestions(self) -> None:
        """Test error message with suggestions."""
        suggestions = ["ETHUSDT", "ETHUSD_PERP"]
        error = SymbolNotFoundError("ETHUSD", "futures-usdt", suggestions)
        assert "Did you mean" in str(error)
        assert "ETHUSDT" in str(error)
        assert error.suggestions == suggestions

    def test_suggestions_limited_to_five(self) -> None:
        """Test that suggestions are limited to 5 in message."""
        suggestions = ["SYM1", "SYM2", "SYM3", "SYM4", "SYM5", "SYM6", "SYM7"]
        error = SymbolNotFoundError("INVALID", "spot", suggestions)
        # SYM6 and SYM7 should not be in message
        assert "SYM6" not in str(error)
        assert "SYM7" not in str(error)
        # But all suggestions should be stored
        assert len(error.suggestions) == 7

    def test_empty_suggestions(self) -> None:
        """Test with empty suggestions list."""
        error = SymbolNotFoundError("INVALID", "spot", [])
        assert "Did you mean" not in str(error)
        assert error.suggestions == []

    def test_none_suggestions(self) -> None:
        """Test with None suggestions."""
        error = SymbolNotFoundError("INVALID", "spot", None)
        assert error.suggestions == []


class TestNoDataAvailableError:
    """Tests for NoDataAvailableError."""

    def test_error_message(self) -> None:
        """Test error message contains all info."""
        error = NoDataAvailableError("ETHUSDT", "2024-01-01", "2024-12-31")
        assert "ETHUSDT" in str(error)
        assert "2024-01-01" in str(error)
        assert "2024-12-31" in str(error)
        assert error.symbol == "ETHUSDT"
        assert error.start_date == "2024-01-01"
        assert error.end_date == "2024-12-31"


class TestBasicExceptions:
    """Tests for basic exceptions without custom attributes."""

    def test_download_error(self) -> None:
        """Test DownloadError can be raised with message."""
        error = DownloadError("Connection failed")
        assert str(error) == "Connection failed"

    def test_checksum_error(self) -> None:
        """Test ChecksumError can be raised with message."""
        error = ChecksumError("Checksum mismatch")
        assert str(error) == "Checksum mismatch"

    def test_validation_error(self) -> None:
        """Test ValidationError can be raised with message."""
        error = ValidationError("Invalid data")
        assert str(error) == "Invalid data"

    def test_rate_limit_error(self) -> None:
        """Test RateLimitError can be raised with message."""
        error = RateLimitError("Too many requests")
        assert str(error) == "Too many requests"
