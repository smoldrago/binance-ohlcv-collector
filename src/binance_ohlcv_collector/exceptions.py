"""Custom exceptions for binance-ohlcv-collector."""

from __future__ import annotations


class BinanceVisionError(Exception):
    """Base exception for binance-ohlcv-collector."""

    pass


class DownloadError(BinanceVisionError):
    """Error during file download."""

    pass


class ChecksumError(BinanceVisionError):
    """Checksum verification failed."""

    pass


class ValidationError(BinanceVisionError):
    """Data validation failed."""

    pass


class SymbolNotFoundError(BinanceVisionError):
    """Symbol not found on Binance."""

    def __init__(self, symbol: str, market_type: str, suggestions: list[str] | None = None):
        self.symbol = symbol
        self.market_type = market_type
        self.suggestions = suggestions or []

        message = f"Symbol '{symbol}' not found for market type '{market_type}'"
        if self.suggestions:
            message += f". Did you mean: {', '.join(self.suggestions[:5])}?"

        super().__init__(message)


class NoDataAvailableError(BinanceVisionError):
    """No data available for the requested period."""

    def __init__(self, symbol: str, start_date: str, end_date: str):
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date

        super().__init__(f"No data available for '{symbol}' between {start_date} and {end_date}")


class RateLimitError(BinanceVisionError):
    """Rate limit exceeded."""

    pass
