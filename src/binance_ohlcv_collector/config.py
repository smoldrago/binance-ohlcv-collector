"""Configuration constants and types for binance-ohlcv-collector."""

from enum import Enum
from typing import Literal

# Binance Vision base URL
BINANCE_VISION_BASE_URL = "https://data.binance.vision/data"

# Binance API base URLs for fetching symbols
BINANCE_API_SPOT_URL = "https://api.binance.com/api/v3/exchangeInfo"
BINANCE_API_FUTURES_USDT_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
BINANCE_API_FUTURES_COIN_URL = "https://dapi.binance.com/dapi/v1/exchangeInfo"


class MarketType(str, Enum):
    """Supported market types."""

    SPOT = "spot"
    FUTURES_USDT = "futures-usdt"
    FUTURES_COIN = "futures-coin"


# Mapping from MarketType to Binance Vision path segment
MARKET_TYPE_PATHS: dict[MarketType, str] = {
    MarketType.SPOT: "spot",
    MarketType.FUTURES_USDT: "futures/um",
    MarketType.FUTURES_COIN: "futures/cm",
}

# Mapping from MarketType to Binance API URL
MARKET_TYPE_API_URLS: dict[MarketType, str] = {
    MarketType.SPOT: BINANCE_API_SPOT_URL,
    MarketType.FUTURES_USDT: BINANCE_API_FUTURES_USDT_URL,
    MarketType.FUTURES_COIN: BINANCE_API_FUTURES_COIN_URL,
}

# Valid timeframes supported by Binance Vision
VALID_TIMEFRAMES: tuple[str, ...] = (
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1mo",
)

# Valid output formats
OutputFormat = Literal["parquet", "csv"]
VALID_OUTPUT_FORMATS: tuple[OutputFormat, ...] = ("parquet", "csv")

# Valid granularities for downloading
Granularity = Literal["daily", "monthly"]
VALID_GRANULARITIES: tuple[Granularity, ...] = ("daily", "monthly")

# Default settings
DEFAULT_MARKET_TYPE = MarketType.FUTURES_USDT
DEFAULT_TIMEFRAME = "15m"
DEFAULT_OUTPUT_FORMAT: OutputFormat = "parquet"
DEFAULT_CONCURRENCY = 4
DEFAULT_TIMEOUT = 30  # seconds
DEFAULT_RETRIES = 3

# Binance klines CSV columns (no header in files)
KLINE_COLUMNS: list[str] = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
]

# Output columns for processed data
OUTPUT_COLUMNS: list[str] = ["timestamp", "open", "high", "low", "close", "volume"]


def detect_market_type(symbol: str) -> MarketType:
    """Detect market type from symbol pattern.

    Parameters
    ----------
    symbol : str
        Trading pair symbol (e.g., "ETHUSDT", "ETHUSD_PERP")

    Returns
    -------
    MarketType
        Detected or default market type.

    """
    # COIN-M futures have _PERP suffix
    if "_PERP" in symbol.upper():
        return MarketType.FUTURES_COIN

    # Default to USDT-M futures
    return DEFAULT_MARKET_TYPE


def validate_timeframe(timeframe: str) -> str:
    """Validate and return timeframe.

    Parameters
    ----------
    timeframe : str
        Kline interval to validate.

    Returns
    -------
    str
        Validated timeframe.

    Raises
    ------
    ValueError
        If timeframe is not valid.

    """
    if timeframe not in VALID_TIMEFRAMES:
        raise ValueError(
            f"Invalid timeframe '{timeframe}'. Must be one of: {', '.join(VALID_TIMEFRAMES)}"
        )
    return timeframe


def validate_output_format(format: str) -> OutputFormat:
    """Validate and return output format.

    Parameters
    ----------
    format : str
        Output format to validate.

    Returns
    -------
    OutputFormat
        Validated output format.

    Raises
    ------
    ValueError
        If format is not valid.

    """
    if format not in VALID_OUTPUT_FORMATS:
        raise ValueError(
            f"Invalid format '{format}'. Must be one of: {', '.join(VALID_OUTPUT_FORMATS)}"
        )
    return format  # type: ignore[return-value]
