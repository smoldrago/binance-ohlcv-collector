"""Fetch and cache available symbols from Binance API."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from difflib import get_close_matches
from pathlib import Path

import httpx

from binance_ohlcv_collector.config import (
    MARKET_TYPE_API_URLS,
    MarketType,
)
from binance_ohlcv_collector.exceptions import SymbolNotFoundError

# Cache settings
CACHE_DIR = Path.home() / ".cache" / "binance-ohlcv-collector"
CACHE_EXPIRY_HOURS = 24


def _get_cache_path(market_type: MarketType) -> Path:
    """Get cache file path for a market type.

    Parameters
    ----------
    market_type : MarketType
        The market type to get cache path for.

    Returns
    -------
    Path
        Path to the cache file.

    """
    return CACHE_DIR / f"symbols_{market_type.value}.json"


def _is_cache_valid(cache_path: Path) -> bool:
    """Check if cache file exists and is not expired.

    Parameters
    ----------
    cache_path : Path
        Path to the cache file.

    Returns
    -------
    bool
        True if cache is valid, False otherwise.

    """
    if not cache_path.exists():
        return False

    # Check if cache is expired
    mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
    return datetime.now() - mtime < timedelta(hours=CACHE_EXPIRY_HOURS)


def _load_from_cache(cache_path: Path) -> list[dict[str, str]]:
    """Load symbols from cache file.

    Parameters
    ----------
    cache_path : Path
        Path to the cache file.

    Returns
    -------
    list[dict[str, str]]
        List of symbol dictionaries.

    """
    with cache_path.open() as f:
        return json.load(f)  # type: ignore[no-any-return]


def _save_to_cache(cache_path: Path, symbols: list[dict[str, str]]) -> None:
    """Save symbols to cache file.

    Parameters
    ----------
    cache_path : Path
        Path to the cache file.
    symbols : list[dict[str, str]]
        List of symbol dictionaries to cache.

    """
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("w") as f:
        json.dump(symbols, f)


def _extract_symbols_from_response(data: dict, market_type: MarketType) -> list[dict[str, str]]:  # type: ignore[type-arg]
    """Extract symbol data from Binance API response.

    Parameters
    ----------
    data : dict
        Response data from Binance API.
    market_type : MarketType
        The market type being queried.

    Returns
    -------
    list[dict[str, str]]
        List of symbol dictionaries with symbol, baseAsset, and quoteAsset.

    """
    symbols = []

    for symbol_info in data.get("symbols", []):
        symbol = symbol_info.get("symbol", "")
        base_asset = symbol_info.get("baseAsset", "")
        quote_asset = symbol_info.get("quoteAsset", "")

        # Filter based on market type
        if market_type == MarketType.SPOT:
            # Include all spot symbols
            if symbol_info.get("status") == "TRADING":
                symbols.append(
                    {
                        "symbol": symbol,
                        "baseAsset": base_asset,
                        "quoteAsset": quote_asset,
                    }
                )

        elif market_type == MarketType.FUTURES_USDT:
            # USDT-M futures: filter for perpetuals with USDT margin
            contract_type = symbol_info.get("contractType", "")
            margin_asset = symbol_info.get("marginAsset", "")
            if contract_type == "PERPETUAL" and margin_asset == "USDT":
                symbols.append(
                    {
                        "symbol": symbol,
                        "baseAsset": base_asset,
                        "quoteAsset": quote_asset,
                    }
                )

        elif market_type == MarketType.FUTURES_COIN:
            # COIN-M futures: filter for perpetuals
            contract_type = symbol_info.get("contractType", "")
            if contract_type == "PERPETUAL":
                symbols.append(
                    {
                        "symbol": symbol,
                        "baseAsset": base_asset,
                        "quoteAsset": quote_asset,
                    }
                )

    return sorted(symbols, key=lambda x: x["symbol"])


async def fetch_symbols_async(
    market_type: MarketType,
    use_cache: bool = True,
) -> list[dict[str, str]]:
    """Fetch available symbols for a market type from Binance API.

    Parameters
    ----------
    market_type : MarketType
        The market type to fetch symbols for.
    use_cache : bool
        Whether to use cached symbols if available.

    Returns
    -------
    list[dict[str, str]]
        List of available symbol dictionaries.

    """
    cache_path = _get_cache_path(market_type)

    # Check cache first
    if use_cache and _is_cache_valid(cache_path):
        return _load_from_cache(cache_path)

    # Fetch from API
    url = MARKET_TYPE_API_URLS[market_type]

    async with httpx.AsyncClient() as client:
        response = await client.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

    symbols = _extract_symbols_from_response(data, market_type)

    # Cache the results
    _save_to_cache(cache_path, symbols)

    return symbols


def fetch_symbols(
    market_type: MarketType,
    use_cache: bool = True,
) -> list[dict[str, str]]:
    """Fetch available symbols for a market type from Binance API (sync version).

    Parameters
    ----------
    market_type : MarketType
        The market type to fetch symbols for.
    use_cache : bool
        Whether to use cached symbols if available.

    Returns
    -------
    list[dict[str, str]]
        List of available symbol dictionaries.

    """
    cache_path = _get_cache_path(market_type)

    # Check cache first
    if use_cache and _is_cache_valid(cache_path):
        return _load_from_cache(cache_path)

    # Fetch from API
    url = MARKET_TYPE_API_URLS[market_type]

    with httpx.Client() as client:
        response = client.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

    symbols = _extract_symbols_from_response(data, market_type)

    # Cache the results
    _save_to_cache(cache_path, symbols)

    return symbols


def validate_symbol(
    symbol: str,
    market_type: MarketType,
    use_cache: bool = True,
) -> str:
    """Validate that a symbol exists for the given market type.

    Parameters
    ----------
    symbol : str
        The symbol to validate.
    market_type : MarketType
        The market type to check against.
    use_cache : bool
        Whether to use cached symbols if available.

    Returns
    -------
    str
        The validated symbol (uppercased).

    Raises
    ------
    SymbolNotFoundError
        If the symbol is not found.

    """
    symbol = symbol.upper()
    symbols = fetch_symbols(market_type, use_cache=use_cache)
    symbol_names = [s["symbol"] for s in symbols]

    if symbol in symbol_names:
        return symbol

    # Find similar symbols for suggestion
    suggestions = get_close_matches(symbol, symbol_names, n=5, cutoff=0.6)

    raise SymbolNotFoundError(symbol, market_type.value, suggestions)


async def validate_symbol_async(
    symbol: str,
    market_type: MarketType,
    use_cache: bool = True,
) -> str:
    """Validate that a symbol exists for the given market type (async version).

    Parameters
    ----------
    symbol : str
        The symbol to validate.
    market_type : MarketType
        The market type to check against.
    use_cache : bool
        Whether to use cached symbols if available.

    Returns
    -------
    str
        The validated symbol (uppercased).

    Raises
    ------
    SymbolNotFoundError
        If the symbol is not found.

    """
    symbol = symbol.upper()
    symbols = await fetch_symbols_async(market_type, use_cache=use_cache)
    symbol_names = [s["symbol"] for s in symbols]

    if symbol in symbol_names:
        return symbol

    # Find similar symbols for suggestion
    suggestions = get_close_matches(symbol, symbol_names, n=5, cutoff=0.6)

    raise SymbolNotFoundError(symbol, market_type.value, suggestions)


def filter_symbols(
    symbols: list[dict[str, str]],
    search: str | None = None,
    base_asset: str | None = None,
    quote_asset: str | None = None,
) -> list[dict[str, str]]:
    """Filter symbols based on search criteria.

    Parameters
    ----------
    symbols : list[dict[str, str]]
        List of symbol dictionaries to filter.
    search : str | None
        Search term to match against symbol names (case-insensitive).
    base_asset : str | None
        Filter by base asset (e.g., 'BTC', 'ETH').
    quote_asset : str | None
        Filter by quote asset (e.g., 'USDT', 'BUSD').

    Returns
    -------
    list[dict[str, str]]
        Filtered list of symbols.

    """
    filtered = symbols

    if search:
        search_upper = search.upper()
        filtered = [s for s in filtered if search_upper in s["symbol"]]

    if base_asset:
        base_upper = base_asset.upper()
        filtered = [s for s in filtered if s["baseAsset"] == base_upper]

    if quote_asset:
        quote_upper = quote_asset.upper()
        filtered = [s for s in filtered if s["quoteAsset"] == quote_upper]

    return filtered


def clear_cache(market_type: MarketType | None = None) -> None:
    """Clear the symbol cache.

    Parameters
    ----------
    market_type : MarketType | None
        The market type to clear cache for. If None, clears all caches.

    """
    if market_type is not None:
        cache_path = _get_cache_path(market_type)
        if cache_path.exists():
            cache_path.unlink()
    else:
        # Clear all caches
        for mt in MarketType:
            cache_path = _get_cache_path(mt)
            if cache_path.exists():
                cache_path.unlink()
