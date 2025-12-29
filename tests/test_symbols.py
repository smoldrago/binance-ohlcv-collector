"""Tests for symbols module."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
import respx
from httpx import Response

from binance_ohlcv_collector.config import (
    BINANCE_API_FUTURES_COIN_URL,
    BINANCE_API_FUTURES_USDT_URL,
    BINANCE_API_SPOT_URL,
    MarketType,
)
from binance_ohlcv_collector.exceptions import SymbolNotFoundError
from binance_ohlcv_collector.symbols import (
    _extract_symbols_from_response,
    _get_cache_path,
    clear_cache,
    fetch_symbols,
    fetch_symbols_async,
    filter_symbols,
    validate_symbol,
    validate_symbol_async,
)

# Sample API responses
SPOT_RESPONSE = {
    "symbols": [
        {"symbol": "ETHUSDT", "baseAsset": "ETH", "quoteAsset": "USDT", "status": "TRADING"},
        {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "status": "TRADING"},
        {
            "symbol": "OLDPAIR",
            "baseAsset": "OLD",
            "quoteAsset": "PAIR",
            "status": "BREAK",
        },  # Not trading
    ]
}

FUTURES_USDT_RESPONSE = {
    "symbols": [
        {
            "symbol": "ETHUSDT",
            "baseAsset": "ETH",
            "quoteAsset": "USDT",
            "contractType": "PERPETUAL",
            "marginAsset": "USDT",
        },
        {
            "symbol": "BTCUSDT",
            "baseAsset": "BTC",
            "quoteAsset": "USDT",
            "contractType": "PERPETUAL",
            "marginAsset": "USDT",
        },
        {
            "symbol": "ETHUSD_250328",
            "baseAsset": "ETH",
            "quoteAsset": "USD",
            "contractType": "NEXT_QUARTER",
            "marginAsset": "USDT",
        },
    ]
}

FUTURES_COIN_RESPONSE = {
    "symbols": [
        {
            "symbol": "ETHUSD_PERP",
            "baseAsset": "ETH",
            "quoteAsset": "USD",
            "contractType": "PERPETUAL",
        },
        {
            "symbol": "BTCUSD_PERP",
            "baseAsset": "BTC",
            "quoteAsset": "USD",
            "contractType": "PERPETUAL",
        },
        {
            "symbol": "ETHUSD_250328",
            "baseAsset": "ETH",
            "quoteAsset": "USD",
            "contractType": "NEXT_QUARTER",
        },
    ]
}


class TestExtractSymbols:
    """Tests for _extract_symbols_from_response."""

    def test_extract_spot_symbols(self) -> None:
        """Test extracting spot symbols filters by trading status."""
        symbols = _extract_symbols_from_response(SPOT_RESPONSE, MarketType.SPOT)
        symbol_names = [s["symbol"] for s in symbols]
        assert "ETHUSDT" in symbol_names
        assert "BTCUSDT" in symbol_names
        assert "OLDPAIR" not in symbol_names  # Not trading

    def test_extract_futures_usdt_symbols(self) -> None:
        """Test extracting USDT-M futures filters for perpetuals."""
        symbols = _extract_symbols_from_response(FUTURES_USDT_RESPONSE, MarketType.FUTURES_USDT)
        symbol_names = [s["symbol"] for s in symbols]
        assert "ETHUSDT" in symbol_names
        assert "BTCUSDT" in symbol_names
        assert "ETHUSD_250328" not in symbol_names  # Quarterly, not perpetual

    def test_extract_futures_coin_symbols(self) -> None:
        """Test extracting COIN-M futures filters for perpetuals."""
        symbols = _extract_symbols_from_response(FUTURES_COIN_RESPONSE, MarketType.FUTURES_COIN)
        symbol_names = [s["symbol"] for s in symbols]
        assert "ETHUSD_PERP" in symbol_names
        assert "BTCUSD_PERP" in symbol_names
        assert "ETHUSD_250328" not in symbol_names  # Quarterly, not perpetual

    def test_symbols_are_sorted(self) -> None:
        """Test that returned symbols are sorted."""
        symbols = _extract_symbols_from_response(SPOT_RESPONSE, MarketType.SPOT)
        symbol_names = [s["symbol"] for s in symbols]
        assert symbol_names == sorted(symbol_names)

    def test_extract_includes_metadata(self) -> None:
        """Test that extracted symbols include metadata."""
        symbols = _extract_symbols_from_response(SPOT_RESPONSE, MarketType.SPOT)
        assert len(symbols) > 0
        assert "symbol" in symbols[0]
        assert "baseAsset" in symbols[0]
        assert "quoteAsset" in symbols[0]

        # Verify values
        eth_symbol = next(s for s in symbols if s["symbol"] == "ETHUSDT")
        assert eth_symbol["baseAsset"] == "ETH"
        assert eth_symbol["quoteAsset"] == "USDT"


class TestGetCachePath:
    """Tests for _get_cache_path."""

    def test_cache_path_includes_market_type(self) -> None:
        """Test that cache path includes market type."""
        path = _get_cache_path(MarketType.SPOT)
        assert "spot" in path.name

        path = _get_cache_path(MarketType.FUTURES_USDT)
        assert "futures-usdt" in path.name

    def test_cache_path_is_in_home_cache(self) -> None:
        """Test that cache is in user's cache directory."""
        path = _get_cache_path(MarketType.SPOT)
        assert ".cache" in str(path)
        assert "binance-ohlcv-collector" in str(path)


class TestFetchSymbols:
    """Tests for fetch_symbols function."""

    @respx.mock
    def test_fetch_spot_symbols(self) -> None:
        """Test fetching spot symbols from API."""
        respx.get(BINANCE_API_SPOT_URL).mock(return_value=Response(200, json=SPOT_RESPONSE))

        # Clear cache to force API call
        clear_cache(MarketType.SPOT)

        symbols = fetch_symbols(MarketType.SPOT, use_cache=False)
        symbol_names = [s["symbol"] for s in symbols]
        assert "ETHUSDT" in symbol_names
        assert "BTCUSDT" in symbol_names

    @respx.mock
    def test_fetch_futures_usdt_symbols(self) -> None:
        """Test fetching USDT-M futures symbols from API."""
        respx.get(BINANCE_API_FUTURES_USDT_URL).mock(
            return_value=Response(200, json=FUTURES_USDT_RESPONSE)
        )

        clear_cache(MarketType.FUTURES_USDT)

        symbols = fetch_symbols(MarketType.FUTURES_USDT, use_cache=False)
        symbol_names = [s["symbol"] for s in symbols]
        assert "ETHUSDT" in symbol_names
        assert "BTCUSDT" in symbol_names

    @respx.mock
    def test_fetch_futures_coin_symbols(self) -> None:
        """Test fetching COIN-M futures symbols from API."""
        respx.get(BINANCE_API_FUTURES_COIN_URL).mock(
            return_value=Response(200, json=FUTURES_COIN_RESPONSE)
        )

        clear_cache(MarketType.FUTURES_COIN)

        symbols = fetch_symbols(MarketType.FUTURES_COIN, use_cache=False)
        symbol_names = [s["symbol"] for s in symbols]
        assert "ETHUSD_PERP" in symbol_names
        assert "BTCUSD_PERP" in symbol_names

    def test_uses_cache_when_valid(self, tmp_path: Path) -> None:
        """Test that cache is used when valid."""
        cached_symbols = [
            {"symbol": "CACHED1", "baseAsset": "CACHE", "quoteAsset": "D1"},
            {"symbol": "CACHED2", "baseAsset": "CACHE", "quoteAsset": "D2"},
        ]

        with (
            patch(
                "binance_ohlcv_collector.symbols._get_cache_path",
                return_value=tmp_path / "cache.json",
            ),
            patch(
                "binance_ohlcv_collector.symbols._is_cache_valid",
                return_value=True,
            ),
        ):
            # Write cache file
            cache_path = tmp_path / "cache.json"
            cache_path.write_text(json.dumps(cached_symbols))

            symbols = fetch_symbols(MarketType.SPOT, use_cache=True)
            assert symbols == cached_symbols


class TestFetchSymbolsAsync:
    """Tests for fetch_symbols_async function."""

    @respx.mock
    async def test_fetch_spot_symbols_async(self) -> None:
        """Test async fetching of spot symbols."""
        respx.get(BINANCE_API_SPOT_URL).mock(return_value=Response(200, json=SPOT_RESPONSE))

        clear_cache(MarketType.SPOT)

        symbols = await fetch_symbols_async(MarketType.SPOT, use_cache=False)
        symbol_names = [s["symbol"] for s in symbols]
        assert "ETHUSDT" in symbol_names
        assert "BTCUSDT" in symbol_names


class TestValidateSymbol:
    """Tests for validate_symbol function."""

    @respx.mock
    def test_valid_symbol(self) -> None:
        """Test validation of valid symbol."""
        respx.get(BINANCE_API_FUTURES_USDT_URL).mock(
            return_value=Response(200, json=FUTURES_USDT_RESPONSE)
        )

        clear_cache(MarketType.FUTURES_USDT)

        result = validate_symbol("ETHUSDT", MarketType.FUTURES_USDT, use_cache=False)
        assert result == "ETHUSDT"

    @respx.mock
    def test_valid_symbol_lowercase(self) -> None:
        """Test validation uppercases the symbol."""
        respx.get(BINANCE_API_FUTURES_USDT_URL).mock(
            return_value=Response(200, json=FUTURES_USDT_RESPONSE)
        )

        clear_cache(MarketType.FUTURES_USDT)

        result = validate_symbol("ethusdt", MarketType.FUTURES_USDT, use_cache=False)
        assert result == "ETHUSDT"

    @respx.mock
    def test_invalid_symbol_raises(self) -> None:
        """Test that invalid symbol raises SymbolNotFoundError."""
        respx.get(BINANCE_API_FUTURES_USDT_URL).mock(
            return_value=Response(200, json=FUTURES_USDT_RESPONSE)
        )

        clear_cache(MarketType.FUTURES_USDT)

        with pytest.raises(SymbolNotFoundError) as exc_info:
            validate_symbol("INVALID", MarketType.FUTURES_USDT, use_cache=False)

        assert exc_info.value.symbol == "INVALID"
        assert exc_info.value.market_type == "futures-usdt"

    @respx.mock
    def test_invalid_symbol_provides_suggestions(self) -> None:
        """Test that invalid symbol error includes suggestions."""
        respx.get(BINANCE_API_FUTURES_USDT_URL).mock(
            return_value=Response(200, json=FUTURES_USDT_RESPONSE)
        )

        clear_cache(MarketType.FUTURES_USDT)

        with pytest.raises(SymbolNotFoundError) as exc_info:
            validate_symbol("ETHUSD", MarketType.FUTURES_USDT, use_cache=False)

        # Should suggest ETHUSDT
        assert len(exc_info.value.suggestions) > 0


class TestValidateSymbolAsync:
    """Tests for validate_symbol_async function."""

    @respx.mock
    async def test_valid_symbol_async(self) -> None:
        """Test async validation of valid symbol."""
        respx.get(BINANCE_API_FUTURES_USDT_URL).mock(
            return_value=Response(200, json=FUTURES_USDT_RESPONSE)
        )

        clear_cache(MarketType.FUTURES_USDT)

        result = await validate_symbol_async("ETHUSDT", MarketType.FUTURES_USDT, use_cache=False)
        assert result == "ETHUSDT"

    @respx.mock
    async def test_invalid_symbol_async_raises(self) -> None:
        """Test that async validation raises for invalid symbol."""
        respx.get(BINANCE_API_FUTURES_USDT_URL).mock(
            return_value=Response(200, json=FUTURES_USDT_RESPONSE)
        )

        clear_cache(MarketType.FUTURES_USDT)

        with pytest.raises(SymbolNotFoundError):
            await validate_symbol_async("INVALID", MarketType.FUTURES_USDT, use_cache=False)


class TestClearCache:
    """Tests for clear_cache function."""

    def test_clear_specific_cache(self, tmp_path: Path) -> None:
        """Test clearing cache for specific market type."""
        cache_file = tmp_path / "symbols_spot.json"
        cache_file.write_text("[]")

        with patch(
            "binance_ohlcv_collector.symbols._get_cache_path",
            return_value=cache_file,
        ):
            assert cache_file.exists()
            clear_cache(MarketType.SPOT)
            assert not cache_file.exists()

    def test_clear_nonexistent_cache_no_error(self) -> None:
        """Test clearing non-existent cache doesn't raise."""
        # Should not raise
        clear_cache(MarketType.SPOT)


class TestFilterSymbols:
    """Tests for filter_symbols function."""

    def test_filter_by_search(self) -> None:
        """Test filtering symbols by search term."""
        symbols = [
            {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT"},
            {"symbol": "ETHUSDT", "baseAsset": "ETH", "quoteAsset": "USDT"},
            {"symbol": "BTCBUSD", "baseAsset": "BTC", "quoteAsset": "BUSD"},
        ]

        # Search is case-insensitive
        filtered = filter_symbols(symbols, search="btc")
        assert len(filtered) == 2
        assert all("BTC" in s["symbol"] for s in filtered)

    def test_filter_by_base_asset(self) -> None:
        """Test filtering symbols by base asset."""
        symbols = [
            {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT"},
            {"symbol": "ETHUSDT", "baseAsset": "ETH", "quoteAsset": "USDT"},
            {"symbol": "BTCBUSD", "baseAsset": "BTC", "quoteAsset": "BUSD"},
        ]

        filtered = filter_symbols(symbols, base_asset="BTC")
        assert len(filtered) == 2
        assert all(s["baseAsset"] == "BTC" for s in filtered)

    def test_filter_by_quote_asset(self) -> None:
        """Test filtering symbols by quote asset."""
        symbols = [
            {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT"},
            {"symbol": "ETHUSDT", "baseAsset": "ETH", "quoteAsset": "USDT"},
            {"symbol": "BTCBUSD", "baseAsset": "BTC", "quoteAsset": "BUSD"},
        ]

        filtered = filter_symbols(symbols, quote_asset="USDT")
        assert len(filtered) == 2
        assert all(s["quoteAsset"] == "USDT" for s in filtered)

    def test_filter_combined(self) -> None:
        """Test combining multiple filters."""
        symbols = [
            {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT"},
            {"symbol": "ETHUSDT", "baseAsset": "ETH", "quoteAsset": "USDT"},
            {"symbol": "BTCBUSD", "baseAsset": "BTC", "quoteAsset": "BUSD"},
        ]

        filtered = filter_symbols(symbols, base_asset="BTC", quote_asset="USDT")
        assert len(filtered) == 1
        assert filtered[0]["symbol"] == "BTCUSDT"

    def test_filter_no_matches(self) -> None:
        """Test filtering with no matches returns empty list."""
        symbols = [
            {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT"},
        ]

        filtered = filter_symbols(symbols, search="NONEXISTENT")
        assert len(filtered) == 0

    def test_filter_case_insensitive(self) -> None:
        """Test that all filters are case-insensitive."""
        symbols = [
            {"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT"},
        ]

        # Search
        assert len(filter_symbols(symbols, search="btc")) == 1
        assert len(filter_symbols(symbols, search="BTC")) == 1
        assert len(filter_symbols(symbols, search="Btc")) == 1

        # Base asset
        assert len(filter_symbols(symbols, base_asset="btc")) == 1
        assert len(filter_symbols(symbols, base_asset="BTC")) == 1

        # Quote asset
        assert len(filter_symbols(symbols, quote_asset="usdt")) == 1
        assert len(filter_symbols(symbols, quote_asset="USDT")) == 1
