# binance-ohlcv-collector

[![PyPI version](https://badge.fury.io/py/binance-ohlcv-collector.svg)](https://pypi.org/project/binance-ohlcv-collector/)
[![Python versions](https://img.shields.io/pypi/pyversions/binance-ohlcv-collector.svg)](https://pypi.org/project/binance-ohlcv-collector/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Download and collect historical OHLCV candlestick data from [Binance Vision](https://data.binance.vision/) for spot and futures markets.

## Features

- **Library-first design** - Use as a Python library or CLI
- **Smart symbol discovery** - Search, filter by base/quote asset, multiple output formats
- **Smart date handling** - Download by months, days, or date range
- **Auto market detection** - Symbols ending in `_PERP` auto-detected as COIN-M futures
- **Concurrent downloads** - Configurable parallel downloads with retry logic
- **Checksum verification** - Validates downloaded files against SHA256 checksums
- **Gap detection** - Identifies missing data in downloaded files
- **Multiple formats** - Output as Parquet (default) or CSV

## Installation

```bash
pip install binance-ohlcv-collector
```

Or with uv:

```bash
uv add binance-ohlcv-collector
```

## Quick Start

### As a Library

```python
from binance_ohlcv_collector import download, download_all, list_symbols

# Download 6 months of ETHUSDT 15m futures data (returns DataFrame)
df = download("ETHUSDT", timeframe="15m", months=6)

# Download with specific date range
df = download("ETHUSDT", start="2024-01-01", end="2024-06-30")

# Download last 30 days
df = download("ETHUSDT", days=30)

# Download multiple symbols (returns dict of DataFrames)
result = download(["ETHUSDT", "BTCUSDT"], months=6)
# result = {"ETHUSDT": df1, "BTCUSDT": df2}

# Save to disk instead of returning DataFrame
path = download("ETHUSDT", months=6, output_dir="./data")
# Returns: Path to saved parquet file

# Download spot data
df = download("ETHUSDT", months=6, market_type="spot")

# Download COIN-M futures (auto-detected from symbol)
df = download("ETHUSD_PERP", months=6)

# List available symbols (returns list of dicts with metadata)
symbols = list_symbols(market_type="futures-usdt")
# Returns: [{"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT"}, ...]

# Filter symbols
from binance_ohlcv_collector import filter_symbols
filtered = filter_symbols(symbols, search="BTC")
filtered = filter_symbols(symbols, base_asset="ETH", quote_asset="USDT")

# Download all available symbols
download_all(
    market_type="futures-usdt",
    timeframe="15m",
    months=12,
    output_dir="./data",
)
```

### As a CLI

```bash
# Download ETHUSDT futures data (default: 15m, futures-usdt)
binance-ohlcv-collector download ETHUSDT --months 6

# Download multiple symbols
binance-ohlcv-collector download ETHUSDT BTCUSDT --months 6

# Download with specific timeframe
binance-ohlcv-collector download ETHUSDT --months 6 --timeframe 1h

# Download by days
binance-ohlcv-collector download ETHUSDT --days 30

# Download with date range
binance-ohlcv-collector download ETHUSDT --start 2024-01-01 --end 2024-06-30

# Download spot data
binance-ohlcv-collector download ETHUSDT --months 6 --market-type spot

# Download to specific directory as CSV
binance-ohlcv-collector download ETHUSDT --months 6 --output ./data --format csv

# List available symbols (grid format, auto-adjusting to terminal width)
binance-ohlcv-collector list

# Search for specific symbols
binance-ohlcv-collector list --search BTC

# Filter by base or quote asset
binance-ohlcv-collector list --base ETH
binance-ohlcv-collector list --quote USDT --limit 20

# Show detailed table with metadata
binance-ohlcv-collector list --search PEPE --format table

# Export to JSON or plain text
binance-ohlcv-collector list --base BTC --quote USDT --format json
binance-ohlcv-collector list --quote USDT --format plain

# List spot or futures-coin markets
binance-ohlcv-collector list --market-type spot
binance-ohlcv-collector list --market-type futures-coin

# Download all symbols (two ways)
binance-ohlcv-collector download --all --months 6
binance-ohlcv-collector download-all --months 6 --output ./data

# Force re-download existing files
binance-ohlcv-collector download ETHUSDT --months 6 --force
```

## CLI Reference

### `download`

Download kline data for one or more symbols.

```
binance-ohlcv-collector download [OPTIONS] SYMBOLS...
```

**Arguments:**
- `SYMBOLS` - One or more symbols to download (e.g., `ETHUSDT BTCUSDT`)

**Options:**
| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--all` | `-a` | - | Download all available symbols |
| `--timeframe` | `-t` | `15m` | Kline interval (1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1mo) |
| `--months` | `-m` | - | Number of months to download |
| `--days` | `-d` | - | Number of days to download |
| `--start` | `-s` | - | Start date (YYYY-MM-DD) |
| `--end` | `-e` | - | End date (YYYY-MM-DD) |
| `--market-type` | `-M` | `futures-usdt` | Market type (spot, futures-usdt, futures-coin) |
| `--output` | `-o` | `data` | Output directory |
| `--format` | `-f` | `parquet` | Output format (parquet, csv) |
| `--force` | - | - | Force re-download existing files |
| `--no-keep-raw` | - | - | Delete raw ZIP files after processing |
| `--no-verify` | - | - | Skip checksum verification |
| `--no-validate` | - | - | Skip data validation (gap detection) |
| `--yes` | `-y` | - | Skip confirmation prompts |
| `--concurrency` | `-c` | `4` | Max concurrent downloads |
| `--retries` | `-r` | `3` | Number of retries per file |
| `--timeout` | - | `30` | Timeout per file in seconds |
| `--quiet` | `-q` | - | Minimal output |
| `--verbose` | `-v` | - | Verbose output |

### `list`

List and discover available trading symbols with powerful filtering options.

```
binance-ohlcv-collector list [OPTIONS]
```

**Options:**
| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--market-type` | `-M` | `futures-usdt` | Market type (spot, futures-usdt, futures-coin) |
| `--search` | `-s` | - | Search symbols containing text (case-insensitive) |
| `--base` | `-b` | - | Filter by base asset (e.g., BTC, ETH) |
| `--quote` | `-q` | - | Filter by quote asset (e.g., USDT, BUSD) |
| `--limit` | `-l` | - | Limit number of results displayed |
| `--format` | `-f` | `grid` | Output format (grid, table, json, plain) |
| `--no-cache` | - | - | Refresh symbols from API (bypass cache) |

**Output Formats:**
- `grid` - Compact multi-column display (default, auto-adjusts to terminal width)
- `table` - Detailed table with Symbol, Base Asset, and Quote Asset columns
- `json` - Machine-readable JSON array of symbol objects
- `plain` - Simple newline-separated list of symbol names

**Examples:**
```bash
# Quick search for Bitcoin pairs
binance-ohlcv-collector list --search BTC
# Output: BTCDOMUSDT  BTCSTUSDT  BTCUSDT  PUMPBTCUSDT

# Find all Ethereum pairs
binance-ohlcv-collector list --base ETH
# Output: ETHUSDT

# Browse first 20 USDT pairs
binance-ohlcv-collector list --quote USDT --limit 20

# Show detailed metadata
binance-ohlcv-collector list --search PEPE --format table

# Export for scripting
binance-ohlcv-collector list --base BTC --format json
```

### `download-all`

Download kline data for all available symbols.

```
binance-ohlcv-collector download-all [OPTIONS]
```

Same options as `download`, except `SYMBOLS` argument is not needed.

## Supported Markets

| Market Type | CLI Value | Example Symbols |
|-------------|-----------|-----------------|
| USDT-M Futures | `futures-usdt` | ETHUSDT, BTCUSDT |
| COIN-M Futures | `futures-coin` | ETHUSD_PERP, BTCUSD_PERP |
| Spot | `spot` | ETHUSDT, BTCUSDT |

## Supported Timeframes

`1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `3d`, `1w`, `1mo`

## Output Structure

When saving to disk, files are organized as:

```
data/
├── raw/                              # Downloaded ZIP files
│   └── futures-usdt/
│       └── ETHUSDT_15m/
│           ├── daily/
│           │   └── ETHUSDT-15m-2024-12-28.zip
│           └── monthly/
│               └── ETHUSDT-15m-2024-11.zip
└── processed/                        # Merged parquet/csv files
    └── futures-usdt/
        └── ETHUSDT-15m-2024-01-01-to-2024-12-28.parquet
```

## DataFrame Output

The returned DataFrame has the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | datetime64[ns, UTC] | Kline open time |
| `open` | float64 | Open price |
| `high` | float64 | High price |
| `low` | float64 | Low price |
| `close` | float64 | Close price |
| `volume` | float64 | Volume |

The DataFrame is indexed by row number, with `timestamp` as a column.

## License

MIT
