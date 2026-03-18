# CryptoData Loader

This directory contains tools to load and analyze cryptocurrency data collected by the engine.

## Files

- `data_loader.py` - Main data loading class and utility functions
- `example_usage.py` - Example script showing how to use the data loader

## Features

- Load data from tar.gz archives created by the engine
- Combine data from multiple days
- Filter by date ranges and symbols
- Get summary statistics
- Automatic cleanup of temporary files

## Quick Start

### Load data for the last 7 days:
```python
from data_loader import load_last_days

df = load_last_days(days=7)
print(df.head())
```

### Load a specific file:
```python
from data_loader import load_file

df = load_file('../crypto_data/BTCUSD/2026-03-18.tar.gz')
```

### List available files:
```python
from data_loader import list_files

files = list_files()
print(files)
```

### Advanced usage with DataLoader class:
```python
from data_loader import DataLoader

loader = DataLoader()

# Load data for a specific date range
df = loader.load_date_range('2026-03-15', '2026-03-18', symbol='BTCUSD')

# Get summary statistics
summary = loader.get_data_summary(df)
print(summary)

# Always cleanup
loader.cleanup()
```

## Data Format

The loaded data contains these columns:
- `timestamp` - DateTime of the candle
- `open` - Opening price
- `high` - Highest price
- `low` - Lowest price
- `close` - Closing price
- `volume` - Trading volume

## Running Examples

```bash
# Run the example script
python example_usage.py

# Or run the main loader
python data_loader.py
```

## Notes

- Data files are stored as compressed tar.gz archives containing Feather files
- The loader automatically extracts and cleans up temporary files
- All timestamps are in UTC
- Prices are in the quote currency (e.g., USD for BTCUSD)