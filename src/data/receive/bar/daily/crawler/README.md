# Exchange Data Crawler

A comprehensive and modular crawler for collecting daily market data from cryptocurrency exchanges and Chinese futures exchanges.

## Features

- **Multi-exchange support**: 
  - Cryptocurrency exchanges (Binance, Coinbase, Kraken)
  - Chinese futures exchanges (CFFEX, SHFE, DCE, CZCE)
- **Customizable date ranges**: Fetch historical data for specific periods
- **Configurable trading pairs/products**: Target specific instruments to collect
- **Organized data storage**: CSV files organized by exchange, product, and date
- **Robust error handling**: Logs errors without crashing the application
- **Rate limit compliance**: Respects exchange API rate limits

## Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/exchange-crawler.git
cd exchange-crawler
```

2. Create a virtual environment:
```bash
# For Windows
python -m venv venv
venv\Scripts\activate

# For macOS/Linux
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Project Structure

```
exchange-crawler/
├── utils.py                  # Common utility functions
├── base_crawler.py           # Base crawler class with shared functionality
├── exchange_crawler.py       # Main crawler class
├── run_crawler.py            # Command-line script to run the crawler
├── requirements.txt          # Project dependencies
├── crypto_exchanges/         # Cryptocurrency exchange crawlers
│   ├── __init__.py
│   ├── binance_crawler.py
│   ├── coinbase_crawler.py
│   └── kraken_crawler.py
└── cn_futures/               # Chinese futures exchange crawlers
    ├── __init__.py
    ├── cffex_crawler.py
    ├── shfe_crawler.py
    ├── dce_crawler.py
    └── czce_crawler.py
```

## Usage

### Command Line Interface

The simplest way to use the crawler is through the provided command-line script:

```bash
python run_crawler.py --exchanges binance cffex --days 30
```

#### Available Options

```
usage: run_crawler.py [-h] [--start-date START_DATE] [--end-date END_DATE] [--days DAYS] [--output-dir OUTPUT_DIR]
                    [--exchanges EXCHANGES [EXCHANGES ...]] [--binance-symbols BINANCE_SYMBOLS [BINANCE_SYMBOLS ...]]
                    [--coinbase-products COINBASE_PRODUCTS [COINBASE_PRODUCTS ...]]
                    [--kraken-pairs KRAKEN_PAIRS [KRAKEN_PAIRS ...]]
                    [--cffex-contracts CFFEX_CONTRACTS [CFFEX_CONTRACTS ...]]
                    [--shfe-products SHFE_PRODUCTS [SHFE_PRODUCTS ...]]
                    [--dce-products DCE_PRODUCTS [DCE_PRODUCTS ...]]
                    [--czce-products CZCE_PRODUCTS [CZCE_PRODUCTS ...]]

Exchange Data Crawler

optional arguments:
  -h, --help            show this help message and exit
  --start-date START_DATE
                        Start date in YYYY-MM-DD format (default: 7 days ago)
  --end-date END_DATE   End date in YYYY-MM-DD format (default: today)
  --days DAYS           Number of days to go back if start-date is not specified (default: 7)
  --output-dir OUTPUT_DIR
                        Directory to save crawled data (default: exchange_data)
  --exchanges EXCHANGES [EXCHANGES ...]
                        List of exchanges to crawl (default: all supported exchanges)
  --binance-symbols BINANCE_SYMBOLS [BINANCE_SYMBOLS ...]
                        Symbols to crawl from Binance (default: BTCUSDT, ETHUSDT)
  --coinbase-products COINBASE_PRODUCTS [COINBASE_PRODUCTS ...]
                        Products to crawl from Coinbase (default: BTC-USD, ETH-USD)
  --kraken-pairs KRAKEN_PAIRS [KRAKEN_PAIRS ...]
                        Pairs to crawl from Kraken (default: XBTUSD, ETHUSD)
  --cffex-contracts CFFEX_CONTRACTS [CFFEX_CONTRACTS ...]
                        Contracts to crawl from CFFEX (default: IF, IC, IH, T, TF)
  --shfe-products SHFE_PRODUCTS [SHFE_PRODUCTS ...]
                        Products to crawl from SHFE (default: cu, al, zn, au, ag)
  --dce-products DCE_PRODUCTS [DCE_PRODUCTS ...]
                        Products to crawl from DCE (default: a, m, c, cs, i)
  --czce-products CZCE_PRODUCTS [CZCE_PRODUCTS ...]
                        Products to crawl from CZCE (default: CF, SR, TA, MA, FG)
```

### Python API

You can also use the crawler directly in your Python code:

```python
from exchange_crawler import ExchangeCrawler
from datetime import datetime, timedelta

# Create crawler instance
crawler = ExchangeCrawler(output_dir="exchange_data")

# Define date range
start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
end_date = datetime.now().strftime('%Y-%m-%d')

# Crawl specific exchanges
binance_data = crawler.crawl_binance(
    start_date=start_date,
    end_date=end_date,
    symbols=["BTCUSDT", "ETHUSDT"]
)

cffex_data = crawler.crawl_cffex(
    start_date=start_date,
    end_date=end_date,
    contracts=["IF", "IC", "IH"]
)

# Crawl all supported exchanges
all_data = crawler.crawl_multiple_exchanges(
    start_date=start_date,
    end_date=end_date
)

# Crawl only Chinese futures exchanges
chinese_futures_data = crawler.crawl_multiple_exchanges(
    exchanges=["cffex", "shfe", "dce", "czce"],
    start_date=start_date,
    end_date=end_date
)
```

## Data Format

All data is saved in CSV format with the following directory structure:

```
exchange_data/
├── binance/
│   ├── daily_btcusdt/
│   │   ├── 2023-01-01.csv
│   │   ├── 2023-01-02.csv
│   │   └── ...
│   └── ...
├── cffex/
│   ├── daily_if/
│   │   ├── 2023-01-01.csv
│   │   ├── 2023-01-02.csv
│   │   └── ...
│   └── ...
└── ...
```

## Extending the Crawler

To add support for a new exchange:

1. Create a new file in `crypto_exchanges/` or `cn_futures/` directory
2. Create a new crawler class that inherits from `BaseCrawler`
3. Implement the `crawl` method
4. Update the `__init__.py` file in the respective directory
5. Add the new crawler to `exchange_crawler.py`

## Dependencies

- requests: HTTP library for API calls
- pandas: Data processing and CSV handling
- beautifulsoup4: HTML parsing
- selenium: Web browser automation (for sites requiring JavaScript)
- lxml: XML/HTML processing

## License

This project is licensed under the MIT License - see the LICENSE file for details.
