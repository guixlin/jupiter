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
├── data_processor.py         # Data processing utilities
├── merge_contracts.py        # Script to merge contract data
├── continuous_kline.py       # Continuous K-line generator
├── generate_continuous.py    # Script to generate continuous K-lines
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

### 1. Downloading Data

To download data from exchanges, use the `run_crawler.py` script:

```bash
python run_crawler.py --exchanges binance cffex --days 30
```

#### Available Options for run_crawler.py

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

### 2. Merging Contract Data

After downloading data, you can merge contract files into product-level files using the `merge_contracts.py` script:

```bash
# Merge all products from CFFEX
python merge_contracts.py --exchanges cffex

# Merge specific products from multiple exchanges
python merge_contracts.py --exchanges shfe dce --products cu a m

# Merge a group of related products together
python merge_contracts.py --exchanges cffex --product-groups "IF IH IC"

# Extract data for specific contracts
python merge_contracts.py --extract-contract --exchange cffex --product IF --contract-ids 2303 2306
```

#### Available Options for merge_contracts.py

```
usage: merge_contracts.py [-h] [--data-dir DATA_DIR] [--output-dir OUTPUT_DIR]
                          [--exchanges EXCHANGES [EXCHANGES ...]]
                          [--products PRODUCTS [PRODUCTS ...]]
                          [--product-groups PRODUCT_GROUPS [PRODUCT_GROUPS ...]]
                          [--extract-contract] [--exchange EXCHANGE]
                          [--product PRODUCT]
                          [--contract-ids CONTRACT_IDS [CONTRACT_IDS ...]]

Merge Exchange Contract Data

optional arguments:
  -h, --help            show this help message and exit
  --data-dir DATA_DIR   Directory containing exchange data (default: exchange_data)
  --output-dir OUTPUT_DIR
                        Directory to save merged files (default: data-dir/merged)
  --exchanges EXCHANGES [EXCHANGES ...]
                        List of exchanges to process (default: all available exchanges)
  --products PRODUCTS [PRODUCTS ...]
                        List of products to process (default: all available products)
  --product-groups PRODUCT_GROUPS [PRODUCT_GROUPS ...]
                        Groups of products to merge together (e.g., --product-groups IF IH IC)
  --extract-contract    Extract data for specific contracts
  --exchange EXCHANGE   Exchange for contract extraction
  --product PRODUCT     Product code for contract extraction
  --contract-ids CONTRACT_IDS [CONTRACT_IDS ...]
                        Contract identifiers to extract (e.g., 2109 2112)
```

### 3. Generating Continuous K-line Data

After merging contract data, you can generate continuous K-line data using the `generate_continuous.py` script:

```bash
# Generate continuous K-line data for IF using volume-based roll strategy with backward adjustment
python generate_continuous.py --exchange cffex --product IF --roll-strategy volume --adjust-method backward

# Generate continuous K-line data for CU using open interest-based roll strategy with ratio adjustment
python generate_continuous.py --exchange shfe --product cu --roll-strategy oi --adjust-method ratio

# Generate continuous K-line data for quarterly contracts only
python generate_continuous.py --exchange dce --product a --contract-months 3 6 9 12
```

#### Available Options for generate_continuous.py

```
usage: generate_continuous.py [-h] [--data-dir DATA_DIR] [--output-dir OUTPUT_DIR]
                            --exchange EXCHANGE --product PRODUCT
                            [--roll-strategy {volume,oi,time,fixed}]
                            [--adjust-method {backward,forward,ratio,difference,none}]
                            [--contract-months CONTRACT_MONTHS [CONTRACT_MONTHS ...]]
                            [--dominant-days DOMINANT_DAYS]
                            [--rollover-days ROLLOVER_DAYS]

Generate Continuous K-line Data

optional arguments:
  -h, --help            show this help message and exit
  --data-dir DATA_DIR   Directory containing exchange data (default: exchange_data)
  --output-dir OUTPUT_DIR
                        Directory to save continuous K-line files (default: data-dir/continuous)
  --exchange EXCHANGE   Exchange name (e.g., cffex, shfe)
  --product PRODUCT     Product code (e.g., IF, cu)
  --roll-strategy {volume,oi,time,fixed}
                        Contract roll strategy: volume, oi (open interest), time (days before expiry), fixed (date)
  --adjust-method {backward,forward,ratio,difference,none}
                        Price adjustment method
  --contract-months CONTRACT_MONTHS [CONTRACT_MONTHS ...]
                        Specific contract months to use (e.g., 3 6 9 12 for quarterly)
  --dominant-days DOMINANT_DAYS
                        Days before expiration to roll (for time-based strategy)
  --rollover-days ROLLOVER_DAYS
                        Days before month end to roll (for fixed strategy)
```

### Python API

You can use the crawler, data processor, and continuous K-line generator directly in your Python code:

#### 1. Downloading Data

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

#### 2. Merging Contract Data

```python
from data_processor import DataProcessor

# Create processor instance
processor = DataProcessor(data_dir="exchange_data")

# Merge all contracts of a specific product
if_data = processor.merge_by_product("cffex", "if")

# Merge a group of related products
index_futures = processor.merge_by_product_group("cffex", ["IF", "IH", "IC"])

# Extract data for a specific contract
march23_contract = processor.extract_specific_contract("cffex", "if", "2303")

# Merge all products from all exchanges
all_merged = processor.merge_all_exchanges()
```

#### 3. Generating Continuous K-line Data

```python
from continuous_kline import ContinuousKline

# Create continuous K-line generator
generator = ContinuousKline(data_dir="exchange_data")

# Generate continuous data with volume-based roll and backward adjustment
if_continuous = generator.generate_continuous(
    exchange="cffex",
    product="if",
    roll_strategy="volume",
    adjust_method="backward"
)

# Generate continuous data with open interest-based roll and ratio adjustment
cu_continuous = generator.generate_continuous(
    exchange="shfe",
    product="cu",
    roll_strategy="oi",
    adjust_method="ratio"
)

# Generate continuous data using only quarterly contracts
a_continuous = generator.generate_continuous(
    exchange="dce",
    product="a",
    roll_strategy="fixed",
    adjust_method="difference",
    contract_months=[3, 6, 9, 12]
)

# Generate continuous data with time-based roll
tf_continuous = generator.generate_continuous(
    exchange="cffex",
    product="tf",
    roll_strategy="time",
    adjust_method="backward",
    dominant_days=5  # Roll 5 days before expiration
)
```

## Data Format

### Raw Data

Raw downloaded data is saved in CSV format with the following directory structure:

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

### Merged Data

After running the merge operation, product-level data is organized:

```
exchange_data/
├── cffex/
│   ├── daily_if/           # Original daily files by contract
│   │   └── ...
│   ├── merged/             # Merged product files
│   │   ├── if.csv          # All IF contracts merged
│   │   ├── ih.csv          # All IH contracts merged
│   │   ├── group_if_ih_ic.csv  # Index futures group
│   │   └── ...
│   ├── contracts/          # Data for specific contracts
│   │   ├── if_2303.csv     # Data for March 2023 IF contract
│   │   ├── if_2306.csv     # Data for June 2023 IF contract
│   │   └── ...
│   └── continuous/         # Continuous K-line data
│       ├── if_continuous_volume_backward.csv    # Volume-based roll with backward adjustment
│       ├── if_continuous_oi_difference.csv      # Open interest-based roll with difference adjustment
│       └── ...
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
