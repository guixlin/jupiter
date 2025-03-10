#!/usr/bin/env python
"""
Script to run the Exchange Crawler
"""

import argparse
from datetime import datetime, timedelta
from exchange_crawler import ExchangeCrawler

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Exchange Data Crawler")
    
    # Date range
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date in YYYY-MM-DD format (default: 7 days ago)"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date in YYYY-MM-DD format (default: today)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to go back if start-date is not specified (default: 7)"
    )
    
    # Output directory
    parser.add_argument(
        "--output-dir",
        type=str,
        default="exchange_data",
        help="Directory to save crawled data (default: exchange_data)"
    )
    
    # Exchange selection
    parser.add_argument(
        "--exchanges",
        type=str,
        nargs="+",
        default=["binance", "coinbase", "kraken", "cffex", "shfe", "dce", "czce"],
        help="List of exchanges to crawl (default: all supported exchanges)"
    )
    
    # Crypto exchange options
    parser.add_argument(
        "--binance-symbols",
        type=str,
        nargs="+",
        default=["BTCUSDT", "ETHUSDT"],
        help="Symbols to crawl from Binance (default: BTCUSDT, ETHUSDT)"
    )
    parser.add_argument(
        "--coinbase-products",
        type=str,
        nargs="+",
        default=["BTC-USD", "ETH-USD"],
        help="Products to crawl from Coinbase (default: BTC-USD, ETH-USD)"
    )
    parser.add_argument(
        "--kraken-pairs",
        type=str,
        nargs="+",
        default=["XBTUSD", "ETHUSD"],
        help="Pairs to crawl from Kraken (default: XBTUSD, ETHUSD)"
    )
    
    # Chinese futures exchange options
    parser.add_argument(
        "--cffex-contracts",
        type=str,
        nargs="+",
        default=["IF", "IC", "IH", "T", "TF"],
        help="Contracts to crawl from CFFEX (default: IF, IC, IH, T, TF)"
    )
    parser.add_argument(
        "--shfe-products",
        type=str,
        nargs="+",
        default=["cu", "al", "zn", "au", "ag"],
        help="Products to crawl from SHFE (default: cu, al, zn, au, ag)"
    )
    parser.add_argument(
        "--dce-products",
        type=str,
        nargs="+",
        default=["a", "m", "c", "cs", "i"],
        help="Products to crawl from DCE (default: a, m, c, cs, i)"
    )
    parser.add_argument(
        "--czce-products",
        type=str,
        nargs="+",
        default=["CF", "SR", "TA", "MA", "FG"],
        help="Products to crawl from CZCE (default: CF, SR, TA, MA, FG)"
    )
    
    return parser.parse_args()

def main():
    """Main function"""
    args = parse_args()
    
    # Process dates
    end_date = args.end_date or datetime.now().strftime('%Y-%m-%d')
    
    if args.start_date:
        start_date = args.start_date
    else:
        start_date = (datetime.now() - timedelta(days=args.days)).strftime('%Y-%m-%d')
    
    print(f"Crawling data from {start_date} to {end_date}")
    print(f"Exchanges: {', '.join(args.exchanges)}")
    print(f"Output directory: {args.output_dir}")
    
    # Create crawler
    crawler = ExchangeCrawler(output_dir=args.output_dir)
    
    # Crawl data from each exchange
    for exchange in args.exchanges:
        try:
            print(f"Crawling {exchange}...")
            
            if exchange.lower() == "binance":
                crawler.crawl_binance(
                    start_date=start_date,
                    end_date=end_date,
                    symbols=args.binance_symbols
                )
            elif exchange.lower() == "coinbase":
                crawler.crawl_coinbase(
                    start_date=start_date,
                    end_date=end_date,
                    products=args.coinbase_products
                )
            elif exchange.lower() == "kraken":
                crawler.crawl_kraken(
                    start_date=start_date,
                    end_date=end_date,
                    pairs=args.kraken_pairs
                )
            elif exchange.lower() == "cffex":
                crawler.crawl_cffex(
                    start_date=start_date,
                    end_date=end_date,
                    contracts=args.cffex_contracts
                )
            elif exchange.lower() == "shfe":
                crawler.crawl_shfe(
                    start_date=start_date,
                    end_date=end_date,
                    products=args.shfe_products
                )
            elif exchange.lower() == "dce":
                crawler.crawl_dce(
                    start_date=start_date,
                    end_date=end_date,
                    products=args.dce_products
                )
            elif exchange.lower() == "czce":
                crawler.crawl_czce(
                    start_date=start_date,
                    end_date=end_date,
                    products=args.czce_products
                )
            else:
                print(f"Unsupported exchange: {exchange}")
                continue
            
            print(f"Successfully crawled {exchange}")
            
        except Exception as e:
            print(f"Error crawling {exchange}: {e}")
    
    print("\nData crawling completed. Check the output directory for results.")

if __name__ == "__main__":
    main()
