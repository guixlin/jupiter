from datetime import datetime, timedelta
import os

from utils import logger, get_date_range
from crypto_exchanges import BinanceCrawler, CoinbaseCrawler, KrakenCrawler
from cn_futures import CFFEXCrawler, SHFECrawler, DCECrawler, CZCECrawler

class ExchangeCrawler:
    """
    Main class to crawl data from various exchanges
    """
    
    def __init__(self, output_dir="exchange_data"):
        """
        Initialize the crawler with an output directory
        
        Parameters:
        -----------
        output_dir : str
            Directory to save crawled data (default: "exchange_data")
        """
        self.output_dir = output_dir
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Created output directory: {output_dir}")
            
        # Initialize crawlers
        self.binance_crawler = BinanceCrawler(output_dir)
        self.coinbase_crawler = CoinbaseCrawler(output_dir)
        self.kraken_crawler = KrakenCrawler(output_dir)
        self.cffex_crawler = CFFEXCrawler(output_dir)
        self.shfe_crawler = SHFECrawler(output_dir)
        self.dce_crawler = DCECrawler(output_dir)
        self.czce_crawler = CZCECrawler(output_dir)
    
    def crawl_binance(self, start_date=None, end_date=None, symbols=None):
        """
        Crawl daily market data from Binance
        
        Parameters:
        -----------
        start_date : str, optional
            Start date in YYYY-MM-DD format. If None, use today - 7 days
        end_date : str, optional
            End date in YYYY-MM-DD format. If None, use today
        symbols : list, optional
            List of trading pairs to crawl. If None, crawl BTC/USDT and ETH/USDT
        
        Returns:
        --------
        dict
            Dictionary containing DataFrames with the crawled data
        """
        return self.binance_crawler.crawl(start_date, end_date, symbols)
    
    def crawl_coinbase(self, start_date=None, end_date=None, products=None):
        """
        Crawl daily market data from Coinbase Pro
        
        Parameters:
        -----------
        start_date : str, optional
            Start date in YYYY-MM-DD format. If None, use today - 7 days
        end_date : str, optional
            End date in YYYY-MM-DD format. If None, use today
        products : list, optional
            List of trading pairs to crawl. If None, crawl BTC-USD and ETH-USD
        
        Returns:
        --------
        dict
            Dictionary containing DataFrames with the crawled data
        """
        return self.coinbase_crawler.crawl(start_date, end_date, products)
    
    def crawl_kraken(self, start_date=None, end_date=None, pairs=None):
        """
        Crawl daily market data from Kraken
        
        Parameters:
        -----------
        start_date : str, optional
            Start date in YYYY-MM-DD format. If None, use today - 7 days
        end_date : str, optional
            End date in YYYY-MM-DD format. If None, use today
        pairs : list, optional
            List of trading pairs to crawl. If None, crawl XBTUSD and ETHUSD
        
        Returns:
        --------
        dict
            Dictionary containing DataFrames with the crawled data
        """
        return self.kraken_crawler.crawl(start_date, end_date, pairs)
    
    def crawl_cffex(self, start_date=None, end_date=None, contracts=None):
        """
        Crawl daily market data from China Financial Futures Exchange (CFFEX)
        
        Parameters:
        -----------
        start_date : str, optional
            Start date in YYYY-MM-DD format. If None, use today - 7 days
        end_date : str, optional
            End date in YYYY-MM-DD format. If None, use today
        contracts : list, optional
            List of contracts to crawl. If None, fetch all available contracts
        
        Returns:
        --------
        dict
            Dictionary containing DataFrames with the crawled data
        """
        return self.cffex_crawler.crawl(start_date, end_date, contracts)
    
    def crawl_shfe(self, start_date=None, end_date=None, products=None):
        """
        Crawl daily market data from Shanghai Futures Exchange (SHFE)
        
        Parameters:
        -----------
        start_date : str, optional
            Start date in YYYY-MM-DD format. If None, use today - 7 days
        end_date : str, optional
            End date in YYYY-MM-DD format. If None, use today
        products : list, optional
            List of products to crawl (e.g., ["cu", "al", "zn"]). If None, fetch all available.
        
        Returns:
        --------
        dict
            Dictionary containing DataFrames with the crawled data
        """
        return self.shfe_crawler.crawl(start_date, end_date, products)
    
    def crawl_dce(self, start_date=None, end_date=None, products=None):
        """
        Crawl daily market data from Dalian Commodity Exchange (DCE)
        
        Parameters:
        -----------
        start_date : str, optional
            Start date in YYYY-MM-DD format. If None, use today - 7 days
        end_date : str, optional
            End date in YYYY-MM-DD format. If None, use today
        products : list, optional
            List of products to crawl (e.g., ["c", "cs", "a"]). If None, fetch all available.
        
        Returns:
        --------
        dict
            Dictionary containing DataFrames with the crawled data
        """
        return self.dce_crawler.crawl(start_date, end_date, products)
    
    def crawl_czce(self, start_date=None, end_date=None, products=None):
        """
        Crawl daily market data from Zhengzhou Commodity Exchange (CZCE)
        
        Parameters:
        -----------
        start_date : str, optional
            Start date in YYYY-MM-DD format. If None, use today - 7 days
        end_date : str, optional
            End date in YYYY-MM-DD format. If None, use today
        products : list, optional
            List of products to crawl (e.g., ["CF", "SR", "TA"]). If None, fetch all available.
        
        Returns:
        --------
        dict
            Dictionary containing DataFrames with the crawled data
        """
        return self.czce_crawler.crawl(start_date, end_date, products)
    
    def crawl_multiple_exchanges(self, exchanges=None, start_date=None, end_date=None):
        """
        Crawl data from multiple exchanges
        
        Parameters:
        -----------
        exchanges : list, optional
            List of exchange names to crawl. If None, crawl all supported exchanges
        start_date : str, optional
            Start date in YYYY-MM-DD format. If None, use today - 7 days
        end_date : str, optional
            End date in YYYY-MM-DD format. If None, use today
            
        Returns:
        --------
        dict
            Dictionary containing results for each exchange
        """
        # Default exchanges if not provided
        if not exchanges:
            exchanges = ["binance", "coinbase", "kraken", "cffex", "shfe", "dce", "czce"]
            
        # Default date range if not provided
        start_date, end_date = get_date_range(start_date, end_date)
            
        logger.info(f"Crawling multiple exchanges: {exchanges} from {start_date} to {end_date}")
        
        results = {}
        
        for exchange in exchanges:
            try:
                if exchange.lower() == "binance":
                    results["binance"] = self.crawl_binance(start_date, end_date)
                elif exchange.lower() == "coinbase":
                    results["coinbase"] = self.crawl_coinbase(start_date, end_date)
                elif exchange.lower() == "kraken":
                    results["kraken"] = self.crawl_kraken(start_date, end_date)
                elif exchange.lower() == "cffex":
                    results["cffex"] = self.crawl_cffex(start_date, end_date)
                elif exchange.lower() == "shfe":
                    results["shfe"] = self.crawl_shfe(start_date, end_date)
                elif exchange.lower() == "dce":
                    results["dce"] = self.crawl_dce(start_date, end_date) 
                elif exchange.lower() == "czce":
                    results["czce"] = self.crawl_czce(start_date, end_date)
                else:
                    logger.warning(f"Unsupported exchange: {exchange}")
            except Exception as e:
                logger.error(f"Error crawling {exchange}: {e}")
        
        return results


# Example usage
if __name__ == "__main__":
    # Create crawler instance
    crawler = ExchangeCrawler(output_dir="exchange_data")
    
    # Define date range
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')  # Last 30 days
    end_date = datetime.now().strftime('%Y-%m-%d')  # Today
    
    # Crypto Exchanges Examples
    
    # Crawl data from Binance
    binance_data = crawler.crawl_binance(
        start_date=start_date,
        end_date=end_date,
        symbols=["BTCUSDT", "ETHUSDT", "ADAUSDT", "DOTUSDT", "SOLUSDT"]
    )
    
    # Chinese Futures Exchanges Examples
    
    # Crawl data from CFFEX (China Financial Futures Exchange)
    cffex_data = crawler.crawl_cffex(
        start_date=start_date,
        end_date=end_date,
        contracts=["IF", "IC", "IH", "T", "TF"]  # Index futures, treasury futures
    )
    
    # Alternatively, crawl all supported exchanges at once
    all_data = crawler.crawl_multiple_exchanges(
        # Include both crypto and Chinese futures exchanges
        exchanges=["binance", "coinbase", "kraken", "cffex", "shfe", "dce", "czce"],
        start_date=start_date,
        end_date=end_date
    )
    
    # Or crawl only Chinese futures exchanges
    chinese_futures_data = crawler.crawl_multiple_exchanges(
        exchanges=["cffex", "shfe", "dce", "czce"],
        start_date=start_date,
        end_date=end_date
    )
    
    print("Data crawling completed. Check the 'exchange_data' directory for results.")
