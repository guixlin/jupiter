import pandas as pd
import time
from datetime import datetime

from base_crawler import BaseCrawler
from utils import logger, get_date_range

class CoinbaseCrawler(BaseCrawler):
    """
    Crawler for Coinbase Pro exchange
    """
    
    def crawl(self, start_date=None, end_date=None, products=None):
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
        # Default date range if not provided
        start_date, end_date = get_date_range(start_date, end_date)
            
        # Default products if not provided
        if not products:
            products = ["BTC-USD", "ETH-USD"]
            
        logger.info(f"Crawling Coinbase data for {len(products)} products from {start_date} to {end_date}")
        
        # Convert dates to ISO format
        start_iso = datetime.strptime(start_date, '%Y-%m-%d').isoformat()
        end_iso = datetime.strptime(end_date, '%Y-%m-%d').isoformat()
        
        results = {}
        
        for product in products:
            try:
                # Coinbase API endpoint for historical data
                url = f"https://api.pro.coinbase.com/products/{product}/candles"
                
                params = {
                    "start": start_iso,
                    "end": end_iso,
                    "granularity": 86400  # Daily (86400 seconds = 1 day)
                }
                
                response = self.session.get(url, params=params)
                response.raise_for_status()
                
                # Parse the response
                data = response.json()
                
                # Create DataFrame
                df = pd.DataFrame(data, columns=['timestamp', 'low', 'high', 'open', 'close', 'volume'])
                
                # Convert timestamp to datetime
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
                df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')
                
                # Save data for each day
                for _, row in df.iterrows():
                    date = row['date']
                    daily_df = df[df['date'] == date]
                    
                    if not daily_df.empty:
                        self._save_data(
                            daily_df, 
                            exchange_name="coinbase", 
                            data_type=f"daily_{product.lower().replace('-', '_')}", 
                            date=date
                        )
                
                results[product] = df
                logger.info(f"Successfully crawled Coinbase data for {product}")
                
                # Respect rate limits
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error crawling Coinbase data for {product}: {e}")
        
        return results
