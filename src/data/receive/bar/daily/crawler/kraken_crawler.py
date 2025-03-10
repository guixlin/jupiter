import pandas as pd
import time
from datetime import datetime

from base_crawler import BaseCrawler
from utils import logger, get_date_range

class KrakenCrawler(BaseCrawler):
    """
    Crawler for Kraken exchange
    """
    
    def crawl(self, start_date=None, end_date=None, pairs=None):
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
        # Default date range if not provided
        start_date, end_date = get_date_range(start_date, end_date)
            
        # Default pairs if not provided
        if not pairs:
            pairs = ["XBTUSD", "ETHUSD"]
            
        logger.info(f"Crawling Kraken data for {len(pairs)} pairs from {start_date} to {end_date}")
        
        # Convert dates to unix timestamps
        start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())
        
        results = {}
        
        for pair in pairs:
            try:
                # Kraken API endpoint for OHLC data
                url = f"https://api.kraken.com/0/public/OHLC"
                
                params = {
                    "pair": pair,
                    "interval": 1440,  # Daily (1440 minutes = 24 hours)
                    "since": start_ts
                }
                
                response = self.session.get(url, params=params)
                response.raise_for_status()
                
                # Parse the response
                data = response.json()
                
                if "error" in data and data["error"]:
                    logger.error(f"Kraken API error for {pair}: {data['error']}")
                    continue
                
                # Extract result (first key in result is the pair name)
                result_key = list(data["result"].keys())[0]
                if result_key != "last":
                    ohlc_data = data["result"][result_key]
                    
                    # Create DataFrame
                    df = pd.DataFrame(ohlc_data, columns=[
                        'timestamp', 'open', 'high', 'low', 'close', 
                        'vwap', 'volume', 'count'
                    ])
                    
                    # Convert timestamp to datetime
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
                    df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')
                    
                    # Convert numeric columns
                    numeric_cols = ['open', 'high', 'low', 'close', 'vwap', 'volume']
                    for col in numeric_cols:
                        df[col] = pd.to_numeric(df[col])
                    
                    # Filter by date range
                    df = df[(df['timestamp'] >= pd.Timestamp(start_date)) & 
                            (df['timestamp'] <= pd.Timestamp(end_date))]
                    
                    # Save data for each day
                    for _, row in df.iterrows():
                        date = row['date']
                        daily_df = df[df['date'] == date]
                        
                        if not daily_df.empty:
                            self._save_data(
                                daily_df, 
                                exchange_name="kraken", 
                                data_type=f"daily_{pair.lower()}", 
                                date=date
                            )
                    
                    results[pair] = df
                    logger.info(f"Successfully crawled Kraken data for {pair}")
                
                # Respect rate limits
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Error crawling Kraken data for {pair}: {e}")
        
        return results
