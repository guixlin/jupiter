import pandas as pd
import time
from datetime import datetime

from base_crawler import BaseCrawler
from utils import logger, get_date_range

class BinanceCrawler(BaseCrawler):
    """
    Crawler for Binance exchange
    """
    
    def crawl(self, start_date=None, end_date=None, symbols=None):
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
        # Default date range if not provided
        start_date, end_date = get_date_range(start_date, end_date)
            
        # Default symbols if not provided
        if not symbols:
            symbols = ["BTCUSDT", "ETHUSDT"]
            
        logger.info(f"Crawling Binance data for {len(symbols)} symbols from {start_date} to {end_date}")
        
        # Convert dates to milliseconds timestamp
        start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
        end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp() * 1000)
        
        results = {}
        
        for symbol in symbols:
            try:
                # Binance API endpoint for OHLCV data
                url = f"https://api.binance.com/api/v3/klines"
                
                params = {
                    "symbol": symbol,
                    "interval": "1d",  # Daily data
                    "startTime": start_ts,
                    "endTime": end_ts,
                    "limit": 1000  # Maximum allowed
                }
                
                response = self.session.get(url, params=params)
                response.raise_for_status()
                
                # Parse the response
                data = response.json()
                
                # Create DataFrame
                df = pd.DataFrame(data, columns=[
                    'timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'close_time', 'quote_asset_volume', 'number_of_trades',
                    'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
                ])
                
                # Convert timestamp to datetime
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')
                
                # Convert numeric columns
                numeric_cols = ['open', 'high', 'low', 'close', 'volume']
                for col in numeric_cols:
                    df[col] = pd.to_numeric(df[col])
                
                # Save data for each day
                for _, row in df.iterrows():
                    date = row['date']
                    daily_df = df[df['date'] == date]
                    
                    if not daily_df.empty:
                        self._save_data(
                            daily_df, 
                            exchange_name="binance", 
                            data_type=f"daily_{symbol.lower()}", 
                            date=date
                        )
                
                results[symbol] = df
                logger.info(f"Successfully crawled Binance data for {symbol}")
                
                # Respect rate limits
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error crawling Binance data for {symbol}: {e}")
        
        return results
