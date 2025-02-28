import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import json
import os
import logging
import re
import zipfile
import io
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("exchange_crawler.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("exchange_crawler")

class ExchangeCrawler:
    """
    A class to crawl daily data from various exchange websites
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
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # Create output directory if it doesn't exist
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Created output directory: {output_dir}")
    
    def _save_data(self, data, exchange_name, data_type, date=None):
        """
        Save data to a CSV file
        
        Parameters:
        -----------
        data : pandas.DataFrame
            Data to save
        exchange_name : str
            Name of the exchange
        data_type : str
            Type of data (e.g., "market_data", "trades", etc.)
        date : str, optional
            Date in YYYY-MM-DD format. If None, use today's date
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
            
        # Create exchange directory if it doesn't exist
        exchange_dir = os.path.join(self.output_dir, exchange_name)
        if not os.path.exists(exchange_dir):
            os.makedirs(exchange_dir)
            logger.info(f"Created exchange directory: {exchange_dir}")
            
        # Create data type directory if it doesn't exist
        data_type_dir = os.path.join(exchange_dir, data_type)
        if not os.path.exists(data_type_dir):
            os.makedirs(data_type_dir)
            logger.info(f"Created data type directory: {data_type_dir}")
            
        # Save data to CSV
        filename = os.path.join(data_type_dir, f"{date}.csv")
        data.to_csv(filename, index=False)
        logger.info(f"Saved data to {filename}")
        
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
        # Default date range if not provided
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
            
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
        # Default date range if not provided
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
            
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
        # Default date range if not provided
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
            
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
    
    def crawl_with_selenium(self, url, exchange_name, data_type, css_selector, date=None, wait_time=10):
        """
        Crawl data from websites that require JavaScript rendering
        
        Parameters:
        -----------
        url : str
            URL to crawl
        exchange_name : str
            Name of the exchange
        data_type : str
            Type of data to crawl
        css_selector : str
            CSS selector to find the data table
        date : str, optional
            Date in YYYY-MM-DD format. If None, use today's date
        wait_time : int
            Time to wait for page to load in seconds
            
        Returns:
        --------
        pandas.DataFrame
            DataFrame with the crawled data
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
            
        logger.info(f"Crawling {exchange_name} with Selenium from {url}")
        
        try:
            # Configure Chrome options
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            
            # Initialize webdriver
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(url)
            
            # Wait for the element to be present
            element = WebDriverWait(driver, wait_time).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
            )
            
            # Get the page source
            page_source = driver.page_source
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(page_source, 'html.parser')
            table = soup.select_one(css_selector)
            
            if table:
                # Extract table data
                headers = []
                for th in table.select('thead th'):
                    headers.append(th.text.strip())
                
                rows = []
                for tr in table.select('tbody tr'):
                    row = []
                    for td in tr.select('td'):
                        row.append(td.text.strip())
                    rows.append(row)
                
                # Create DataFrame
                df = pd.DataFrame(rows, columns=headers)
                
                # Save data
                self._save_data(df, exchange_name, data_type, date)
                
                logger.info(f"Successfully crawled {exchange_name} data with Selenium")
                return df
            else:
                logger.error(f"Table not found with selector: {css_selector}")
                return None
                
        except Exception as e:
            logger.error(f"Error crawling with Selenium: {e}")
            return None
        finally:
            if 'driver' in locals():
                driver.quit()

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
        # Default date range if not provided
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
            
        logger.info(f"Crawling CFFEX data from {start_date} to {end_date}")
        
        # Convert dates to required format (YYYYMMDD)
        start_fmt = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y%m%d')
        end_fmt = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y%m%d')
        
        # Generate list of dates between start and end (CFFEX requires daily queries)
        date_list = []
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current_date <= end_date_obj:
            date_list.append(current_date.strftime('%Y%m%d'))
            current_date += timedelta(days=1)
        
        results = {}
        all_data = []
        
        # CFFEX data is organized by date
        for date in date_list:
            year = date[:4]
            month = date[4:6]
            day = date[6:8]
            
            try:
                # CFFEX historical data URL pattern
                url = f"http://www.cffex.com.cn/sj/historysj/{year}{month}/zip/{date}_1.zip"
                
                logger.info(f"Downloading CFFEX data for date: {date}")
                
                # Download the ZIP file
                response = self.session.get(url)
                
                # Check if the request was successful
                if response.status_code == 200:
                    try:
                        # Create a BytesIO object from the response content
                        zip_file = zipfile.ZipFile(io.BytesIO(response.content))
                        
                        # List files in the ZIP archive
                        file_list = zip_file.namelist()
                        
                        # Process each file in the ZIP
                        for file_name in file_list:
                            logger.info(f"Processing file: {file_name}")
                            
                            # Skip if not a CSV or a specific contract we want
                            if not file_name.endswith('.csv'):
                                continue
                                
                            if contracts and not any(contract in file_name for contract in contracts):
                                continue
                            
                            # Extract and read the CSV
                            with zip_file.open(file_name) as csv_file:
                                content = csv_file.read().decode('gbk')  # Chinese encoding
                                df = pd.read_csv(io.StringIO(content))
                                
                                # Add date column if not present
                                if '日期' not in df.columns and 'date' not in df.columns.str.lower():
                                    df['日期'] = f"{year}-{month}-{day}"
                                    
                                # Rename columns to English if they are in Chinese
                                if '合约代码' in df.columns:
                                    column_map = {
                                        '日期': 'date',
                                        '合约代码': 'contract_code',
                                        '昨结算': 'prev_settlement',
                                        '今开盘': 'open',
                                        '最高价': 'high', 
                                        '最低价': 'low',
                                        '今收盘': 'close',
                                        '今结算': 'settlement',
                                        '涨跌': 'change',
                                        '成交量': 'volume',
                                        '成交金额': 'amount',
                                        '持仓量': 'open_interest'
                                    }
                                    df = df.rename(columns=lambda x: column_map.get(x, x))
                                
                                all_data.append(df)
                                
                                # Extract contract type from filename
                                match = re.search(r'(IF|IC|IH|T[FSH]|[A-Z]{1,2}\d{3,4})', file_name)
                                if match:
                                    contract_type = match.group(1)
                                else:
                                    contract_type = "unknown"
                                    
                                # Save data
                                formatted_date = f"{year}-{month}-{day}"
                                self._save_data(
                                    df, 
                                    exchange_name="cffex", 
                                    data_type=f"daily_{contract_type.lower()}", 
                                    date=formatted_date
                                )
                                
                                logger.info(f"Successfully processed CFFEX data for {date}, contract: {contract_type}")
                    
                    except zipfile.BadZipFile:
                        logger.error(f"Bad ZIP file for date: {date}")
                else:
                    logger.warning(f"Failed to download CFFEX data for date: {date}, status code: {response.status_code}")
            
            except Exception as e:
                logger.error(f"Error crawling CFFEX data for date {date}: {e}")
            
            # Respect rate limits
            time.sleep(2)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            results["all"] = combined_df
            
        return results
    
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
        # Default date range if not provided
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
            
        logger.info(f"Crawling SHFE data from {start_date} to {end_date}")
        
        # Generate list of dates between start and end
        date_list = []
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current_date <= end_date_obj:
            # Skip weekends as exchange is closed
            if current_date.weekday() < 5:  # 0-4 are Monday to Friday
                date_list.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        results = {}
        all_data = []
        
        for date in date_list:
            year, month, day = date.split('-')
            
            try:
                # SHFE daily data URL
                url = "http://www.shfe.com.cn/data/dailydata/kx/kx{}.dat".format(date.replace('-', ''))
                
                logger.info(f"Downloading SHFE data for date: {date}")
                
                response = self.session.get(url)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        
                        if 'o_curinstrument' in data:
                            daily_data = data['o_curinstrument']
                            
                            if daily_data:
                                df = pd.DataFrame(daily_data)
                                
                                # Filter by products if specified
                                if products:
                                    df = df[df['PRODUCTID'].str.lower().isin([p.lower() for p in products])]
                                
                                if not df.empty:
                                    # Add date column
                                    df['DATE'] = date
                                    
                                    # Rename columns to standardized names
                                    column_map = {
                                        'PRODUCTID': 'product_id',
                                        'PRODUCTNAME': 'product_name',
                                        'DELIVERYMONTH': 'delivery_month',
                                        'PRESETTLEMENTPRICE': 'prev_settlement',
                                        'OPENPRICE': 'open',
                                        'HIGHESTPRICE': 'high',
                                        'LOWESTPRICE': 'low',
                                        'CLOSEPRICE': 'close',
                                        'SETTLEMENTPRICE': 'settlement',
                                        'ZD1_CHG': 'change',
                                        'VOLUME': 'volume',
                                        'TURNOVER': 'turnover',
                                        'OPENINTEREST': 'open_interest',
                                        'DATE': 'date'
                                    }
                                    
                                    df = df.rename(columns=lambda x: column_map.get(x, x))
                                    
                                    # Save data by product
                                    for product in df['product_id'].unique():
                                        product_df = df[df['product_id'] == product]
                                        
                                        self._save_data(
                                            product_df, 
                                            exchange_name="shfe", 
                                            data_type=f"daily_{product.lower()}", 
                                            date=date
                                        )
                                    
                                    all_data.append(df)
                                    logger.info(f"Successfully processed SHFE data for {date}")
                                else:
                                    logger.warning(f"No matching products found for date: {date}")
                            else:
                                logger.warning(f"No data available for date: {date}")
                        else:
                            logger.warning(f"Unexpected data format for date: {date}")
                    except Exception as e:
                        logger.error(f"Error processing SHFE data for date {date}: {e}")
                else:
                    logger.warning(f"Failed to download SHFE data for date: {date}, status code: {response.status_code}")
            
            except Exception as e:
                logger.error(f"Error crawling SHFE data for date {date}: {e}")
            
            # Respect rate limits
            time.sleep(2)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            results["all"] = combined_df
            
        return results
        
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
        # Default date range if not provided
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
            
        logger.info(f"Crawling DCE data from {start_date} to {end_date}")
        
        # Generate list of dates between start and end
        date_list = []
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current_date <= end_date_obj:
            # Skip weekends as exchange is closed
            if current_date.weekday() < 5:  # 0-4 are Monday to Friday
                date_list.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        results = {}
        all_data = []
        
        # DCE requires a session with cookies and potentially form submission
        # We'll use Selenium for this as it's more reliable for DCE's website
        
        try:
            # Configure Chrome options
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--lang=zh-CN")  # Set language to Chinese
            
            # Initialize webdriver
            driver = webdriver.Chrome(options=chrome_options)
            
            for date in date_list:
                year, month, day = date.split('-')
                
                try:
                    # Format date for DCE's form
                    dce_date = f"{year}-{month}-{day}"
                    
                    # DCE market data page
                    url = "http://www.dce.com.cn/publicweb/quotesdata/dayQuotesCh.html"
                    driver.get(url)
                    
                    # Wait for page to load
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.ID, "marketData"))
                    )
                    
                    # Fill the search form
                    # Select the trading date
                    date_input = driver.find_element(By.ID, "tradingDate")
                    driver.execute_script(f"arguments[0].value = '{dce_date}'", date_input)
                    
                    # Submit the form
                    search_button = driver.find_element(By.CSS_SELECTOR, ".search_btn")
                    search_button.click()
                    
                    # Wait for results to load
                    time.sleep(3)
                    
                    # Get the page source
                    page_source = driver.page_source
                    
                    # Parse with BeautifulSoup
                    soup = BeautifulSoup(page_source, 'html.parser')
                    
                    # Find the data table
                    table = soup.find('table', {'id': 'printData'})
                    
                    if table:
                        # Extract table data
                        headers = []
                        for th in table.select('tr.head_text th'):
                            headers.append(th.text.strip())
                        
                        rows = []
                        for tr in table.select('tr:not(.head_text)'):
                            row = []
                            for td in tr.select('td'):
                                row.append(td.text.strip())
                            if row:  # Skip empty rows
                                rows.append(row)
                        
                        # Create DataFrame
                        if headers and rows:
                            df = pd.DataFrame(rows, columns=headers)
                            
                            # Add date column
                            df['日期'] = date
                            
                            # Rename columns to standardized names
                            column_map = {
                                '商品名称': 'product_name',
                                '交割月份': 'delivery_month',
                                '开盘价': 'open',
                                '最高价': 'high',
                                '最低价': 'low',
                                '收盘价': 'close',
                                '前结算价': 'prev_settlement',
                                '结算价': 'settlement',
                                '涨跌': 'change',
                                '成交量': 'volume',
                                '持仓量': 'open_interest',
                                '成交额': 'turnover',
                                '日期': 'date'
                            }
                            
                            df = df.rename(columns=lambda x: column_map.get(x, x))
                            
                            # Extract product code
                            df['product_code'] = df['delivery_month'].str.extract(r'([A-Za-z]+)')
                            
                            # Filter by products if specified
                            if products:
                                df = df[df['product_code'].str.lower().isin([p.lower() for p in products])]
                            
                            if not df.empty:
                                # Save data by product
                                for product in df['product_code'].unique():
                                    product_df = df[df['product_code'] == product]
                                    
                                    self._save_data(
                                        product_df, 
                                        exchange_name="dce", 
                                        data_type=f"daily_{product.lower()}", 
                                        date=date
                                    )
                                
                                all_data.append(df)
                                logger.info(f"Successfully processed DCE data for {date}")
                            else:
                                logger.warning(f"No matching products found for date: {date}")
                        else:
                            logger.warning(f"Empty table data for date: {date}")
                    else:
                        logger.warning(f"Data table not found for date: {date}")
                        
                except Exception as e:
                    logger.error(f"Error crawling DCE data for date {date}: {e}")
                
                # Respect rate limits
                time.sleep(3)
                
        except Exception as e:
            logger.error(f"Error initializing webdriver for DCE: {e}")
        finally:
            if 'driver' in locals():
                driver.quit()
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            results["all"] = combined_df
            
        return results
        
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
        # Default date range if not provided
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
            
        logger.info(f"Crawling CZCE data from {start_date} to {end_date}")
        
        # Generate list of dates between start and end
        date_list = []
        current_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        while current_date <= end_date_obj:
            # Skip weekends as exchange is closed
            if current_date.weekday() < 5:  # 0-4 are Monday to Friday
                date_list.append(current_date)
            current_date += timedelta(days=1)
        
        results = {}
        all_data = []
        
        for date_obj in date_list:
            year = date_obj.year
            month = date_obj.month
            day = date_obj.day
            date = date_obj.strftime('%Y-%m-%d')
            
            try:
                # CZCE daily data URL (format differs by year)
                if year >= 2015:
                    url = f"http://www.czce.com.cn/cn/DFSStaticFiles/Future/{year}/{date_obj.strftime('%Y%m%d')}/FutureDataDaily.htm"
                else:
                    url = f"http://www.czce.com.cn/cn/exchange/{year}/datadaily/{date_obj.strftime('%Y%m%d')}.htm"
                
                logger.info(f"Downloading CZCE data for date: {date}")
                
                response = self.session.get(url)
                response.encoding = 'utf-8'  # Ensure proper encoding
                
                if response.status_code == 200:
                    try:
                        # Parse HTML tables
                        tables = pd.read_html(response.text, header=0)
                        
                        if tables:
                            # CZCE format varies by year, find the main data table
                            main_df = None
                            for df in tables:
                                if len(df.columns) > 5 and '品种月份' in df.columns or '合约代码' in df.columns:
                                    main_df = df
                                    break
                            
                            if main_df is not None:
                                # Standardize column names
                                if '品种月份' in main_df.columns:
                                    main_df = main_df.rename(columns={'品种月份': '合约代码'})
                                
                                # Add date column
                                main_df['日期'] = date
                                
                                # Standardize column names
                                column_map = {
                                    '合约代码': 'contract_code',
                                    '昨结算': 'prev_settlement',
                                    '今开盘': 'open',
                                    '最高价': 'high',
                                    '最低价': 'low',
                                    '今收盘': 'close',
                                    '今结算': 'settlement',
                                    '涨跌': 'change',
                                    '成交量': 'volume',
                                    '成交额': 'turnover',
                                    '持仓量': 'open_interest',
                                    '日期': 'date'
                                }
                                
                                main_df = main_df.rename(columns=lambda x: column_map.get(x, x))
                                
                                # Extract product code
                                main_df['product_code'] = main_df['contract_code'].str.extract(r'([A-Za-z]+)')
                                
                                # Filter by products if specified
                                if products:
                                    main_df = main_df[main_df['product_code'].str.lower().isin([p.lower() for p in products])]
                                
                                if not main_df.empty:
                                    # Save data by product
                                    for product in main_df['product_code'].unique():
                                        product_df = main_df[main_df['product_code'] == product]
                                        
                                        self._save_data(
                                            product_df, 
                                            exchange_name="czce", 
                                            data_type=f"daily_{product.lower()}", 
                                            date=date
                                        )
                                    
                                    all_data.append(main_df)
                                    logger.info(f"Successfully processed CZCE data for {date}")
                                else:
                                    logger.warning(f"No matching products found for date: {date}")
                            else:
                                logger.warning(f"Data table not found for date: {date}")
                        else:
                            logger.warning(f"No tables found for date: {date}")
                    except Exception as e:
                        logger.error(f"Error processing CZCE data for date {date}: {e}")
                else:
                    logger.warning(f"Failed to download CZCE data for date: {date}, status code: {response.status_code}")
            
            except Exception as e:
                logger.error(f"Error crawling CZCE data for date {date}: {e}")
            
            # Respect rate limits
            time.sleep(2)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            results["all"] = combined_df
            
        return results

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
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not end_date:
            end_date = datetime.now().strftime('%Y-%m-%d')
            
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
    
    # Crawl data from Coinbase
    coinbase_data = crawler.crawl_coinbase(
        start_date=start_date,
        end_date=end_date,
        products=["BTC-USD", "ETH-USD", "ADA-USD"]
    )
    
    # Crawl data from Kraken
    kraken_data = crawler.crawl_kraken(
        start_date=start_date,
        end_date=end_date,
        pairs=["XBTUSD", "ETHUSD"]
    )
    
    # Chinese Futures Exchanges Examples
    
    # Crawl data from CFFEX (China Financial Futures Exchange)
    cffex_data = crawler.crawl_cffex(
        start_date=start_date,
        end_date=end_date,
        contracts=["IF", "IC", "IH", "T", "TF"]  # Index futures, treasury futures
    )
    
    # Crawl data from SHFE (Shanghai Futures Exchange)
    shfe_data = crawler.crawl_shfe(
        start_date=start_date,
        end_date=end_date,
        products=["cu", "al", "zn", "pb", "ni", "sn", "au", "ag", "rb", "hc"]  # Metals, rebar
    )
    
    # Crawl data from DCE (Dalian Commodity Exchange)
    dce_data = crawler.crawl_dce(
        start_date=start_date,
        end_date=end_date,
        products=["a", "m", "y", "p", "c", "cs", "jd", "l", "v", "pp", "j", "jm", "i"]  # Agricultural, chemical, energy
    )
    
    # Crawl data from CZCE (Zhengzhou Commodity Exchange)
    czce_data = crawler.crawl_czce(
        start_date=start_date,
        end_date=end_date,
        products=["CF", "SR", "OI", "RM", "TA", "MA", "FG", "ZC", "AP"]  # Cotton, sugar, glass, apple, etc.
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
