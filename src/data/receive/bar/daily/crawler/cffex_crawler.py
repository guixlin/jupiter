import pandas as pd
import time
import re
import zipfile
import io
from datetime import datetime

from base_crawler import BaseCrawler
from utils import logger, get_date_range, generate_date_list

class CFFEXCrawler(BaseCrawler):
    """
    Crawler for China Financial Futures Exchange (CFFEX)
    """
    
    def crawl(self, start_date=None, end_date=None, contracts=None):
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
        start_date, end_date = get_date_range(start_date, end_date)
            
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
