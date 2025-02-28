import pandas as pd
import time
from datetime import datetime

from base_crawler import BaseCrawler
from utils import logger, get_date_range, generate_date_list

class CZCECrawler(BaseCrawler):
    """
    Crawler for Zhengzhou Commodity Exchange (CZCE)
    """
    
    def crawl(self, start_date=None, end_date=None, products=None):
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
        start_date, end_date = get_date_range(start_date, end_date)
            
        logger.info(f"Crawling CZCE data from {start_date} to {end_date}")
        
        # Generate list of dates between start and end (skip weekends)
        date_list = generate_date_list(start_date, end_date, skip_weekends=True)
        
        results = {}
        all_data = []
        
        for date in date_list:
            date_obj = datetime.strptime(date, '%Y-%m-%d')
            year = date_obj.year
            month = date_obj.month
            day = date_obj.day
            
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
