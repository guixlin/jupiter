import pandas as pd
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

from base_crawler import BaseCrawler
from utils import logger, get_date_range, generate_date_list

class DCECrawler(BaseCrawler):
    """
    Crawler for Dalian Commodity Exchange (DCE)
    """
    
    def crawl(self, start_date=None, end_date=None, products=None):
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
        start_date, end_date = get_date_range(start_date, end_date)
            
        logger.info(f"Crawling DCE data from {start_date} to {end_date}")
        
        # Generate list of dates between start and end (skip weekends)
        date_list = generate_date_list(start_date, end_date, skip_weekends=True)
        
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
