import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd

from utils import logger, save_data, create_session, get_date_range, generate_date_list

class BaseCrawler:
    """
    Base class for exchange data crawlers
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
        self.session = create_session()
        
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
                save_data(df, exchange_name, data_type, date, self.output_dir)
                
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
    
    def _save_data(self, data, exchange_name, data_type, date=None):
        """Wrapper for save_data utility function"""
        save_data(data, exchange_name, data_type, date, self.output_dir)
