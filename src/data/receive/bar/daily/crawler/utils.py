import logging
import os
import pandas as pd
from datetime import datetime

# Configure logging
def setup_logging(log_file="exchange_crawler.log"):
    """
    Configure logging for the crawler
    
    Parameters:
    -----------
    log_file : str
        Name of the log file
    
    Returns:
    --------
    logging.Logger
        Configured logger
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger("exchange_crawler")

# Create logger
logger = setup_logging()

def save_data(data, exchange_name, data_type, date=None, output_dir="exchange_data"):
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
    output_dir : str
        Directory to save data
    """
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
        
    # Create exchange directory if it doesn't exist
    exchange_dir = os.path.join(output_dir, exchange_name)
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

def create_session():
    """
    Create a requests session with appropriate headers
    
    Returns:
    --------
    requests.Session
        Configured session
    """
    import requests
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    })
    
    return session

def get_date_range(start_date=None, end_date=None, days=7):
    """
    Get a date range for crawling
    
    Parameters:
    -----------
    start_date : str, optional
        Start date in YYYY-MM-DD format. If None, use today - days
    end_date : str, optional
        End date in YYYY-MM-DD format. If None, use today
    days : int
        Number of days to go back if start_date is None
        
    Returns:
    --------
    tuple
        (start_date, end_date) in YYYY-MM-DD format
    """
    from datetime import datetime, timedelta
    
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    if not start_date:
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
    
    return start_date, end_date

def generate_date_list(start_date, end_date, skip_weekends=True):
    """
    Generate a list of dates between start_date and end_date
    
    Parameters:
    -----------
    start_date : str
        Start date in YYYY-MM-DD format
    end_date : str
        End date in YYYY-MM-DD format
    skip_weekends : bool
        Whether to skip weekends
        
    Returns:
    --------
    list
        List of dates in YYYY-MM-DD format
    """
    from datetime import datetime, timedelta
    
    date_list = []
    current_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    
    while current_date <= end_date_obj:
        if not skip_weekends or current_date.weekday() < 5:  # 0-4 are Monday to Friday
            date_list.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    
    return date_list
