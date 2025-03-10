import os
import glob
import pandas as pd
from datetime import datetime
import re

from utils import logger

class DataProcessor:
    """
    Process and merge downloaded exchange data
    """
    
    def __init__(self, data_dir="exchange_data"):
        """
        Initialize the data processor
        
        Parameters:
        -----------
        data_dir : str
            Directory containing the exchange data (default: "exchange_data")
        """
        self.data_dir = data_dir
        
    def merge_by_product(self, exchange, product=None, output_dir=None):
        """
        Merge all contract files for a product into a single file
        
        Parameters:
        -----------
        exchange : str
            Name of the exchange (e.g., "cffex", "shfe", "dce", "czce")
        product : str or None
            Product code (e.g., "IF", "cu"). If None, process all products
        output_dir : str or None
            Directory to save merged files. If None, save in a 'merged' subdirectory
            of the exchange directory
            
        Returns:
        --------
        dict
            Dictionary with product codes as keys and DataFrames as values
        """
        exchange = exchange.lower()
        exchange_dir = os.path.join(self.data_dir, exchange)
        
        if not os.path.exists(exchange_dir):
            logger.error(f"Exchange directory not found: {exchange_dir}")
            return {}
        
        # Set output directory
        if output_dir is None:
            output_dir = os.path.join(exchange_dir, "merged")
            
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logger.info(f"Created output directory: {output_dir}")
        
        # Get all product directories
        product_dirs = []
        
        if product is not None:
            # For a specific product, find all matching directories
            product = product.lower()
            for dir_name in os.listdir(exchange_dir):
                if os.path.isdir(os.path.join(exchange_dir, dir_name)) and product in dir_name.lower():
                    product_dirs.append(dir_name)
        else:
            # For all products, get all directories except 'merged'
            for dir_name in os.listdir(exchange_dir):
                if os.path.isdir(os.path.join(exchange_dir, dir_name)) and dir_name != "merged":
                    product_dirs.append(dir_name)
        
        results = {}
        
        # Process each product directory
        for product_dir in product_dirs:
            try:
                # Extract product code from directory name (e.g., "daily_if" -> "if")
                product_code = product_dir.replace("daily_", "")
                
                # Get all CSV files for this product
                csv_files = glob.glob(os.path.join(exchange_dir, product_dir, "*.csv"))
                
                if not csv_files:
                    logger.warning(f"No CSV files found for {product_dir}")
                    continue
                
                logger.info(f"Merging {len(csv_files)} files for {product_dir}")
                
                all_data = []
                
                # Read and combine all CSV files
                for csv_file in csv_files:
                    try:
                        # Extract date from filename
                        file_date = os.path.splitext(os.path.basename(csv_file))[0]
                        
                        # Read CSV file
                        df = pd.read_csv(csv_file)
                        
                        # Ensure date column exists
                        if "date" not in df.columns:
                            df["date"] = file_date
                        
                        all_data.append(df)
                    except Exception as e:
                        logger.error(f"Error reading {csv_file}: {e}")
                
                if not all_data:
                    logger.warning(f"No valid data found for {product_dir}")
                    continue
                
                # Combine all data into a single DataFrame
                merged_df = pd.concat(all_data, ignore_index=True)
                
                # Save merged data
                output_file = os.path.join(output_dir, f"{product_code}.csv")
                merged_df.to_csv(output_file, index=False)
                
                logger.info(f"Merged data saved to {output_file}")
                
                results[product_code] = merged_df
                
            except Exception as e:
                logger.error(f"Error processing {product_dir}: {e}")
        
        return results
    
    def merge_by_product_group(self, exchange, product_group, output_dir=None):
        """
        Merge contracts of the same product group (e.g., index futures like IF, IH, IC)
        
        Parameters:
        -----------
        exchange : str
            Name of the exchange
        product_group : list
            List of product codes to merge (e.g., ["IF", "IH", "IC"])
        output_dir : str or None
            Directory to save merged files. If None, save in a 'merged' subdirectory
            
        Returns:
        --------
        pandas.DataFrame
            Merged DataFrame for the product group
        """
        exchange = exchange.lower()
        
        # Set output directory
        if output_dir is None:
            output_dir = os.path.join(self.data_dir, exchange, "merged")
            
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        all_data = []
        
        # Process each product in the group
        for product in product_group:
            product_data = self.merge_by_product(exchange, product, output_dir)
            
            if product.lower() in product_data:
                df = product_data[product.lower()]
                
                # Add product code if not present
                if "product_code" not in df.columns:
                    df["product_code"] = product
                
                all_data.append(df)
        
        if not all_data:
            logger.warning(f"No data found for product group {product_group}")
            return pd.DataFrame()
        
        # Combine all products into a single DataFrame
        merged_df = pd.concat(all_data, ignore_index=True)
        
        # Generate a group name from the product codes
        group_name = "_".join(product.lower() for product in product_group)
        
        # Save merged group data
        output_file = os.path.join(output_dir, f"group_{group_name}.csv")
        merged_df.to_csv(output_file, index=False)
        
        logger.info(f"Merged product group data saved to {output_file}")
        
        return merged_df
    
    def merge_all_exchanges(self, exchanges=None, output_dir=None):
        """
        Merge data from multiple exchanges
        
        Parameters:
        -----------
        exchanges : list or None
            List of exchange names. If None, process all available exchanges
        output_dir : str or None
            Directory to save merged files. If None, save in a 'merged' subdirectory
            
        Returns:
        --------
        dict
            Dictionary with exchange names as keys and dictionaries of merged data as values
        """
        if exchanges is None:
            # Find all exchange directories
            exchanges = []
            for dir_name in os.listdir(self.data_dir):
                if os.path.isdir(os.path.join(self.data_dir, dir_name)):
                    exchanges.append(dir_name)
        
        # Set output directory
        if output_dir is None:
            output_dir = os.path.join(self.data_dir, "merged")
            
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        results = {}
        
        # Process each exchange
        for exchange in exchanges:
            exchange_output_dir = os.path.join(output_dir, exchange)
            
            if not os.path.exists(exchange_output_dir):
                os.makedirs(exchange_output_dir)
            
            results[exchange] = self.merge_by_product(exchange, None, exchange_output_dir)
        
        return results

    def extract_specific_contract(self, exchange, product, contract_id, output_dir=None):
        """
        Extract data for a specific contract (e.g., 'IF2109') across all dates
        
        Parameters:
        -----------
        exchange : str
            Name of the exchange
        product : str
            Product code (e.g., "IF")
        contract_id : str
            Contract identifier (e.g., "2109" for September 2021)
        output_dir : str or None
            Directory to save output files. If None, save in a 'contracts' subdirectory
            
        Returns:
        --------
        pandas.DataFrame
            Data for the specific contract
        """
        exchange = exchange.lower()
        product = product.lower()
        
        # First merge all data for the product
        product_data = self.merge_by_product(exchange, product)
        
        if product not in product_data:
            logger.warning(f"No data found for product {product}")
            return pd.DataFrame()
        
        merged_df = product_data[product]
        
        # Set output directory
        if output_dir is None:
            output_dir = os.path.join(self.data_dir, exchange, "contracts")
            
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Extract data for the specific contract
        contract_pattern = f"{product}{contract_id}"
        
        # Look for contract_code column
        if "contract_code" in merged_df.columns:
            contract_df = merged_df[merged_df["contract_code"].str.contains(contract_pattern, case=False, na=False)]
        # Look for delivery_month column
        elif "delivery_month" in merged_df.columns:
            contract_df = merged_df[merged_df["delivery_month"].str.contains(contract_pattern, case=False, na=False)]
        # Try to find by matching any column
        else:
            contract_df = pd.DataFrame()
            for col in merged_df.columns:
                if merged_df[col].dtype == 'object':  # Only check string columns
                    matches = merged_df[merged_df[col].str.contains(contract_pattern, case=False, na=False)]
                    if not matches.empty:
                        contract_df = matches
                        break
        
        if contract_df.empty:
            logger.warning(f"No data found for contract {contract_pattern}")
            return pd.DataFrame()
        
        # Save contract data
        output_file = os.path.join(output_dir, f"{product}_{contract_id}.csv")
        contract_df.to_csv(output_file, index=False)
        
        logger.info(f"Contract data saved to {output_file}")
        
        return contract_df
