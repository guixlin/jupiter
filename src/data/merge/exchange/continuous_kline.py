import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import glob
import re

from utils import logger
from data_processor import DataProcessor

class ContinuousKline:
    """
    Generate continuous K-line data from futures contracts.
    
    This class handles the creation of continuous price series by stitching together
    data from sequential futures contracts, using various roll strategies and
    price adjustment methods.
    """
    
    def __init__(self, data_dir="exchange_data"):
        """
        Initialize the continuous K-line generator.
        
        Parameters:
        -----------
        data_dir : str
            Directory containing the exchange data (default: "exchange_data")
        """
        self.data_dir = data_dir
        self.processor = DataProcessor(data_dir)
        
    def _load_product_data(self, exchange, product):
        """
        Load all data for a product, merging if necessary.
        
        Parameters:
        -----------
        exchange : str
            Exchange name
        product : str
            Product code
            
        Returns:
        --------
        pandas.DataFrame
            Merged product data
        """
        exchange = exchange.lower()
        product = product.lower()
        
        # Check if merged data already exists
        merged_dir = os.path.join(self.data_dir, exchange, "merged")
        merged_file = os.path.join(merged_dir, f"{product}.csv")
        
        if os.path.exists(merged_file):
            logger.info(f"Loading existing merged data from {merged_file}")
            return pd.read_csv(merged_file)
        else:
            # Merge data first
            logger.info(f"Merged data not found, creating for {exchange} {product}")
            product_data = self.processor.merge_by_product(exchange, product)
            
            if product in product_data:
                return product_data[product]
            else:
                logger.error(f"No data found for {exchange} {product}")
                return pd.DataFrame()
    
    def _identify_contracts(self, df, contract_col="contract_code"):
        """
        Identify unique contracts and their respective time periods.
        
        Parameters:
        -----------
        df : pandas.DataFrame
            Product data
        contract_col : str
            Column name containing contract identifiers
            
        Returns:
        --------
        list
            List of unique contracts sorted by expiration date
        """
        # Find the contract column if the specified one doesn't exist
        if contract_col not in df.columns:
            for col in ["contract_code", "delivery_month", "symbol", "instrument_id"]:
                if col in df.columns:
                    contract_col = col
                    break
            else:
                # If we can't find a contract column, try to extract from a string column
                for col in df.columns:
                    if df[col].dtype == "object":
                        if any(isinstance(val, str) and re.search(r"\d{2,4}", val) for val in df[col].dropna()):
                            contract_col = col
                            break
                else:
                    logger.error("Cannot identify contract column")
                    return []
        
        # Extract unique contracts
        contracts = df[contract_col].unique().tolist()
        
        # Sort contracts by expiration date (extracted from contract code)
        # This pattern assumes formats like "IF2109", "CU2203", etc.
        def extract_date(contract):
            match = re.search(r"([A-Za-z]+)(\d{2,4})(\d{2})?", str(contract))
            if match:
                # Handle different date formats (with 2 or 4 digit years)
                product, year, month = match.groups()
                if month is None:
                    # Handle format like "2109" (YYMM)
                    year, month = year[:2], year[2:]
                
                # Handle 2-digit years
                if len(year) == 2:
                    year = "20" + year  # Assume 20xx for simplicity
                
                return f"{year}{month}"
            return "999999"  # Default for unmatched
        
        # Sort by expiration date
        contracts.sort(key=extract_date)
        
        return contracts
    
    def generate_continuous(self, exchange, product, roll_strategy="volume", 
                           adjust_method="backward", contract_months=None,
                           dominant_days=0, rollover_days=0, output_dir=None):
        """
        Generate continuous K-line data.
        
        Parameters:
        -----------
        exchange : str
            Exchange name
        product : str
            Product code
        roll_strategy : str
            Strategy for rolling contracts:
            - "volume": Roll when next contract has higher volume
            - "oi": Roll when next contract has higher open interest
            - "time": Roll fixed days before expiration
            - "fixed": Roll on fixed dates for contract months
        adjust_method : str
            Method for price adjustment:
            - "backward": Adjust historical prices
            - "forward": Adjust new contract prices
            - "ratio": Use price ratio for adjustment
            - "difference": Use price difference for adjustment
            - "none": No adjustment (raw price)
        contract_months : list or None
            List of contract months to use (e.g., [3, 6, 9, 12] for quarterly)
            If None, use all available contracts
        dominant_days : int
            For time-based roll: days before expiration to roll
        rollover_days : int
            For fixed roll: days before month end to roll
        output_dir : str or None
            Directory to save the continuous data
            
        Returns:
        --------
        pandas.DataFrame
            Continuous K-line data
        """
        exchange = exchange.lower()
        product = product.lower()
        
        # Load product data
        df = self._load_product_data(exchange, product)
        
        if df.empty:
            logger.error(f"No data found for {exchange} {product}")
            return df
        
        # Ensure date column exists and is datetime
        if "date" not in df.columns:
            logger.error("Date column not found")
            return df
        
        df["date"] = pd.to_datetime(df["date"])
        
        # Identify the contract column
        contract_col = None
        for col in ["contract_code", "delivery_month", "symbol", "instrument_id"]:
            if col in df.columns:
                contract_col = col
                break
        
        if contract_col is None:
            logger.error("Contract column not found")
            return df
        
        # Identify OHLCV columns
        columns = {
            "open": None,
            "high": None,
            "low": None,
            "close": None,
            "volume": None,
            "open_interest": None
        }
        
        for col_type, col_name in columns.items():
            for possible_name in [col_type, f"{col_type}_price", f"{col_type}_px"]:
                if possible_name in df.columns:
                    columns[col_type] = possible_name
                    break
            
            # Special cases for volume and open interest
            if col_type == "volume" and col_name is None:
                for vol_name in ["volume", "vol", "turnover", "成交量"]:
                    if vol_name in df.columns:
                        columns["volume"] = vol_name
                        break
                        
            if col_type == "open_interest" and col_name is None:
                for oi_name in ["open_interest", "oi", "position", "持仓量"]:
                    if oi_name in df.columns:
                        columns["open_interest"] = oi_name
                        break
        
        # Check if we have all required columns
        required_cols = ["open", "high", "low", "close"]
        missing_cols = [col for col in required_cols if columns[col] is None]
        
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return df
        
        # Identify contracts
        contracts = self._identify_contracts(df, contract_col)
        
        if not contracts:
            logger.error("No valid contracts found")
            return df
        
        # Filter by contract months if specified
        if contract_months:
            def extract_month(contract):
                match = re.search(r"([A-Za-z]+)(\d{2,4})(\d{2})?", str(contract))
                if match:
                    product, year, month = match.groups()
                    if month is None:
                        # Handle format like "2109" (YYMM)
                        year, month = year[:2], year[2:]
                    
                    return int(month)
                return 0
            
            contracts = [c for c in contracts if extract_month(c) in contract_months]
        
        # Generate continuous data
        continuous_df = pd.DataFrame()
        
        # Track adjustment factor
        adjustment_factor = 0  # For difference adjustment
        adjustment_ratio = 1.0  # For ratio adjustment
        
        for i, contract in enumerate(contracts):
            # Get data for this contract
            contract_df = df[df[contract_col] == contract].copy()
            
            if contract_df.empty:
                continue
            
            # Sort by date
            contract_df = contract_df.sort_values("date")
            
            # Determine next contract (if available)
            next_contract = contracts[i+1] if i < len(contracts) - 1 else None
            
            # Calculate rollover date based on strategy
            rollover_date = None
            
            if roll_strategy == "volume" and next_contract and columns["volume"]:
                # Get data for next contract
                next_df = df[df[contract_col] == next_contract].copy()
                
                if not next_df.empty and not contract_df.empty:
                    # Find date when next contract's volume exceeds current contract
                    merged = pd.merge(
                        contract_df[["date", columns["volume"]]].rename(columns={columns["volume"]: "curr_vol"}),
                        next_df[["date", columns["volume"]]].rename(columns={columns["volume"]: "next_vol"}),
                        on="date", how="inner"
                    )
                    
                    if not merged.empty:
                        # Find first date where next contract has higher volume
                        higher_vol = merged[merged["next_vol"] > merged["curr_vol"]]
                        
                        if not higher_vol.empty:
                            rollover_date = higher_vol["date"].min()
            
            elif roll_strategy == "oi" and next_contract and columns["open_interest"]:
                # Get data for next contract
                next_df = df[df[contract_col] == next_contract].copy()
                
                if not next_df.empty and not contract_df.empty:
                    # Find date when next contract's OI exceeds current contract
                    merged = pd.merge(
                        contract_df[["date", columns["open_interest"]]].rename(columns={columns["open_interest"]: "curr_oi"}),
                        next_df[["date", columns["open_interest"]]].rename(columns={columns["open_interest"]: "next_oi"}),
                        on="date", how="inner"
                    )
                    
                    if not merged.empty:
                        # Find first date where next contract has higher OI
                        higher_oi = merged[merged["next_oi"] > merged["curr_oi"]]
                        
                        if not higher_oi.empty:
                            rollover_date = higher_oi["date"].min()
            
            elif roll_strategy == "time":
                # Extract expiration date from contract
                expiry = self._extract_expiry_date(contract)
                
                if expiry:
                    # Roll dominant_days before expiration
                    rollover_date = expiry - timedelta(days=dominant_days)
            
            elif roll_strategy == "fixed" and next_contract:
                # Extract expiration month from contract
                match = re.search(r"([A-Za-z]+)(\d{2,4})(\d{2})?", str(contract))
                
                if match:
                    product, year, month = match.groups()
                    
                    if month is None:
                        # Handle format like "2109" (YYMM)
                        year, month = year[:2], year[2:]
                    
                    # Handle 2-digit years
                    if len(year) == 2:
                        year = "20" + year  # Assume 20xx for simplicity
                    
                    # Create date for end of month
                    month_end = self._get_month_end(int(year), int(month))
                    
                    # Roll rollover_days before month end
                    rollover_date = month_end - timedelta(days=rollover_days)
            
            # Apply price adjustments
            if i > 0 and (adjust_method in ["backward", "forward", "ratio", "difference"]):
                # Find the adjustment point (last day of previous contract or first day of current)
                if continuous_df.empty or contract_df.empty:
                    continue
                
                prev_last_date = continuous_df["date"].max()
                curr_first_date = contract_df["date"].min()
                
                # Find overlapping date (if any)
                overlap_date = None
                overlap_dates = continuous_df[continuous_df["date"].isin(contract_df["date"])]["date"]
                
                if not overlap_dates.empty:
                    overlap_date = overlap_dates.min()
                
                if overlap_date is not None:
                    # Get prices on the overlap date
                    prev_price = continuous_df.loc[continuous_df["date"] == overlap_date, "adj_close"].iloc[0]
                    curr_price = contract_df.loc[contract_df["date"] == overlap_date, columns["close"]].iloc[0]
                    
                    if adjust_method == "difference":
                        # Calculate price difference
                        adjustment_factor = prev_price - curr_price
                    elif adjust_method == "ratio":
                        # Calculate price ratio
                        if curr_price != 0:
                            adjustment_ratio = prev_price / curr_price
                
                # Apply the adjustment to the contract data
                if adjust_method == "difference":
                    for col in ["open", "high", "low", "close"]:
                        if columns[col]:
                            contract_df[f"adj_{col}"] = contract_df[columns[col]] + adjustment_factor
                elif adjust_method == "ratio":
                    for col in ["open", "high", "low", "close"]:
                        if columns[col]:
                            contract_df[f"adj_{col}"] = contract_df[columns[col]] * adjustment_ratio
                elif adjust_method == "backward" or adjust_method == "forward":
                    # These are more complex and involve propagating adjustments
                    # We'll implement a simplified version here
                    adjustment = 0
                    
                    if overlap_date is not None:
                        prev_price = continuous_df.loc[continuous_df["date"] == overlap_date, "adj_close"].iloc[0]
                        curr_price = contract_df.loc[contract_df["date"] == overlap_date, columns["close"]].iloc[0]
                        adjustment = prev_price - curr_price
                    
                    for col in ["open", "high", "low", "close"]:
                        if columns[col]:
                            contract_df[f"adj_{col}"] = contract_df[columns[col]] + adjustment
            else:
                # For the first contract or no adjustment
                for col in ["open", "high", "low", "close"]:
                    if columns[col]:
                        contract_df[f"adj_{col}"] = contract_df[columns[col]]
            
            # Filter data up to rollover date if needed
            if rollover_date is not None:
                contract_df = contract_df[contract_df["date"] <= rollover_date]
            
            # Add contract info
            contract_df["contract"] = contract
            
            # Select the columns we want to keep
            cols_to_keep = ["date", "contract", "adj_open", "adj_high", "adj_low", "adj_close"]
            
            if columns["volume"]:
                cols_to_keep.append(columns["volume"])
                contract_df["volume"] = contract_df[columns["volume"]]
            
            if columns["open_interest"]:
                cols_to_keep.append(columns["open_interest"])
                contract_df["open_interest"] = contract_df[columns["open_interest"]]
            
            # Add to continuous data
            if continuous_df.empty:
                continuous_df = contract_df[cols_to_keep].copy()
            else:
                # Avoid duplicates
                new_dates = contract_df[~contract_df["date"].isin(continuous_df["date"])]["date"]
                contract_df = contract_df[contract_df["date"].isin(new_dates)]
                
                if not contract_df.empty:
                    continuous_df = pd.concat([continuous_df, contract_df[cols_to_keep]], ignore_index=True)
        
        # Sort by date
        continuous_df = continuous_df.sort_values("date").reset_index(drop=True)
        
        # Rename columns for clarity
        continuous_df = continuous_df.rename(columns={
            "adj_open": "open",
            "adj_high": "high",
            "adj_low": "low",
            "adj_close": "close"
        })
        
        # Save to file if output directory specified
        if output_dir:
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            # Create descriptive filename
            filename = f"{product}_continuous_{roll_strategy}_{adjust_method}.csv"
            filepath = os.path.join(output_dir, filename)
            
            continuous_df.to_csv(filepath, index=False)
            logger.info(f"Continuous data saved to {filepath}")
        
        return continuous_df
    
    def _extract_expiry_date(self, contract):
        """
        Extract expiration date from contract code.
        
        Parameters:
        -----------
        contract : str
            Contract identifier
            
        Returns:
        --------
        datetime or None
            Expiration date if extractable, None otherwise
        """
        match = re.search(r"([A-Za-z]+)(\d{2,4})(\d{2})?", str(contract))
        
        if match:
            product, year, month = match.groups()
            
            if month is None:
                # Handle format like "2109" (YYMM)
                year, month = year[:2], year[2:]
            
            # Handle 2-digit years
            if len(year) == 2:
                year = "20" + year  # Assume 20xx for simplicity
            
            # Approximate expiry as last day of the month
            # This is a simplification; actual expiry rules vary by exchange
            month_end = self._get_month_end(int(year), int(month))
            
            return month_end
        
        return None
    
    def _get_month_end(self, year, month):
        """
        Get the last day of the specified month.
        
        Parameters:
        -----------
        year : int
            Year
        month : int
            Month (1-12)
            
        Returns:
        --------
        datetime
            Date of the last day of the month
        """
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        
        return next_month - timedelta(days=1)
