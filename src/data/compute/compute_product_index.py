import os
import pandas as pd
import numpy as np
import glob
import re

def extract_product_code(contract_name):
    """Extract product code from contract name (e.g., 'IC' from 'IC2101')"""
    match = re.match(r'^[A-Za-z]+', contract_name)
    if match:
        return match.group(0)
    return None

def calculate_weighted_index(group, price_column, weight_column):
    """
    Calculate weighted average of prices for a group of contracts.
    
    Args:
        group: DataFrame containing contracts for a specific date
        price_column: Column name for the price metric (open, high, low, close)
        weight_column: Column name for the weight metric (volume, oi)
        
    Returns:
        Weighted average price or None if calculation not possible
    """
    # Remove rows with missing price or weight values
    valid_data = group.dropna(subset=[price_column, weight_column])
    
    # Skip if no valid data or total weight is zero
    total_weight = valid_data[weight_column].sum()
    if len(valid_data) == 0 or total_weight == 0:
        return None
    
    # Calculate weights as proportion of total
    weights = valid_data[weight_column] / total_weight
    
    # Calculate weighted average
    weighted_avg = (valid_data[price_column] * weights).sum()
    
    return weighted_avg

def process_futures_files(directory_path, output_directory=None, create_separate_files=False):
    """
    Process all CSV files in the specified directory, calculating weighted indices
    for different price metrics (open, high, low, close) using both volume and OI weights.
    Also includes total volume and total open interest for each date.
    
    Args:
        directory_path: Path to directory containing futures CSV files
        output_directory: Directory to save output files. If None, uses the input directory.
        create_separate_files: If True, also create separate files for each index type
    """
    if output_directory is None:
        output_directory = directory_path
    
    # Ensure output directory exists
    os.makedirs(output_directory, exist_ok=True)
    
    # Define price columns to calculate indices for
    price_columns = ['open', 'high', 'low', 'close']
    weight_columns = ['volume', 'oi']
    
    # Find all CSV files in the directory
    csv_files = glob.glob(os.path.join(directory_path, "*.csv"))
    
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        print(f"Processing {file_name}...")
        
        try:
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Skip if required columns are missing
            required_columns = ['date', 'contract', 'exchange'] + price_columns + weight_columns
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                print(f"  Skipping {file_name}. Missing columns: {missing_columns}")
                continue
                
            # Extract product code from the first contract
            if df.empty:
                print(f"  Skipping {file_name}. File is empty.")
                continue
                
            # Get the first valid contract name
            valid_contracts = df['contract'].dropna()
            if valid_contracts.empty:
                print(f"  Skipping {file_name}. No valid contract names found.")
                continue
                
            first_contract = valid_contracts.iloc[0]
            product_code = extract_product_code(first_contract)
            
            if not product_code:
                print(f"  Skipping {file_name}. Could not extract product code from {first_contract}.")
                continue
                
            # Get the exchange from the first row
            exchange = df['exchange'].iloc[0]
            
            # Ensure date is in correct format
            if df['date'].dtype == 'object':
                # Try to convert string dates to numeric
                df['date'] = pd.to_numeric(df['date'], errors='coerce')
                df = df.dropna(subset=['date'])
                
            # Convert date to integer if it's float
            if df['date'].dtype == 'float':
                df['date'] = df['date'].astype(int)
            
            # Group by date
            grouped = df.groupby('date')
            
            # Dictionary to store all indices
            all_indices = {}
            
            # Prepare data for the merged file
            merged_data = []
            
            # Process each date
            for date, group in grouped:
                # Calculate total volume and total OI for this date
                total_volume = group['volume'].sum()
                total_oi = group['oi'].sum()
                
                # Initialize data for this date
                date_data = {
                    'date': date,
                    'exchange': exchange,
                    'product': product_code,
                    'total_volume': int(total_volume),
                    'total_oi': int(total_oi)
                }
                
                # Calculate all indices for this date
                for price_col in price_columns:
                    for weight_col in weight_columns:
                        index_name = f"{weight_col}_{price_col}_index"
                        weighted_avg = calculate_weighted_index(group, price_col, weight_col)
                        
                        # Add to date data
                        date_data[index_name] = round(weighted_avg, 2) if weighted_avg is not None else None
                        
                        # For separate files option
                        if create_separate_files:
                            if index_name not in all_indices:
                                all_indices[index_name] = []
                                
                            if weighted_avg is not None:
                                all_indices[index_name].append({
                                    'date': date,
                                    'exchange': exchange,
                                    'product': product_code,
                                    'index': round(weighted_avg, 2)
                                })
                
                # Add to merged data
                merged_data.append(date_data)
            
            # Convert merged data to DataFrame
            merged_df = pd.DataFrame(merged_data)
            
            # Sort by date
            merged_df = merged_df.sort_values('date')
            
            # Format date as YYYY-MM-DD
            merged_df['date'] = merged_df['date'].apply(
                lambda x: f"{str(int(x))[:4]}-{str(int(x))[4:6]}-{str(int(x))[6:8]}"
            )
            
            # Generate merged filename and save
            merged_filename = f"{exchange}_{product_code}_index.csv"
            merged_output_path = os.path.join(output_directory, merged_filename)
            merged_df.to_csv(merged_output_path, index=False)
            
            # Print statistics
            price_indices_count = len(price_columns) * len(weight_columns)
            print(f"  Created {merged_filename} with {len(merged_df)} dates and {price_indices_count} price indices")
            print(f"  Volume range: {merged_df['total_volume'].min():,} to {merged_df['total_volume'].max():,}")
            print(f"  OI range: {merged_df['total_oi'].min():,} to {merged_df['total_oi'].max():,}")
            
            # Save separate files if requested
            if create_separate_files:
                for index_name, indices in all_indices.items():
                    if indices:
                        # Convert to DataFrame
                        index_df = pd.DataFrame(indices)
                        
                        # Sort by date
                        index_df = index_df.sort_values('date')
                        
                        # Format date as YYYY-MM-DD
                        index_df['date'] = index_df['date'].apply(
                            lambda x: f"{str(int(x))[:4]}-{str(int(x))[4:6]}-{str(int(x))[6:8]}"
                        )
                        
                        # Reorder columns for better readability
                        index_df = index_df[['date', 'exchange', 'product', 'index']]
                        
                        # Save to CSV file
                        filename = f"{exchange}_{product_code}_{index_name}.csv"
                        output_path = os.path.join(output_directory, filename)
                        index_df.to_csv(output_path, index=False)
                        print(f"  Created {filename} with {len(index_df)} dates")
            
        except Exception as e:
            print(f"  Error processing {file_name}: {str(e)}")

def explain_methodology():
    """Print an explanation of the index calculation methodology"""
    print("\nIndex Calculation Methodology:")
    print("-" * 70)
    print("For each CSV file, the script calculates 8 different indices:")
    print("  - 4 price metrics (open, high, low, close)")
    print("  - Each weighted by 2 different methods (volume, open interest)")
    print("\nPrice Metrics:")
    print("  - open: Opening price of each contract")
    print("  - high: Highest price of each contract")
    print("  - low: Lowest price of each contract") 
    print("  - close: Closing price of each contract")
    print("\nWeighting Methods:")
    print("1. Volume-Weighted:")
    print("   - Each contract's price is weighted by its proportion of the total trading volume")
    print("   - Formula: Index = Σ(price_i * (volume_i / total_volume))")
    print("   - This gives more importance to highly traded contracts")
    print("\n2. Open Interest-Weighted:")
    print("   - Each contract's price is weighted by its proportion of the total open interest")
    print("   - Formula: Index = Σ(price_i * (oi_i / total_oi))")
    print("   - This gives more importance to contracts with higher open interest")
    print("\nOutput File:")
    print("  All indices are merged into a single file with naming pattern:")
    print("  <exchange>_<product>_merged_indices.csv")
    print("  This file contains columns for:")
    print("  - date: Trading date (YYYY-MM-DD format)")
    print("  - exchange: Exchange code")
    print("  - product: Product code")
    print("  - total_volume: Sum of trading volume across all contracts for the date")
    print("  - total_oi: Sum of open interest across all contracts for the date")
    print("  - volume_open_index: Volume-weighted opening price")
    print("  - volume_high_index: Volume-weighted highest price")
    print("  - volume_low_index: Volume-weighted lowest price")
    print("  - volume_close_index: Volume-weighted closing price")
    print("  - oi_open_index: OI-weighted opening price")
    print("  - oi_high_index: OI-weighted highest price")
    print("  - oi_low_index: OI-weighted lowest price")
    print("  - oi_close_index: OI-weighted closing price")
    print("-" * 70)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Calculate and merge weighted indices for futures contracts with total volume and OI statistics.')
    parser.add_argument('input_dir', help='Directory containing futures CSV files')
    parser.add_argument('--output_dir', help='Directory to save output files', default=None)
    parser.add_argument('--separate_files', action='store_true', help='Create separate files for each index type in addition to the merged file')
    parser.add_argument('--explain', action='store_true', help='Explain the index calculation methodology')
    
    args = parser.parse_args()
    
    if args.explain:
        explain_methodology()
    
    process_futures_files(args.input_dir, args.output_dir, args.separate_files)
