import pandas as pd
import re
import os
import glob

def process_directory(input_dir, output_dir):
    """
    Process all CSV files in the input directory, extract product codes,
    and save grouped data to the output directory.
    
    Args:
        input_dir (str): Directory containing CSV files to process
        output_dir (str): Directory where output files will be saved
    """
    print(f"Processing all CSV files in: {input_dir}")
    print(f"Output will be saved to: {output_dir}")
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    
    # Get all CSV files in the input directory
    csv_files = glob.glob(os.path.join(input_dir, "*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return
    
    print(f"Found {len(csv_files)} CSV files to process")
    
    # Dictionary to store DataFrames by exchange and product
    exchange_product_dfs = {}
    
    # Process each CSV file
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        print(f"\nProcessing file: {file_name}")
        
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)
            
            # Check if required columns exist
            if 'contract' not in df.columns:
                print(f"Warning: 'contract' column not found in {file_name}, skipping file")
                continue
            
            if 'exchange' not in df.columns:
                print(f"Warning: 'exchange' column not found in {file_name}, skipping file")
                continue
            
            # Add source file column for tracking
            df['source_file'] = file_name
            
            # Extract product code from contract field using regex
            def extract_product_code(contract):
                if pd.isna(contract):
                    return None
                
                # Extract the alphabetic prefix (product code)
                match = re.match(r'^([a-zA-Z]+)', str(contract))
                if match:
                    return match.group(1)
                return None
            
            # Add product_code column
            df['product_code'] = df['contract'].apply(extract_product_code)
            
            # Create a combined key of exchange and product code
            df['exchange_product'] = df.apply(
                lambda row: f"{row['exchange']}_{row['product_code']}" 
                if not pd.isna(row['exchange']) and not pd.isna(row['product_code']) 
                else None, 
                axis=1
            )
            
            # Group by exchange_product and add to the corresponding DataFrame
            for key, group_df in df.groupby('exchange_product'):
                if key is None:
                    continue
                
                if key in exchange_product_dfs:
                    exchange_product_dfs[key] = pd.concat([exchange_product_dfs[key], group_df], ignore_index=True)
                else:
                    exchange_product_dfs[key] = group_df
                    
            print(f"Successfully processed {file_name}")
            
        except Exception as e:
            print(f"Error processing {file_name}: {e}")
    
    # Save each exchange_product DataFrame to a separate CSV file
    print("\nSaving grouped data by exchange and product:")
    for exchange_product, df in exchange_product_dfs.items():
        # Remove the temporary columns before saving
        output_df = df.drop(columns=['product_code', 'exchange_product'])
        
        # Create output filename using [exchange]_[product].csv format
        output_file = os.path.join(output_dir, f"{exchange_product}.csv")
        
        # Save to CSV
        output_df.to_csv(output_file, index=False)
        
        print(f"Saved {len(df)} rows to {output_file}")
    
    # Print summary
    print("\nSummary:")
    print(f"Total exchange-product combinations found: {len(exchange_product_dfs)}")
    for exchange_product, df in sorted(exchange_product_dfs.items()):
        print(f"  {exchange_product}: {len(df)} rows")

if __name__ == "__main__":
    import sys
    
    # Check if command-line arguments are provided
    if len(sys.argv) != 3:
        print("Usage: python3 merge_product.py <input_directory> <output_directory>")
        print("Example: python3 merge_product.py ./reformated_data ./merged_data")
        sys.exit(1)
    
    # Get input and output directories from command-line arguments
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    
    # Process all files in the directory
    process_directory(input_dir, output_dir)
