import pandas as pd
import os
import re
import glob

def extract_product(contract_name):
    """Extract the product code from contract name"""
    match = re.match(r'^([A-Za-z]+)', contract_name)
    return match.group(1) if match else 'unknown'

def identify_major_contracts(csv_file, output_dir='.'):
    """
    Identify major contracts from a CSV file.
    A contract becomes the major contract when its volume is larger than the previous major contract's volume.
    
    Args:
        csv_file (str): Path to the CSV file
        output_dir (str): Directory to save the output file
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file)
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Sort by date
        df = df.sort_values('date')
        
        # Get unique dates
        dates = df['date'].unique()
        
        # Track major contract and results
        current_major_contract = None
        major_contracts = []
        
        # Process each date
        for i, date in enumerate(dates):
            date_df = df[df['date'] == date]
            
            # For the first date, choose the contract with highest volume
            if i == 0:
                major_row = date_df.loc[date_df['volume'].idxmax()]
                current_major_contract = major_row['contract']
            else:
                # Check if current major contract exists on this date
                major_df = date_df[date_df['contract'] == current_major_contract]
                
                if not major_df.empty:
                    # Check if any other contract has higher volume
                    for _, row in date_df.iterrows():
                        if (row['contract'] != current_major_contract and 
                            row['volume'] > major_df.iloc[0]['volume']):
                            current_major_contract = row['contract']
                            print(f"[{os.path.basename(csv_file)}] Major contract changed on {date}: new major is {current_major_contract}")
                            break
                    
                    # Add the row for current major contract
                    major_row = date_df[date_df['contract'] == current_major_contract].iloc[0]
                else:
                    # Current major doesn't exist, find a new one
                    major_row = date_df.loc[date_df['volume'].idxmax()]
                    current_major_contract = major_row['contract']
                    print(f"[{os.path.basename(csv_file)}] Major contract changed on {date}: new major is {current_major_contract} (previous not found)")
            
            major_contracts.append(major_row)
        
        # Convert the list of Series to DataFrame
        major_df = pd.DataFrame(major_contracts)
        
        # Extract product and exchange for filename
        product = extract_product(major_df['contract'].iloc[0])
        exchange = major_df['exchange'].iloc[0]
        
        # Create output filename
        output_file = os.path.join(output_dir, f"{exchange}_{product}_major.csv")
        
        # Save to CSV
        major_df.to_csv(output_file, index=False)
        print(f"Saved {len(major_df)} major contract rows from {os.path.basename(csv_file)} to {output_file}")
        
        return True
    
    except Exception as e:
        print(f"Error processing {csv_file}: {str(e)}")
        return False

def process_directory(input_dir, output_dir='.', pattern='*.csv'):
    """
    Process all CSV files in a directory that match the pattern.
    
    Args:
        input_dir (str): Directory containing CSV files
        output_dir (str): Directory to save output files
        pattern (str): File pattern to match (default: '*.csv')
    """
    # Get all CSV files in the directory that match the pattern
    csv_files = glob.glob(os.path.join(input_dir, pattern))
    
    if not csv_files:
        print(f"No files matching '{pattern}' found in {input_dir}")
        return
    
    # Process each file
    successful = 0
    failed = 0
    
    for csv_file in csv_files:
        print(f"Processing {os.path.basename(csv_file)}...")
        if identify_major_contracts(csv_file, output_dir):
            successful += 1
        else:
            failed += 1
    
    print(f"\nProcessing complete. Summary:")
    print(f"- Files processed successfully: {successful}")
    print(f"- Files failed: {failed}")
    print(f"- Total files: {successful + failed}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract major contracts from futures data')
    parser.add_argument('--input_dir', '-i', required=True, help='Input directory path containing CSV files')
    parser.add_argument('--output_dir', '-o', required=True, help='Output directory path')
    parser.add_argument('--pattern', '-p', default='*.csv', help='File pattern to match (default: *.csv)')
    
    args = parser.parse_args()
    
    process_directory(args.input_dir, args.output_dir, args.pattern)
