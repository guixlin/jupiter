import os
import pandas as pd
import glob
import sys
import re

def process_csv_files(directory_path, output_directory=None):
    """
    Process all CSV files in the specified directory by:
    1. Removing the 'source_file' column
    2. Keeping only rows where 'contract' format is "<alpha><digital>"
    3. Sorting records by date (integer) in ascending order
    
    Args:
        directory_path (str): Path to the directory containing CSV files
        output_directory (str, optional): Directory to save processed files. 
                                         If None, will overwrite original files.
    """
    # Create output directory if it doesn't exist
    if output_directory and not os.path.exists(output_directory):
        os.makedirs(output_directory)
    
    # Get all CSV files in the directory
    csv_files = glob.glob(os.path.join(directory_path, "*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {directory_path}")
        all_files = os.listdir(directory_path)
        print(f"Files in directory: {all_files}")
        return
    
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        print(f"Processing {file_name}...")
        
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)
            
            # Check if 'source_file' column exists
            if 'source_file' in df.columns:
                # Remove 'source_file' column
                df = df.drop('source_file', axis=1)
            
            # Check if 'contract' column exists
            if 'contract' in df.columns:
                # Define regex pattern for "<alpha><digital>"
                # Pattern: starts with one or more letters followed by one or more digits
                pattern = r'^[A-Za-z]+[0-9]+$'
                
                # Keep only rows where contract matches the pattern
                original_row_count = len(df)
                df = df[df['contract'].str.match(pattern, na=False)]
                filtered_row_count = len(df)
                removed_rows = original_row_count - filtered_row_count
                print(f"Removed {removed_rows} rows where contract doesn't match '<alpha><digital>' format")
            else:
                print(f"Warning: No 'contract' column found in {file_name}")
            
            # Check if 'date' column exists
            if 'date' in df.columns:
                # Sort by date ascending
                df = df.sort_values(by='date')
            else:
                print(f"Warning: No 'date' column found in {file_name}")
            
            # Save the processed file
            if output_directory:
                output_path = os.path.join(output_directory, file_name)
            else:
                output_path = file_path
                
            df.to_csv(output_path, index=False)
            print(f"Successfully processed {file_name}")
            
        except Exception as e:
            print(f"Error processing {file_name}: {e}")

# Use command line arguments for input and output directories
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 sort_data.py <input_directory> [output_directory]")
        sys.exit(1)
    
    input_directory = sys.argv[1]
    
    if len(sys.argv) > 2:
        output_directory = sys.argv[2]
        process_csv_files(input_directory, output_directory)
    else:
        process_csv_files(input_directory)
