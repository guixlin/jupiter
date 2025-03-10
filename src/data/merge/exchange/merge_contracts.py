#!/usr/bin/env python
"""
Script to merge contract data into product files
"""

import argparse
import os
from data_processor import DataProcessor

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Merge Exchange Contract Data")
    
    # Input/Output directories
    parser.add_argument(
        "--data-dir",
        type=str,
        default="exchange_data",
        help="Directory containing exchange data (default: exchange_data)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Directory to save merged files (default: data-dir/merged)"
    )
    
    # Exchange and product selection
    parser.add_argument(
        "--exchanges",
        type=str,
        nargs="+",
        help="List of exchanges to process (default: all available exchanges)"
    )
    parser.add_argument(
        "--products",
        type=str,
        nargs="+",
        help="List of products to process (default: all available products)"
    )
    
    # Grouping options
    parser.add_argument(
        "--product-groups",
        type=str,
        nargs="+",
        action="append",
        help="Groups of products to merge together (e.g., --product-groups IF IH IC)"
    )
    
    # Contract extraction
    parser.add_argument(
        "--extract-contract",
        action="store_true",
        help="Extract data for specific contracts"
    )
    parser.add_argument(
        "--exchange",
        type=str,
        help="Exchange for contract extraction"
    )
    parser.add_argument(
        "--product",
        type=str,
        help="Product code for contract extraction"
    )
    parser.add_argument(
        "--contract-ids",
        type=str,
        nargs="+",
        help="Contract identifiers to extract (e.g., 2109 2112)"
    )
    
    return parser.parse_args()

def main():
    """Main function"""
    args = parse_args()
    
    # Create data processor
    processor = DataProcessor(data_dir=args.data_dir)
    
    # Check if we're extracting specific contracts
    if args.extract_contract:
        if not args.exchange or not args.product or not args.contract_ids:
            print("Error: --exchange, --product, and --contract-ids are required for contract extraction")
            return
        
        print(f"Extracting contract data for {args.product} from {args.exchange}")
        
        for contract_id in args.contract_ids:
            processor.extract_specific_contract(
                args.exchange,
                args.product,
                contract_id,
                args.output_dir
            )
        
        return
    
    # Process product groups if specified
    if args.product_groups:
        for group in args.product_groups:
            if not args.exchanges:
                print("Error: --exchanges is required when using --product-groups")
                return
            
            for exchange in args.exchanges:
                print(f"Merging product group {group} from {exchange}")
                processor.merge_by_product_group(exchange, group, args.output_dir)
        
        return
    
    # Otherwise, merge by product
    if args.exchanges:
        for exchange in args.exchanges:
            if args.products:
                # Process specific products
                for product in args.products:
                    print(f"Merging {product} data from {exchange}")
                    processor.merge_by_product(exchange, product, args.output_dir)
            else:
                # Process all products
                print(f"Merging all products from {exchange}")
                processor.merge_by_product(exchange, None, args.output_dir)
    else:
        # Process all exchanges
        print("Merging data from all exchanges")
        processor.merge_all_exchanges(None, args.output_dir)
    
    print("Merging completed.")

if __name__ == "__main__":
    main()
