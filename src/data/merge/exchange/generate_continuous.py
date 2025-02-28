#!/usr/bin/env python
"""
Script to generate continuous K-line data from futures contracts
"""

import argparse
import os
from datetime import datetime
from continuous_kline import ContinuousKline

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Generate Continuous K-line Data")
    
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
        help="Directory to save continuous K-line files (default: data-dir/continuous)"
    )
    
    # Exchange and product selection
    parser.add_argument(
        "--exchange",
        type=str,
        required=True,
        help="Exchange name (e.g., cffex, shfe)"
    )
    parser.add_argument(
        "--product",
        type=str,
        required=True,
        help="Product code (e.g., IF, cu)"
    )
    
    # Roll strategy options
    parser.add_argument(
        "--roll-strategy",
        type=str,
        choices=["volume", "oi", "time", "fixed"],
        default="volume",
        help="Contract roll strategy: volume, oi (open interest), time (days before expiry), fixed (date)"
    )
    
    # Adjustment method
    parser.add_argument(
        "--adjust-method",
        type=str,
        choices=["backward", "forward", "ratio", "difference", "none"],
        default="backward",
        help="Price adjustment method"
    )
    
    # Contract selection
    parser.add_argument(
        "--contract-months",
        type=int,
        nargs="+",
        help="Specific contract months to use (e.g., 3 6 9 12 for quarterly)"
    )
    
    # Roll parameters
    parser.add_argument(
        "--dominant-days",
        type=int,
        default=0,
        help="Days before expiration to roll (for time-based strategy)"
    )
    parser.add_argument(
        "--rollover-days",
        type=int,
        default=0,
        help="Days before month end to roll (for fixed strategy)"
    )
    
    return parser.parse_args()

def main():
    """Main function"""
    args = parse_args()
    
    # Set output directory if not specified
    output_dir = args.output_dir
    if not output_dir:
        output_dir = os.path.join(args.data_dir, "continuous")
        
    # Create continuous K-line generator
    generator = ContinuousKline(data_dir=args.data_dir)
    
    print(f"Generating continuous K-line data for {args.product} from {args.exchange}")
    print(f"Roll strategy: {args.roll_strategy}")
    print(f"Adjustment method: {args.adjust_method}")
    
    if args.contract_months:
        print(f"Using contract months: {args.contract_months}")
    
    # Generate continuous data
    continuous_df = generator.generate_continuous(
        exchange=args.exchange,
        product=args.product,
        roll_strategy=args.roll_strategy,
        adjust_method=args.adjust_method,
        contract_months=args.contract_months,
        dominant_days=args.dominant_days,
        rollover_days=args.rollover_days,
        output_dir=output_dir
    )
    
    if not continuous_df.empty:
        print(f"Successfully generated continuous data with {len(continuous_df)} bars")
        print(f"Date range: {continuous_df['date'].min()} to {continuous_df['date'].max()}")
        
        # Print output location
        filename = f"{args.product}_continuous_{args.roll_strategy}_{args.adjust_method}.csv"
        filepath = os.path.join(output_dir, filename)
        print(f"Data saved to: {filepath}")
    else:
        print("Error generating continuous data")

if __name__ == "__main__":
    main()
