#!/usr/bin/env python3
"""
monitor.py

Simple pipeline monitor:
- Checks if CSV and Parquet outputs exist
- Compares schemas
- Prints summary statistics
"""

import argparse
import os
import pandas as pd
import pyarrow.parquet as pq

def parse_args():
    parser = argparse.ArgumentParser(description="Monitor ETL pipeline outputs")
    parser.add_argument("--csv-dir", required=True, help="Directory with CSV outputs (csv_out)")
    parser.add_argument("--parquet-dir", required=True, help="Directory with Parquet outputs (parquet_out)")
    return parser.parse_args()

def check_csv(csv_dir):
    if not os.path.exists(csv_dir):
        raise FileNotFoundError(f"CSV directory not found: {csv_dir}")
    
    csv_files = [f for f in os.listdir(csv_dir) if f.endswith(".csv")]
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {csv_dir}")
    
    print(f"Found {len(csv_files)} CSV files in {csv_dir}")
    sample_csv = pd.read_csv(os.path.join(csv_dir, csv_files[0]))
    print(f"CSV sample columns: {list(sample_csv.columns)}")
    return sample_csv

def check_parquet(parquet_dir):
    if not os.path.exists(parquet_dir):
        raise FileNotFoundError(f"Parquet directory not found: {parquet_dir}")
    
    parquet_files = [f for f in os.listdir(parquet_dir) if f.endswith(".parquet") or f.endswith(".parquet.snappy")]
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found in {parquet_dir}")
    
    print(f"Found {len(parquet_files)} Parquet files in {parquet_dir}")
    sample_parquet = pq.read_table(os.path.join(parquet_dir, parquet_files[0])).to_pandas()
    print(f"Parquet sample columns: {list(sample_parquet.columns)}")
    return sample_parquet

def compare_schemas(csv_df, parquet_df):
    csv_cols = set(csv_df.columns)
    parquet_cols = set(parquet_df.columns)
    if csv_cols == parquet_cols:
        print("✅ CSV and Parquet schemas match.")
    else:
        print("⚠️ CSV and Parquet schemas mismatch!")
        print(f"Only in CSV: {csv_cols - parquet_cols}")
        print(f"Only in Parquet: {parquet_cols - csv_cols}")

def main():
    args = parse_args()
    
    csv_df = check_csv(args.csv_dir)
    parquet_df = check_parquet(args.parquet_dir)
    compare_schemas(csv_df, parquet_df)
    
    print("\nPipeline monitor finished successfully!")

if __name__ == "__main__":
    main()