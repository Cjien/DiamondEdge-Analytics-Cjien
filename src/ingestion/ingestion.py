# Author: Cheng-Chien Chang (B13902062)
# Project: DiamondEdge Analytics - Data Ingestion
import pybaseball as pyb
import pandas as pd
import os
from datetime import datetime, timedelta

def fetch_statcast_data(start_date: str, end_date: str, output_dir: str = "data/raw"):
    """
    Fetches pitch-by-pitch Statcast data for a given date range.
    The data is fetched in batches and stored as CSV files for pipeline processing.
    """
    os.makedirs(output_dir, exist_ok=True)
    file_path = f"{output_dir}/statcast_{start_date}_to_{end_date}.csv"
    
    print(f"[Ingestion] Fetching Statcast data from {start_date} to {end_date}...")
    pyb.cache.enable() # Enable caching to prevent repetitive network requests
    
    try:
        data = pyb.statcast(start_dt=start_date, end_dt=end_date)
        
        if not data.empty:
            data.to_csv(file_path, index=False)
            print(f"[Ingestion] Successfully saved {len(data)} pitches to {file_path}")
        else:
            print("[Ingestion] No data found for this date range.")
            
    except Exception as e:
        print(f"[Ingestion Error] Failed to fetch data: {e}")

if __name__ == "__main__":
    # Fetch the past 7 days of data for the MVP pipeline
    today = datetime.now()
    start_dt = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    end_dt = today.strftime('%Y-%m-%d')
    
    fetch_statcast_data(start_dt, end_dt)