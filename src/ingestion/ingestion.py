# Author: 張正謙 (B13902062)
# Project: DiamondEdge Analytics - Data Ingestion
import pybaseball as pyb
import pandas as pd
import os
from datetime import datetime, timedelta

def fetch_statcast_data(start_date: str, end_date: str, output_dir: str = "data/raw"):
    """
    獲取指定日期範圍內的 Statcast 逐球數據。
    為了符合系統設計，這裡將資料分批拉取並存儲為 CSV 檔案。
    """
    os.makedirs(output_dir, exist_ok=True)
    file_path = f"{output_dir}/statcast_{start_date}_to_{end_date}.csv"
    
    print(f"[Ingestion] 正在獲取 {start_date} 到 {end_date} 的 Statcast 數據...")
    # 啟用快取以避免重複發送請求被封鎖
    pyb.cache.enable() 
    
    try:
        # 抓取 Statcast 數據
        data = pyb.statcast(start_dt=start_date, end_dt=end_date)
        
        if not data.empty:
            # 儲存全部數據以測試 Spark 的處理能力
            data.to_csv(file_path, index=False)
            print(f"[Ingestion] 成功儲存 {len(data)} 筆逐球數據至 {file_path}")
        else:
            print("[Ingestion] 該區間無數據。")
            
    except Exception as e:
        print(f"[Ingestion Error] 資料獲取失敗: {e}")

if __name__ == "__main__":
    # 設定抓取過去一週的數據作為 MVP 測試
    today = datetime.now()
    start_dt = (today - timedelta(days=7)).strftime('%Y-%m-%d')
    end_dt = today.strftime('%Y-%m-%d')
    
    fetch_statcast_data(start_dt, end_dt)