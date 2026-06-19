# Author: 張正謙 (B13902062)
# Project: DiamondEdge Analytics - Data Delivery API
from fastapi import FastAPI, HTTPException
import pandas as pd
import os

app = FastAPI(
    title="DiamondEdge Analytics API",
    description="提供基於 Statcast 進階數據的夢幻棒球高價值球員推薦 (支援打者與投手)"
)

# 指向 Spark 處理完的數據路徑 (相對路徑)
BATTERS_PARQUET = "data/processed/value_batters.parquet"
PITCHERS_PARQUET = "data/processed/value_pitchers.parquet"

@app.get("/")
def read_root():
    return {"message": "歡迎來到 DiamondEdge Analytics 核心 API"}

@app.get("/api/batters")
def get_value_batters(limit: int = 10):
    """獲取高價值打者推薦名單 (依據強擊球率 Hard Hit Rate)"""
    if not os.path.exists(BATTERS_PARQUET):
        raise HTTPException(status_code=404, detail="找不到打者數據，請先執行處理管線。")
    
    try:
        df = pd.read_parquet(BATTERS_PARQUET)
        df_sorted = df.sort_values(by="hard_hit_rate", ascending=False)
        return {"status": "success", "category": "batters", "data": df_sorted.head(limit).to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"讀取數據發生錯誤: {str(e)}")

@app.get("/api/pitchers")
def get_value_pitchers(limit: int = 10):
    """獲取高價值投手推薦名單 (依據揮空率 SwStr%)"""
    if not os.path.exists(PITCHERS_PARQUET):
        raise HTTPException(status_code=404, detail="找不到投手數據，請先執行處理管線。")
    
    try:
        df = pd.read_parquet(PITCHERS_PARQUET)
        df_sorted = df.sort_values(by="swstr_rate", ascending=False)
        return {"status": "success", "category": "pitchers", "data": df_sorted.head(limit).to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"讀取數據發生錯誤: {str(e)}")