# Author: Cheng-Chien Chang (B13902062)
# Project: DiamondEdge Analytics - Data Delivery API
from fastapi import FastAPI, HTTPException
import pandas as pd
import os

app = FastAPI(
    title="DiamondEdge Analytics API",
    description="Provides Sabermetric-driven value player recommendations for Fantasy Baseball."
)

BATTERS_PARQUET = "data/processed/value_batters.parquet"
PITCHERS_PARQUET = "data/processed/value_pitchers.parquet"

@app.get("/")
def read_root():
    return {"message": "Welcome to the DiamondEdge Analytics Core API"}

@app.get("/api/batters")
def get_value_batters(limit: int = 10):
    """Retrieve high-value batters ranked by Hard Hit Rate."""
    if not os.path.exists(BATTERS_PARQUET):
        raise HTTPException(status_code=404, detail="Batter data not found. Run processing pipeline first.")
    
    try:
        df = pd.read_parquet(BATTERS_PARQUET)
        df_sorted = df.sort_values(by="hard_hit_rate", ascending=False)
        return {"status": "success", "category": "batters", "data": df_sorted.head(limit).to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Data reading error: {str(e)}")

@app.get("/api/pitchers")
def get_value_pitchers(limit: int = 10):
    """Retrieve high-value pitchers ranked by Swinging Strike Rate (SwStr%)."""
    if not os.path.exists(PITCHERS_PARQUET):
        raise HTTPException(status_code=404, detail="Pitcher data not found. Run processing pipeline first.")
    
    try:
        df = pd.read_parquet(PITCHERS_PARQUET)
        df_sorted = df.sort_values(by="swstr_rate", ascending=False)
        return {"status": "success", "category": "pitchers", "data": df_sorted.head(limit).to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Data reading error: {str(e)}")