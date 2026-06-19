# DiamondEdge Analytics: 棒球數據變現系統

## 專案簡介 (Project Overview)
這是一個將 MLB Statcast 進階棒球數據轉化為高潛力球員預測的端到端系統，目標客群為高籌碼的夢幻棒球 (Fantasy Baseball) 玩家。

## 系統架構 (Architecture)
- **Ingestion**: 使用 `pybaseball` 爬取 Statcast 逐球數據。
- **Processing**: 使用 PySpark 進行進階賽伯米崔克指標 (如 xwOBA, Hard Hit Rate) 的運算。
- **Delivery**: [待補上 PostgreSQL 與 API/Dashboard 架構]

## 如何在本地端運行 (How to Run Locally)
1. 安裝依賴套件：`pip install -r requirements.txt`
2. 獲取資料：執行 `python src/ingestion/ingestion.py`
3. 處理資料：執行 `python src/processing/processing_spark.py`