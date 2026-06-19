# DiamondEdge Analytics: Monetizing Statcast Data ⚾💎

**Big Data Systems (Spring 2026) - Final Project**
**Author:** Cheng-Chien Chang (張正謙) | B13902062
**Institution:** National Taiwan University

---

## 📖 Project Overview

DiamondEdge Analytics is an end-to-end baseball data monetization system designed for high-stakes Fantasy Baseball managers and sports bettors. By automatically ingesting and processing massive MLB Statcast pitch-by-pitch datasets, the system extracts high-value predictive metrics to help users identify undervalued players ("Value Picks") before traditional box-score metrics catch up.

### Data & Methodology
The system bypasses surface-level stats and calculates advanced Sabermetrics:
* **Batter Pipeline:** Tracks **Hard Hit Rate** (percentage of batted balls >= 95 mph). By setting a minimum sample size (15 batted balls) and an above-average exit velocity threshold, the system flags batters with elite contact quality who might be experiencing a temporary statistical slump.
* **Pitcher Pipeline:** Tracks **Swinging Strike Rate (SwStr%)**. By setting a minimum pitch count (50 pitches), the system calculates the percentage of total pitches that result in a swinging strike—the premier leading indicator for a pitcher's strikeout potential (K/9).

---

## Part 1: Business Analysis & System Report

### 1. Target Customer
The target customer segment for DiamondEdge Analytics consists of high-stakes Fantasy Baseball managers and daily sports bettors. Currently, this segment relies on universally accessible, free projections (such as ESPN or Yahoo standard rankings) or generalized public models (like ZiPS or Steamer). 

DiamondEdge provides an asymmetric information advantage by analyzing raw, pitch-by-pitch Statcast data. By calculating advanced Sabermetric indicators—specifically **Hard Hit Rate** for batters and **Swinging Strike Rate (SwStr%)** for pitchers—the system identifies "Buy-Low" and "Sell-High" candidates before their traditional box-score metrics reflect their true performance.

### 2. Evidence of Demand and Willingness to Pay
To validate this market need, a multi-faceted data acquisition process was utilized:
* **Public Forum Analysis:** Discussions on Reddit (specifically `r/fantasybaseball` and `r/sportsbook`) were analyzed by tracking keywords such as "advanced metrics," "statcast trends," and "paid edge." The recurring frustration identified was the immense time required to manually aggregate and clean Statcast data.
* **Competitor Benchmarking:** Existing premium Sabermetric subscriptions, such as Baseball Prospectus' PECOTA or RotoGrinders, charge between $15 and $40 per month.

**Willingness to Pay:** Based on these findings, users are willing to pay a subscription fee of **$20 per month**. Non-monetarily, DiamondEdge saves a manager approximately 5 to 7 hours of manual data aggregation per week, representing a significant reduction in effort and time investment.

### 3. System Design
The end-to-end system pipeline is engineered to handle large datasets efficiently:
* **Data Sources (Ingestion):** A Python pipeline utilizing the `pybaseball` library scrapes daily, pitch-by-pitch Statcast data directly from Baseball Savant. The raw data is temporarily stored as CSV files.
* **Storage and Processing:** Due to the sheer volume of pitch-level data, the system utilizes **Apache Spark (PySpark)** for distributed batch processing. The processing pipeline splits into two distinct analytical flows:
    1. **Batters:** Filters for a minimum of 15 batted balls, calculating the Hard Hit Rate. It also incorporates a reverse lookup function to map MLBAM IDs to real player names.
    2. **Pitchers:** Filters for a minimum of 50 pitches thrown, calculating the Swinging Strike Rate (SwStr%).
* **Delivery:** The refined outputs are saved in the optimized `.parquet` format and delivered via a RESTful API built with **FastAPI**, creating distinct `/api/batters` and `/api/pitchers` endpoints for frontend consumption.

### 4. Go-to-Market Difficulties (Bonus)
Promoting DiamondEdge Analytics involves several realistic operational and market obstacles:
* **Data Acquisition Risks:** Scraping Baseball Savant at scale risks IP bans and violates Terms of Service for commercial use. Transitioning to a fully commercialized product would require purchasing an expensive official MLB Stats API license.
* **Trust & The Cold-Start Problem:** Convincing managers to trust a newly developed algorithm over established industry giants requires verifiable, long-term backtesting and transparent success records.

---
## Execution Guide (Running the Pipeline)
1. Fetch Raw Data

Bash
python3 src/ingestion/ingestion.py
2. Process Data via PySpark

Bash
python3 src/processing/processing_spark.py
3. Start the API Server

Bash
uvicorn src.delivery.api:app --reload

---

## Part 2: Project Structure & Setup

### Directory Structure
```text
DiamondEdge-Analytics/
├── data/
│   ├── raw/             # Ignored by Git: Raw Statcast CSV files
│   └── processed/       # Ignored by Git: Processed Parquet files
├── src/
│   ├── ingestion/       # Module 1: Data scraping (ingestion.py)
│   ├── processing/      # Module 2: PySpark pipeline (processing_spark.py)
│   └── delivery/        # Module 3: FastAPI server (api.py)
├── requirements.txt     # Python dependency list
└── README.md            # This document