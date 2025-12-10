# AQI FORECASTING with XGBOOST

Delta AQI Pipeline implements a scalable data processing workflow using Delta Lake to ingest, transform, and manage Air Quality Index (AQI) data from public APIs. The pipeline follows Medallion architecture (bronze, silver, gold layers) for data quality evolution and supports incremental processing with ACID transactions.

Architecture
The solution leverages Delta Lake for reliable data lake operations, enabling schema enforcement, time travel, and efficient merges for CDC-like updates. Key components include API ingestion for real-time AQI metrics (PM2.5, NO2, SO2, etc.), data validation in bronze layer, cleansing/normalization in silver layer, and aggregated analytics in gold layer.​

Bronze Layer: Raw JSON/CSV from AQI sources stored as-is with metadata timestamps

Silver Layer: Cleaned data with standardized schema, null handling, and quality checks

Gold Layer: Aggregated views for city-wise AQI trends, daily summaries, and ML-ready features

## File Information

| Component                    | Technology                                              |
| ---------------------------- | ------------------------------------------------------- |
| ML                           | XGBOOST & Regression                                    |
| data/                        | Raw AQI CSV/JSON datasets from APIs                     |
| ingest.py                    | API extraction and raw data ingestion to Bronze Delta   |
| bronze_transform.py          | Initial validation, schema enforcement for Bronze layer |
| silver_transform.py          | Data cleaning, deduplication, normalization to Silver   |
| gold_aggregate.py            | Aggregations, feature engineering for Gold analytics    |
| pipeline.py / pipeline.ipynb | Main Spark ETL orchestrator; notebook for testing       |
| config.yaml                  | Pipeline configuration (API keys, paths, schemas)       |
| aqi_dashboard.pbix           | Power BI dashboards for AQI trends and alerts           |
