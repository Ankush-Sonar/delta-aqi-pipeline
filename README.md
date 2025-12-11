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

 ## Medallion Architecture
 
Medallion architecture organizes Delta Lake data into three progressive layers: Bronze (raw), Silver (cleaned), and Gold (aggregated). This layered approach ensures data quality evolution while maintaining lineage and auditability.​

### Bronze Layer
Stores raw AQI data ingested from REST APIs (CPCB/Waqi) in Delta format without transformation. Includes metadata like ingestion timestamp, source URL, and raw JSON payload for full reproducibility.
- Captures all API responses as-is
- Schema-on-read with automatic evolution
- Handles late-arriving data via MERGE operations

### Silver Layer
Applies data quality rules, cleansing, and normalization to Bronze data. Standardizes AQI metrics (PM2.5, NO2, SO2, CO, O3), resolves duplicates, and enforces business rules.

- Removes null/invalid readings
- Normalizes city/station names
- Calculates derived metrics (AQI categories)
- Partitions by date/city for query performance

### Gold Layer
Business-ready aggregates and views optimized for analytics and ML. Provides city-wise trends, daily summaries, and station rankings.

