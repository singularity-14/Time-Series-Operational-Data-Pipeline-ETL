# Time-Series Operational Data Pipeline (ETL)

This project demonstrates a production-style Extract, Transform, Load (ETL) engineering pipeline dealing with real-world time-series operational data!

It processes the **UCI Household Power Consumption Dataset**, down-sampling high-frequency minute-by-minute energy sensor streams into cleaned aggregate tables, enriching the logs with static equipment metadata, and storing it dynamically into a SQL Warehouse for Data Analytics.

## Pipeline Architecture

1. **Extract (`1_fetch_sensor_data.py`)**: Connects to an external endpoint and dynamically downloads a compressed chunk of 100,000 real-world sensor logs to act as our "Raw Intake Zone."
2. **Transform (`2_etl_pipeline.py`)**: 
    - Cleans missing/corrupted dataset artifacts.
    - Parses string timestamps into native Python `datetime` objects.
    - Executes a **Rolling Window Aggregation** (resampling minutes to hours) to reduce data bloat while preserving mathematical data integrity.
    - Joins the time-series logs against structured **Equipment Metadata** (`submeter_metadata.csv`) to map anonymous IDs to actual assets (e.g., Submeter 3 -> HVAC).
3. **Load (`2_etl_pipeline.py`)**: Bootstraps an SQLite Database mapping and loads the transformed dataset into a pristine `energy_warehouse.db`.
4. **Analytics (`3_analytics.py`)**: Executes complex `SQL GROUP BY` queries interacting with our structured data warehouse.

## How to Run Locally

### 1. Requirements
Install the required pipelines dependencies:
```bash
pip install -r requirements.txt
```

### 2. Run the Full ETL Pipeline sequentially
```bash
python 1_fetch_sensor_data.py
python 2_etl_pipeline.py
```
*Depending on data download speeds, step one will take a few moments. Step two will execute the pandas transformations and create `energy_warehouse.db` in your folder.*

### 3. Verify Operations with Analytics
```bash
python 3_analytics.py
```
*This will query the SQL database and prove the data has been joined, aggregated, and stored correctly!*
