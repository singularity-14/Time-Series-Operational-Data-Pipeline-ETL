import pandas as pd
import os

URL = 'https://archive.ics.uci.edu/ml/machine-learning-databases/00235/household_power_consumption.zip'
RAW_DATA_PATH = 'raw_sensor_data.csv'

def fetch_data():
    print(f"Downloading raw operational time-series from {URL} ...")
    try:
        # Stream just the first 100,000 rows to simulate operational telemetry block
        # Data is comma/semicolon delimited and zipped
        df = pd.read_csv(URL, compression='zip', sep=';', nrows=100000, 
                         na_values=['?'], low_memory=False)
        print(f"Data fetched successfully. Rows ingested: {df.shape[0]}")
        
        # Save locally as the 'raw' ingestion zone
        df.to_csv(RAW_DATA_PATH, index=False)
        print(f"Raw IoT sensor stream saved locally as {RAW_DATA_PATH}")
    except Exception as e:
        print(f"Failed to fetch data: {e}")

if __name__ == "__main__":
    fetch_data()
