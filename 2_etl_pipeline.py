import pandas as pd
import sqlite3
import os

RAW_DATA_PATH = 'raw_sensor_data.csv'
METADATA_PATH = 'submeter_metadata.csv'
DB_PATH = 'energy_warehouse.db'

def run_etl_pipeline():
    print("--- [Extract] ---")
    if not os.path.exists(RAW_DATA_PATH):
        raise FileNotFoundError("Raw data missing. Please run 1_fetch_sensor_data.py first.")
    
    print("Loading raw time-series data...")
    raw_df = pd.read_csv(RAW_DATA_PATH)
    
    print("Loading equipment metadata...")
    meta_df = pd.read_csv(METADATA_PATH)

    print("\n--- [Transform] ---")
    # 1. Handle missing values
    print("Handling missing telemetry...")
    df = raw_df.dropna().copy()
    
    # 2. Parse Timestamp
    print("Parsing operational timestamps...")
    # The dataset uses day/month/year format
    df['Datetime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], format="%d/%m/%Y %H:%M:%S")
    df.set_index('Datetime', inplace=True)
    df.drop(columns=['Date', 'Time'], inplace=True)
    
    # 3. Resample the minute-by-minute data to Hourly aggregates to save space and analytics time
    print("Aggregating high-frequency signals into 1-Hour windows...")
    # Select only numeric columns for resampling
    numeric_cols = df.select_dtypes(include=['number']).columns
    hourly_df = df[numeric_cols].resample('1h').mean()
    hourly_df.reset_index(inplace=True)
    
    # 4. Melt (Unpivot) the dataset so each sub-meter has its own row per hour
    print("Standardizing schema (Melting) for relational joins...")
    melted_df = hourly_df.melt(
        id_vars=['Datetime', 'Global_active_power', 'Voltage'], 
        value_vars=['Sub_metering_1', 'Sub_metering_2', 'Sub_metering_3'],
        var_name='meter_id',
        value_name='energy_consumed_watt_hour'
    )
    
    # 5. Join with Equipment Metadata
    print("Enriching telemetry with static Equipment Metadata...")
    enriched_df = pd.merge(melted_df, meta_df, on='meter_id', how='left')
    
    print(f"Transformed dataset ready. Dimensions: {enriched_df.shape}")

    print("\n--- [Load] ---")
    print(f"Connecting to SQL Data Warehouse ({DB_PATH})...")
    conn = sqlite3.connect(DB_PATH)
    
    # Save the transformed dataframe to an SQL table
    table_name = 'hourly_equipment_usage'
    enriched_df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    print(f"Successfully loaded {enriched_df.shape[0]} operational records into '{table_name}' table!")
    conn.close()

if __name__ == "__main__":
    run_etl_pipeline()
