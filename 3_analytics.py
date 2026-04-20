import sqlite3
import pandas as pd

DB_PATH = 'energy_warehouse.db'

def run_analytics():
    print(f"Connecting to {DB_PATH}...\n")
    conn = sqlite3.connect(DB_PATH)
    
    # Example SQL Query: Find out which asset type consumes the most energy on average
    query = """
    SELECT 
        asset_type,
        location,
        COUNT(*) as total_hourly_logs,
        ROUND(AVG(energy_consumed_watt_hour), 2) as avg_hourly_energy_consumption,
        ROUND(MAX(energy_consumed_watt_hour), 2) as peak_load_recorded
    FROM hourly_equipment_usage
    GROUP BY asset_type, location
    ORDER BY avg_hourly_energy_consumption DESC;
    """
    
    print("Executing SQL Aggregation Query against Transformed Operational Data...")
    print("-" * 70)
    print(query.strip())
    print("-" * 70)
    
    # Use pandas to easily format and print the SQL results
    result_df = pd.read_sql_query(query, conn)
    print("\nQUERY RESULTS:")
    print(result_df.to_string(index=False))
    
    conn.close()

if __name__ == "__main__":
    run_analytics()
