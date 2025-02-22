# Data_analysis

import os
import shutil
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from concurrent.futures import ThreadPoolExecutor

# Database connection parameters
host = "Localhost"
dbname = "Adaptricity_CLP_Testing"
port = "5432"
username = "postgres"  # PostgreSQL username
password = "123456"

# Folder paths
input_folder = "D:/ETL/OAUTH2.0API/LV DATA"  # Folder containing your Parquet files
history_folder = "D:/ETL/OAUTH2.0API/LV DATA/history"  # Folder where processed files will be moved to

# Ensure history folder exists
if not os.path.exists(history_folder):
    os.makedirs(history_folder)

# Function to process a single file
def process_file(file_path):
    try:
        # Establish a separate connection for each thread
        conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            port=port,
            user=username,
            password=password
        )
        cursor = conn.cursor()

        # Load the Parquet file into a pandas DataFrame
        df = pd.read_parquet(file_path, engine="pyarrow")  # or use fastparquet
        filename = os.path.basename(file_path)

        print(f"Processing file: {filename} (rows: {len(df)})")

        # Prepare the list of tuples for batch insertion
        rows_to_insert = [
            (
                1,1,
                row['meter_no'], 
                row['interval_date_time'], 
                row['KWH'], 
                row['KVARhRcvd']
            )
            for _, row in df.iterrows()
        ]

        # Use execute_values for bulk insert
        sql_query = """
            INSERT INTO adaptricity_Tenant01.tb_raw_timeseries (
                tenantid,iterationid,meterid, time_stamp, totalactiveenergyconsumed, totalreactiveenergyconsumed
            ) VALUES %s;
        """
        execute_values(cursor, sql_query, rows_to_insert)

        # Commit the transaction
        conn.commit()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        # Move the processed file to the history folder
        shutil.move(file_path, os.path.join(history_folder, filename))

        print(f"File {filename} processed and moved to history folder.")

    except Exception as e:
        print(f"Error processing file {os.path.basename(file_path)}: {e}")

# Get the list of files to process
parquet_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith(".parquet")]

# Process files in parallel using ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=4) as executor:  # Adjust max_workers based on system capabilities
    executor.map(process_file, parquet_files)
