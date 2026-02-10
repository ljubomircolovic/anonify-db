import sys
import os
import yaml
import time
from dotenv import load_dotenv

# Ensure the src directory is in the system path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_utils import get_db_connection, setup_target_table
from database.generator import generate_bulk_data
from engine.anonify_engine import process_data

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

# Load configuration
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

def main():
    conn = get_db_connection()
    if not conn:
        return

    try:
        num_rows = config['generator']['num_rows']
        batch_size = config['generator']['batch_size']

        # Start timing the entire process
        start_time = time.time()

        # Step 1: Bulk Data Generation
        generate_bulk_data(conn, num_rows, batch_size)

        # Step 2: Anonymization Process
        print(f"?? Starting transformation of {num_rows} records...")
        process_data(conn, setup_target_table)

        # Calculate duration
        end_time = time.time()
        duration = end_time - start_time
        rows_per_second = num_rows / duration if duration > 0 else 0

        print(f"\n{'='*50}")
        print(f"?? PERFORMANCE SUMMARY")
        print(f"{'='*50}")
        print(f"Total Rows Processed: {num_rows}")
        print(f"Total Time Taken:     {duration:.2f} seconds")
        print(f"Throughput:           {rows_per_second:.2f} rows/sec")
        print(f"{'='*50}")
        print(f"[SUCCESS] Anonymization completed!")

    finally:
        conn.close()

if __name__ == "__main__":
    main()