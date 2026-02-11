import sys
import os
import yaml
import time
import logging
from dotenv import load_dotenv

# Ensure the src directory is in the system path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_utils import get_db_connection, setup_target_table
from database.generator import generate_bulk_data
from engine.anonify_engine import process_data
from database.exporter import export_to_csv

# Setup logging configuration
if not os.path.exists("logs"):
    os.makedirs("logs")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/app.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

# Load configuration
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

def main():
    logging.info("--- Starting AnonifyDB Engine ---")
    
    conn = get_db_connection()
    if not conn:
        logging.error("Database connection could not be established. Exiting.")
        return

    try:
        num_rows = config['generator']['num_rows']
        batch_size = config['generator']['batch_size']

        start_time = time.time()

        # Step 1: Bulk Data Generation
        logging.info(f"Step 1: Generating {num_rows} synthetic records...")
        generate_bulk_data(conn, num_rows, batch_size)

        # Step 2: Anonymization Process
        logging.info(f"Step 2: Starting transformation of {num_rows} records...")
        process_data(conn, setup_target_table)
        
        # Step 3: Export Data if enabled
        if config.get('export', {}).get('enabled'):
            logging.info("Step 3: Exporting data to CSV...")
            export_to_csv(
                conn, 
                config['database']['target_table'],
                config['export']['output_dir'],
                config['export']['file_name']
            )

        # Performance Summary
        end_time = time.time()
        duration = end_time - start_time
        rows_per_second = num_rows / duration if duration > 0 else 0

        logging.info("="*50)
        logging.info("?? PERFORMANCE SUMMARY")
        logging.info(f"Total Rows Processed: {num_rows}")
        logging.info(f"Total Time Taken:     {duration:.2f} seconds")
        logging.info(f"Throughput:           {rows_per_second:.2f} rows/sec")
        logging.info("="*50)
        logging.info("? Anonymization process completed successfully.")

    except Exception as e:
        logging.error(f"? An unexpected error occurred: {str(e)}")
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()