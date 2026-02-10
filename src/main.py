import sys
import os
import yaml
from dotenv import load_dotenv

# Ensure the src directory is in the system path for module discovery
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_utils import get_db_connection, setup_target_table
from database.generator import generate_bulk_data
from engine.anonify_engine import process_data

# Load environment variables from the root .env file
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

# Load configuration settings
CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

def main():
    conn = get_db_connection()
    if not conn:
        return

    try:
        # Step 1: Stress Test - Bulk Data Generation
        num_rows = config['generator']['num_rows']
        batch_size = config['generator']['batch_size']
        generate_bulk_data(conn, num_rows, batch_size)

        # Step 2: Transformation - Anonymize the generated data
        process_data(conn, setup_target_table)
        
        print(f"\n[SUCCESS] Anonymization of {num_rows} rows completed!")
    finally:
        conn.close()

if __name__ == "__main__":
    main()