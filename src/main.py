import sys
import os
from dotenv import load_dotenv  # OVO TI NEDOSTAJE

# Dodajemo src putanju u sistem da bi Python video module
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.db_utils import get_db_connection, seed_test_data, setup_target_table
from engine.anonify_engine import process_data

load_dotenv()
def main():
    conn = get_db_connection()
    if not conn:
        return

    try:
        seed_test_data(conn)
        process_data(conn, setup_target_table)
        print("\n[SUCCESS] Data transformation complete.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()