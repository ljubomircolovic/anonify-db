from dotenv import load_dotenv
from db_utils import get_db_connection, seed_test_data, setup_target_table
from anonify_engine import process_data

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