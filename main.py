import os
import psycopg2
from dotenv import load_dotenv
from faker import Faker

# Load environment variables from .env
load_dotenv()
fake = Faker()

def get_db_connection():
    """Abstraction for database connection."""
    return psycopg2.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME")
    )

def anonify_engine():
    """
    Core engine for database anonymization.
    Currently: PostgreSQL
    Roadmap: MySQL, SQL Server, Oracle
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Fetching raw data from the landing table
        cur.execute("SELECT full_name, email, salary FROM users_raw;")
        rows = cur.fetchall()

        print(f"\n{'--- ANONIFY DB | MULTI-DB ENGINE ---':^60}")
        print(f"{'SOURCE DATA':<25} | {'ANONYMIZED OUTPUT':<25}")
        print("-" * 65)

        for row in rows:
            real_name, real_email, real_salary = row

            # Anonymization logic
            # Note: Faker is DB-agnostic, which makes expansion easier
            fake_name = fake.name()
            fake_email = fake.free_email()

            print(f"{real_name:<25} | {fake_name:<25}")
            print(f"{real_email:<25} | {fake_email:<25}")
            print(f"{str(real_salary) + ' EUR':<25} | {'[PROTECTED]':<25}")
            print("-" * 65)

        cur.close()
    except Exception as e:
        print(f"[ERROR] Database engine failure: {e}")
    finally:
        if conn:
            conn.close()
            print("\n[SUCCESS] Session closed. Connection safely terminated.")

if __name__ == "__main__":
    anonify_engine()