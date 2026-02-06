import os
import psycopg2
from dotenv import load_dotenv
from faker import Faker

# Load environment variables (DB credentials and config)
load_dotenv()

# Konfiguracija lokalizacije: Mozes dodati ili ukloniti jezike po potrebi
# de_DE (Nemacka), sr_RS (Srbija), en_US (USA)
SUPPORTED_LOCALES = ['de_DE', 'en_US']
fake = Faker(SUPPORTED_LOCALES)

def get_db_connection():
    """Abstraction for database connection using environment variables."""
    try:
        return psycopg2.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME")
        )
    except Exception as e:
        print(f"[ERROR] Could not establish connection: {e}")
        return None

def anonify_engine():
    """
    Core engine for database anonymization.
    Features: Multi-locale support (DE, SRB, EN), Secure DB handling.
    """
    conn = get_db_connection()
    if not conn:
        return

    try:
        cur = conn.cursor()

        # SQL query to fetch production-like data
        query = "SELECT full_name, email, salary FROM users_raw;"
        cur.execute(query)
        rows = cur.fetchall()

        # Professional Terminal Output
        print(f"\n{'--- ANONIFY DB | LOCALIZED MULTI-DB ENGINE ---':^65}")
        print(f"{'SOURCE DATA':<25} | {'ANONYMIZED (DE/SRB/EN)':<30}")
        print("-" * 70)

        for row in rows:
            real_name, real_email, real_salary = row

            # Faker randomly picks identity from SUPPORTED_LOCALES
            fake_name = fake.name()
            fake_email = fake.free_email()

            # Formatting the output for a clean engineering report
            print(f"{real_name:<25} | {fake_name:<30}")
            print(f"{real_email:<25} | {fake_email:<30}")
            print(f"{str(real_salary) + ' EUR':<25} | {'[PROTECTED]':<30}")
            print("-" * 70)

        cur.close()
    except Exception as e:
        print(f"[ERROR] Anonymization process failed: {e}")
    finally:
        if conn:
            conn.close()
            print("\n[SUCCESS] Security session finished. Connection closed.")

if __name__ == "__main__":
    anonify_engine()