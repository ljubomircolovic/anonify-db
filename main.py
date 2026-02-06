import os
import psycopg2
from dotenv import load_dotenv
from faker import Faker

# Load environment variables
load_dotenv()

# Localization for DACH and US markets
SUPPORTED_LOCALES = ['de_DE', 'en_US']
fake = Faker(SUPPORTED_LOCALES)

def get_db_connection():
    """Abstraction for database connection."""
    try:
        return psycopg2.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME")
        )
    except Exception as e:
        print(f"[ERROR] Connection failed: {e}")
        return None

def setup_target_table(cur):
    """Creates the masked table if it doesn't exist."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users_masked (
            id SERIAL PRIMARY KEY,
            full_name VARCHAR(255),
            email VARCHAR(255),
            salary_status VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    # Brisanje starih podataka kako bismo uvek imali svez test (opciono)
    cur.execute("TRUNCATE TABLE users_masked;")

def anonify_engine():
    """Core engine for fetching, transforming, and saving data."""
    conn = get_db_connection()
    if not conn:
        return

    try:
        cur = conn.cursor()

        # Step 1: Prepare the target table
        setup_target_table(cur)

        # Step 2: Fetch raw data
        cur.execute("SELECT full_name, email, salary FROM users_raw;")
        rows = cur.fetchall()

        print(f"\n{'--- ANONIFY DB | TRANSFORM & LOAD MODE ---':^65}")
        print(f"{'STATUS':<15} | {'TARGET TABLE: users_masked'}")
        print("-" * 65)

        # Step 3: Process and Insert
        for row in rows:
            real_name, real_email, real_salary = row

            # Transformation Logic
            fake_name = fake.name()
            fake_email = fake.free_email()
            salary_label = "[PROTECTED]" # Sakrivamo cifre, ostavljamo labelu

            # Execute Insert
            cur.execute("""
                INSERT INTO users_masked (full_name, email, salary_status)
                VALUES (%s, %s, %s);
            """, (fake_name, fake_email, salary_label))

            print(f"{'[MAPPING]':<15} | {real_name} -> {fake_name}")

        # Step 4: Commit changes
        conn.commit()
        cur.close()
        print("\n[SUCCESS] Data transformation complete. Check DBeaver for results.")

    except Exception as e:
        if conn:
            conn.rollback() # Ako nesto pukne, nista se ne upisuje (Data Integrity)
        print(f"[ERROR] Process failed: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    anonify_engine()