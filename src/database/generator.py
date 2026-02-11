import logging
from faker import Faker
import random

# Get the logger inherited from main
logger = logging.getLogger(__name__)

def generate_bulk_data(conn, num_rows, batch_size):
    """
    Generates synthetic test data and inserts it into the users_raw table.
    Ensures table existence and uses batch insertion.
    """
    fake = Faker('de_DE')
    cur = conn.cursor()

    # Ensure source table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users_raw (
            id SERIAL PRIMARY KEY,
            full_name VARCHAR(255),
            email VARCHAR(255),
            salary VARCHAR(50)
        );
    """)

    logger.info(f"?? Starting Stress Test Generator: Creating {num_rows} rows...")

    # Clean the source table and reset auto-incrementing IDs
    cur.execute("TRUNCATE TABLE users_raw RESTART IDENTITY CASCADE;")

    rows = []
    for i in range(1, num_rows + 1):
        full_name = fake.name()
        email = f"{full_name.lower().replace(' ', '.')}@example.com"
        salary = f"{random.randint(30, 200)}k"

        rows.append((full_name, email, salary))

        if i % batch_size == 0:
            cur.executemany(
                "INSERT INTO users_raw (full_name, email, salary) VALUES (%s, %s, %s)",
                rows
            )
            conn.commit()
            rows = []
            logger.info(f"? Batch inserted: {i} rows so far...")

    if rows:
        cur.executemany("INSERT INTO users_raw (full_name, email, salary) VALUES (%s, %s, %s)", rows)
        conn.commit()

    cur.close()
    logger.info(f"?? Database populated with {num_rows} synthetic records.")