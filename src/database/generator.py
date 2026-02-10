
from faker import Faker
import random

def generate_bulk_data(conn, num_rows, batch_size):
    """
    Generates synthetic test data and inserts it into the users_raw table.
    Uses batch insertion for better performance and memory management.
    """
    fake = Faker('de_DE')
    cur = conn.cursor()

    print(f"?? Starting Stress Test Generator: Creating {num_rows} rows...")

    # Clean the source table and reset auto-incrementing IDs
    cur.execute("TRUNCATE TABLE users_raw RESTART IDENTITY CASCADE;")

    rows = []
    for i in range(1, num_rows + 1):
        full_name = fake.name()
        # Generate a simple email based on the name
        email = f"{full_name.lower().replace(' ', '.')}@example.com"
        # Random salary between 30k and 200k for realistic distribution
        salary = f"{random.randint(30, 200)}k"

        rows.append((full_name, email, salary))

        # Batch insert when reaching batch_size
        if i % batch_size == 0:
            cur.executemany(
                "INSERT INTO users_raw (full_name, email, salary) VALUES (%s, %s, %s)",
                rows
            )
            conn.commit()
            rows = []
            print(f"? Inserted {i} rows...")

    # Insert remaining rows if any
    if rows:
        cur.executemany("INSERT INTO users_raw (full_name, email, salary) VALUES (%s, %s, %s)", rows)
        conn.commit()

    cur.close()
    print(f"?? Success! users_raw now contains {num_rows} synthetic records.")