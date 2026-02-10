import os
import psycopg2

def get_db_connection():
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

def seed_test_data(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users_raw (
                id SERIAL PRIMARY KEY,
                full_name TEXT, email TEXT, salary TEXT
            );
        """)
        cur.execute("SELECT COUNT(*) FROM users_raw;")
        if cur.fetchone()[0] == 0:
            test_users = [
                ('Ljubomir Colovic', 'ljubomir@example.com', '120k'),
                ('Marko Markovic', 'marko@test.rs', '80k'),
                ('Jovan Jovanovic', 'jovan@dhl.de', '95k')
            ]
            cur.executemany("INSERT INTO users_raw (full_name, email, salary) VALUES (%s, %s, %s);", test_users)
            conn.commit()

def setup_target_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users_masked (
            id SERIAL PRIMARY KEY,
            full_name VARCHAR(255),
            email VARCHAR(255),
            salary_status VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    cur.execute("TRUNCATE TABLE users_masked;")