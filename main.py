import os
import psycopg2
from dotenv import load_dotenv
from faker import Faker

# Ucitaj promenljive iz .env fajla
load_dotenv()
fake = Faker()

def anonify_engine():
    try:
        # Citamo podatke iz okruzenja (Environment variables)
        conn = psycopg2.connect(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            database=os.getenv("DB_NAME")
        )
        cur = conn.cursor()

        # Izvlacimo podatke iz tabele koju si napravio
        cur.execute("SELECT full_name, email, salary FROM users_raw;")
        rows = cur.fetchall()

        print(f"\n{'--- ANONIFY DB REPORT ---':^50}")
        print(f"{'REAL DATA':<25} | {'ANONYMIZED':<25}")
        print("-" * 55)

        for row in rows:
            real_name, real_email, real_salary = row

            # Generisanje laznih podataka (Faker)
            fake_name = fake.name()
            fake_email = fake.free_email()

            # Ispis rezultata bez latinicnih karaktera
            print(f"{real_name:<25} | {fake_name:<25}")
            print(f"{real_email:<25} | {fake_email:<25}")
            print(f"{str(real_salary) + ' EUR':<25} | {'HIDDEN':<25}")
            print("-" * 55)

        cur.close()
        conn.close()
        print("\n[SUCCESS] Connection closed safely.")

    except Exception as e:
        print(f"[ERROR] Connection failed: {e}")

if __name__ == "__main__":
    anonify_engine()