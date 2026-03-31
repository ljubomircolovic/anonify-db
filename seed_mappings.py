import os
import psycopg2
from faker import Faker
from dotenv import load_dotenv

load_dotenv()

def seed_data():
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    cur = conn.cursor()

    # 1. Uzimamo ID-eve kategorija (pretpostavljamo da si pokrenuo SQL od maločas)
    cur.execute("SELECT id, category_name FROM metadata.mapping_catalog")
    categories = {name: id for id, name in cur.fetchall()}

    # 2. Faker za različite regione
    fake_de = Faker('de_DE')
    fake_en = Faker('en_US')

    print("Seeding mappings...")

    # Punimo imena
    for _ in range(500):
        cur.execute("INSERT INTO metadata.mapping_values (catalog_id, fake_value, language_code) VALUES (%s, %s, %s)",
                    (categories['first_name'], fake_de.first_name(), 'de_DE'))
        cur.execute("INSERT INTO metadata.mapping_values (catalog_id, fake_value, language_code) VALUES (%s, %s, %s)",
                    (categories['first_name'], fake_en.first_name(), 'en_US'))

    # Punimo gradove
    for _ in range(200):
        cur.execute("INSERT INTO metadata.mapping_values (catalog_id, fake_value, language_code) VALUES (%s, %s, %s)",
                    (categories['city'], fake_de.city(), 'de_DE'))
        cur.execute("INSERT INTO metadata.mapping_values (catalog_id, fake_value, language_code) VALUES (%s, %s, %s)",
                    (categories['city'], fake_en.city(), 'en_US'))

    conn.commit()
    cur.close()
    conn.close()
    print("Done! Mapping tables are ready.")

if __name__ == "__main__":
    seed_data()